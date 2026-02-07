"""FFmpeg Worker - Processes videos from the queue."""

import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx
import redis.asyncio as redis
import structlog
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from pydantic_settings import BaseSettings

from tasks import (
    TRANSCODE_PROFILES,
    OutputFormat,
    build_ffmpeg_command,
    get_output_extension,
)


# Configuration
class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379"
    input_path: str = "/app/storage/input"
    output_path: str = "/app/storage/output"
    worker_concurrency: int = 2
    consumer_group: str = "video-workers"
    consumer_name: str = f"worker-{os.getpid()}"
    max_retries: int = 3
    retry_delay: int = 5
    block_timeout: int = 5000  # ms
    log_level: str = "INFO"
    metrics_port: int = 9100

    class Config:
        env_prefix = ""


settings = Settings()

# Structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
)
logger = structlog.get_logger()

# Prometheus metrics
JOBS_PROCESSED = Counter(
    'video_jobs_processed_total',
    'Total number of video jobs processed',
    ['status', 'profile']
)
PROCESSING_DURATION = Histogram(
    'video_processing_duration_seconds',
    'Time to process a video',
    ['profile'],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600]
)
ACTIVE_JOBS = Gauge(
    'video_active_jobs',
    'Number of jobs currently being processed'
)
RETRY_COUNTER = Counter(
    'video_job_retries_total',
    'Total number of job retries'
)

# Stream names
PROCESSING_STREAM = "video:processing"
DEAD_LETTER_STREAM = "video:dlq"
STATUS_HASH = "video:status"


class VideoProcessor:
    """Handles video processing with FFmpeg."""

    def __init__(self):
        self.redis: Optional[redis.Redis] = None
        self.running = True

    async def connect(self):
        """Connect to Redis."""
        self.redis = redis.from_url(settings.redis_url, decode_responses=True)

        # Create consumer group if it doesn't exist
        try:
            await self.redis.xgroup_create(
                PROCESSING_STREAM,
                settings.consumer_group,
                id="0",
                mkstream=True
            )
            logger.info("Created consumer group", group=settings.consumer_group)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
            logger.info("Consumer group already exists", group=settings.consumer_group)

    async def close(self):
        """Close Redis connection."""
        if self.redis:
            await self.redis.close()

    async def update_job_status(
        self,
        job_id: str,
        status: str,
        error: Optional[str] = None,
        outputs: Optional[list] = None,
    ):
        """Update job status in Redis."""
        job_data = await self.redis.hget(STATUS_HASH, job_id)
        if job_data:
            data = json.loads(job_data)
            data["status"] = status
            data["updated_at"] = datetime.utcnow().isoformat()
            if error:
                data["error"] = error
            if outputs:
                data["outputs"] = outputs
            await self.redis.hset(STATUS_HASH, job_id, json.dumps(data))

    async def download_video(self, url: str, output_path: Path) -> bool:
        """Download video from URL."""
        try:
            async with httpx.AsyncClient() as client:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    with open(output_path, "wb") as f:
                        async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                            f.write(chunk)
            return True
        except Exception as e:
            logger.error("Failed to download video", url=url, error=str(e))
            return False

    def run_ffmpeg(self, command: list[str]) -> tuple[bool, str]:
        """Run FFmpeg command and return success status and output."""
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout
            )
            if result.returncode != 0:
                return False, result.stderr
            return True, result.stdout
        except subprocess.TimeoutExpired:
            return False, "FFmpeg timed out"
        except Exception as e:
            return False, str(e)

    async def process_video(self, job_id: str, file_path: str, filename: str) -> dict:
        """
        Process a video through all transcoding profiles.

        Returns dict with status and outputs.
        """
        input_path = Path(file_path)

        if not input_path.exists():
            return {
                "success": False,
                "error": f"Input file not found: {input_path}",
                "outputs": []
            }

        # Create output directory for this job
        output_dir = Path(settings.output_path) / job_id
        output_dir.mkdir(parents=True, exist_ok=True)

        outputs = []
        errors = []

        # Process each profile
        profiles_to_process = [
            OutputFormat.MP4_720P,
            OutputFormat.MP4_480P,
            OutputFormat.THUMBNAIL,
        ]

        for format_type in profiles_to_process:
            profile = TRANSCODE_PROFILES[format_type]
            output_ext = get_output_extension(profile)
            output_file = output_dir / f"{profile.name}{output_ext}"

            logger.info(
                "Processing profile",
                job_id=job_id,
                profile=profile.name
            )

            with PROCESSING_DURATION.labels(profile=profile.name).time():
                command = build_ffmpeg_command(
                    str(input_path),
                    str(output_file),
                    profile
                )

                success, output = self.run_ffmpeg(command)

            if success:
                outputs.append({
                    "profile": profile.name,
                    "path": str(output_file),
                    "format": format_type.value
                })
                JOBS_PROCESSED.labels(status="success", profile=profile.name).inc()
                logger.info(
                    "Profile completed",
                    job_id=job_id,
                    profile=profile.name,
                    output=str(output_file)
                )
            else:
                errors.append({
                    "profile": profile.name,
                    "error": output
                })
                JOBS_PROCESSED.labels(status="error", profile=profile.name).inc()
                logger.error(
                    "Profile failed",
                    job_id=job_id,
                    profile=profile.name,
                    error=output
                )

        return {
            "success": len(errors) == 0,
            "outputs": outputs,
            "errors": errors,
            "error": "; ".join([e["error"] for e in errors]) if errors else None
        }

    async def handle_message(self, message_id: str, data: dict):
        """Handle a single message from the stream."""
        job_id = data.get("job_id")
        file_path = data.get("file_path")
        source_url = data.get("source_url")
        filename = data.get("filename", "unknown")

        logger.info("Processing job", job_id=job_id, filename=filename)
        ACTIVE_JOBS.inc()

        try:
            await self.update_job_status(job_id, "processing")

            # Handle URL source
            if source_url and not file_path:
                file_path = Path(settings.input_path) / f"{job_id}.mp4"
                logger.info("Downloading video", job_id=job_id, url=source_url)

                if not await self.download_video(source_url, file_path):
                    await self.update_job_status(
                        job_id, "failed", "Failed to download video"
                    )
                    await self.redis.xack(
                        PROCESSING_STREAM, settings.consumer_group, message_id
                    )
                    return

            # Process the video
            result = await self.process_video(job_id, file_path, filename)

            if result["success"]:
                await self.update_job_status(
                    job_id, "completed", outputs=result["outputs"]
                )
                logger.info("Job completed", job_id=job_id, outputs=len(result["outputs"]))
            else:
                await self.update_job_status(
                    job_id, "failed", error=result["error"], outputs=result["outputs"]
                )
                logger.error("Job failed", job_id=job_id, error=result["error"])

            # Acknowledge message
            await self.redis.xack(
                PROCESSING_STREAM, settings.consumer_group, message_id
            )

        except Exception as e:
            logger.exception("Unexpected error processing job", job_id=job_id)
            await self.update_job_status(job_id, "failed", error=str(e))

            # Move to dead letter queue after max retries
            # For now, just acknowledge to prevent infinite loop
            await self.redis.xack(
                PROCESSING_STREAM, settings.consumer_group, message_id
            )

            # Add to DLQ
            await self.redis.xadd(
                DEAD_LETTER_STREAM,
                {
                    "job_id": job_id,
                    "original_message_id": message_id,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

        finally:
            ACTIVE_JOBS.dec()

    async def run(self):
        """Main worker loop."""
        logger.info(
            "Worker starting",
            consumer_name=settings.consumer_name,
            concurrency=settings.worker_concurrency
        )

        while self.running:
            try:
                # Read from stream
                messages = await self.redis.xreadgroup(
                    groupname=settings.consumer_group,
                    consumername=settings.consumer_name,
                    streams={PROCESSING_STREAM: ">"},
                    count=settings.worker_concurrency,
                    block=settings.block_timeout
                )

                if not messages:
                    continue

                # Process messages concurrently
                tasks = []
                for stream_name, stream_messages in messages:
                    for message_id, data in stream_messages:
                        task = asyncio.create_task(
                            self.handle_message(message_id, data)
                        )
                        tasks.append(task)

                if tasks:
                    await asyncio.gather(*tasks)

            except asyncio.CancelledError:
                logger.info("Worker cancelled")
                break
            except Exception as e:
                logger.exception("Error in worker loop")
                await asyncio.sleep(1)

        logger.info("Worker stopped")


async def main():
    """Main entry point."""
    # Start Prometheus metrics server
    start_http_server(settings.metrics_port)
    logger.info("Metrics server started", port=settings.metrics_port)

    processor = VideoProcessor()
    await processor.connect()

    try:
        await processor.run()
    finally:
        await processor.close()


if __name__ == "__main__":
    asyncio.run(main())
