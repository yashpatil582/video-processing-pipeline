"""Ingestion Service - Accepts video uploads and queues for processing."""

import asyncio
import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiofiles
import redis.asyncio as redis
import structlog
from fastapi import FastAPI, File, HTTPException, UploadFile, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response


# Configuration
class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379"
    storage_path: str = "/app/storage/input"
    max_file_size: int = 500 * 1024 * 1024  # 500MB
    allowed_extensions: list = [".mp4", ".mov", ".avi", ".mkv", ".webm"]
    log_level: str = "INFO"

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
UPLOAD_COUNTER = Counter(
    'video_uploads_total',
    'Total number of video uploads',
    ['status']
)
UPLOAD_SIZE_HISTOGRAM = Histogram(
    'video_upload_size_bytes',
    'Size of uploaded videos in bytes',
    buckets=[1e6, 5e6, 10e6, 50e6, 100e6, 500e6]
)
QUEUE_SIZE_GAUGE = Gauge(
    'processing_queue_size',
    'Number of videos in processing queue'
)
UPLOAD_DURATION_HISTOGRAM = Histogram(
    'video_upload_duration_seconds',
    'Time to upload and queue a video'
)

# FastAPI app
app = FastAPI(
    title="Video Ingestion Service",
    description="Upload videos for processing",
    version="1.0.0"
)

# Redis connection
redis_client: Optional[redis.Redis] = None

# Stream names
PROCESSING_STREAM = "video:processing"
STATUS_HASH = "video:status"


class VideoUploadResponse(BaseModel):
    job_id: str
    filename: str
    status: str
    message: str
    created_at: str


class JobStatus(BaseModel):
    job_id: str
    status: str
    filename: str
    created_at: str
    updated_at: str
    error: Optional[str] = None
    outputs: list = Field(default_factory=list)


@app.on_event("startup")
async def startup():
    """Initialize Redis connection on startup."""
    global redis_client
    redis_client = redis.from_url(settings.redis_url, decode_responses=True)
    await logger.ainfo("Ingestion service started", redis_url=settings.redis_url)

    # Ensure storage directory exists
    Path(settings.storage_path).mkdir(parents=True, exist_ok=True)


@app.on_event("shutdown")
async def shutdown():
    """Close Redis connection on shutdown."""
    global redis_client
    if redis_client:
        await redis_client.close()


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        await redis_client.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
        )


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    # Update queue size
    try:
        queue_length = await redis_client.xlen(PROCESSING_STREAM)
        QUEUE_SIZE_GAUGE.set(queue_length)
    except Exception:
        pass

    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


@app.post("/upload", response_model=VideoUploadResponse)
async def upload_video(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None
):
    """
    Upload a video file for processing.

    The video will be saved and queued for transcoding to multiple formats.
    """
    with UPLOAD_DURATION_HISTOGRAM.time():
        # Validate file extension
        file_ext = Path(file.filename).suffix.lower()
        if file_ext not in settings.allowed_extensions:
            UPLOAD_COUNTER.labels(status="rejected").inc()
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type. Allowed: {settings.allowed_extensions}"
            )

        # Generate job ID
        job_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()

        # Save file
        file_path = Path(settings.storage_path) / f"{job_id}{file_ext}"

        try:
            # Stream file to disk
            file_size = 0
            async with aiofiles.open(file_path, 'wb') as out_file:
                while chunk := await file.read(1024 * 1024):  # 1MB chunks
                    file_size += len(chunk)
                    if file_size > settings.max_file_size:
                        await out_file.close()
                        file_path.unlink()
                        UPLOAD_COUNTER.labels(status="rejected").inc()
                        raise HTTPException(
                            status_code=413,
                            detail=f"File too large. Max size: {settings.max_file_size / 1024 / 1024}MB"
                        )
                    await out_file.write(chunk)

            UPLOAD_SIZE_HISTOGRAM.observe(file_size)

        except HTTPException:
            raise
        except Exception as e:
            UPLOAD_COUNTER.labels(status="error").inc()
            await logger.aerror("Failed to save file", error=str(e), job_id=job_id)
            raise HTTPException(status_code=500, detail="Failed to save file")

        # Create job status
        job_data = {
            "job_id": job_id,
            "filename": file.filename,
            "file_path": str(file_path),
            "file_size": file_size,
            "status": "queued",
            "created_at": timestamp,
            "updated_at": timestamp,
            "outputs": [],
            "error": None
        }

        # Store status in Redis
        await redis_client.hset(
            STATUS_HASH,
            job_id,
            json.dumps(job_data)
        )

        # Add to processing stream
        await redis_client.xadd(
            PROCESSING_STREAM,
            {
                "job_id": job_id,
                "file_path": str(file_path),
                "filename": file.filename
            }
        )

        UPLOAD_COUNTER.labels(status="success").inc()
        await logger.ainfo(
            "Video uploaded successfully",
            job_id=job_id,
            filename=file.filename,
            size=file_size
        )

        return VideoUploadResponse(
            job_id=job_id,
            filename=file.filename,
            status="queued",
            message="Video uploaded and queued for processing",
            created_at=timestamp
        )


@app.post("/upload-url")
async def upload_from_url(url: str):
    """
    Queue a video from URL for processing.

    The worker will download and process the video.
    """
    job_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat()

    job_data = {
        "job_id": job_id,
        "filename": url.split("/")[-1],
        "source_url": url,
        "status": "queued",
        "created_at": timestamp,
        "updated_at": timestamp,
        "outputs": [],
        "error": None
    }

    await redis_client.hset(STATUS_HASH, job_id, json.dumps(job_data))
    await redis_client.xadd(
        PROCESSING_STREAM,
        {
            "job_id": job_id,
            "source_url": url,
            "filename": job_data["filename"]
        }
    )

    UPLOAD_COUNTER.labels(status="success").inc()

    return VideoUploadResponse(
        job_id=job_id,
        filename=job_data["filename"],
        status="queued",
        message="Video URL queued for processing",
        created_at=timestamp
    )


@app.get("/jobs/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    """Get the status of a processing job."""
    job_data = await redis_client.hget(STATUS_HASH, job_id)

    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found")

    data = json.loads(job_data)
    return JobStatus(**data)


@app.get("/jobs")
async def list_jobs(limit: int = 50):
    """List all processing jobs."""
    all_jobs = await redis_client.hgetall(STATUS_HASH)

    jobs = []
    for job_id, job_data in all_jobs.items():
        jobs.append(json.loads(job_data))

    # Sort by created_at descending
    jobs.sort(key=lambda x: x.get("created_at", ""), reverse=True)

    return {"jobs": jobs[:limit], "total": len(jobs)}


@app.delete("/jobs/{job_id}")
async def cancel_job(job_id: str):
    """Cancel a pending job."""
    job_data = await redis_client.hget(STATUS_HASH, job_id)

    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found")

    data = json.loads(job_data)
    if data["status"] not in ["queued", "pending"]:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel job in status: {data['status']}"
        )

    data["status"] = "cancelled"
    data["updated_at"] = datetime.utcnow().isoformat()

    await redis_client.hset(STATUS_HASH, job_id, json.dumps(data))

    return {"message": "Job cancelled", "job_id": job_id}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
