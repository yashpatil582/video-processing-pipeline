"""Status API - Query job status and system metrics."""

import json
from collections import defaultdict
from datetime import datetime
from typing import Optional

import redis.asyncio as redis
import structlog
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response


# Configuration
class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379"
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
API_REQUESTS = Counter(
    'api_requests_total',
    'Total API requests',
    ['endpoint', 'method', 'status']
)

# FastAPI app
app = FastAPI(
    title="Video Processing Status API",
    description="Query job status and system metrics",
    version="1.0.0"
)

# Redis connection
redis_client: Optional[redis.Redis] = None

# Stream/Hash names
PROCESSING_STREAM = "video:processing"
DEAD_LETTER_STREAM = "video:dlq"
STATUS_HASH = "video:status"


class JobStatus(BaseModel):
    job_id: str
    status: str
    filename: str
    created_at: str
    updated_at: str
    error: Optional[str] = None
    outputs: list = Field(default_factory=list)
    file_size: Optional[int] = None


class SystemStats(BaseModel):
    queue_depth: int
    dlq_depth: int
    total_jobs: int
    jobs_by_status: dict
    processing_rate_per_minute: float


class QueueInfo(BaseModel):
    stream_length: int
    consumer_groups: list
    pending_messages: int


@app.on_event("startup")
async def startup():
    """Initialize Redis connection on startup."""
    global redis_client
    redis_client = redis.from_url(settings.redis_url, decode_responses=True)
    await logger.ainfo("Status API started", redis_url=settings.redis_url)


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
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


@app.get("/jobs/{job_id}", response_model=JobStatus)
async def get_job(job_id: str):
    """Get detailed status of a specific job."""
    API_REQUESTS.labels(endpoint="/jobs/{job_id}", method="GET", status="200").inc()

    job_data = await redis_client.hget(STATUS_HASH, job_id)

    if not job_data:
        API_REQUESTS.labels(endpoint="/jobs/{job_id}", method="GET", status="404").inc()
        raise HTTPException(status_code=404, detail="Job not found")

    data = json.loads(job_data)
    return JobStatus(**data)


@app.get("/jobs")
async def list_jobs(
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """
    List all jobs with optional filtering.

    Args:
        status: Filter by status (queued, processing, completed, failed)
        limit: Maximum number of results
        offset: Number of results to skip
    """
    API_REQUESTS.labels(endpoint="/jobs", method="GET", status="200").inc()

    all_jobs = await redis_client.hgetall(STATUS_HASH)

    jobs = []
    for job_id, job_data in all_jobs.items():
        data = json.loads(job_data)
        if status is None or data.get("status") == status:
            jobs.append(data)

    # Sort by created_at descending
    jobs.sort(key=lambda x: x.get("created_at", ""), reverse=True)

    # Apply pagination
    paginated = jobs[offset:offset + limit]

    return {
        "jobs": paginated,
        "total": len(jobs),
        "limit": limit,
        "offset": offset
    }


@app.get("/stats", response_model=SystemStats)
async def get_system_stats():
    """Get system-wide statistics."""
    API_REQUESTS.labels(endpoint="/stats", method="GET", status="200").inc()

    # Get queue depths
    try:
        queue_depth = await redis_client.xlen(PROCESSING_STREAM)
    except Exception:
        queue_depth = 0

    try:
        dlq_depth = await redis_client.xlen(DEAD_LETTER_STREAM)
    except Exception:
        dlq_depth = 0

    # Get job statistics
    all_jobs = await redis_client.hgetall(STATUS_HASH)
    jobs_by_status = defaultdict(int)

    for job_data in all_jobs.values():
        data = json.loads(job_data)
        jobs_by_status[data.get("status", "unknown")] += 1

    # Calculate processing rate (simplified - just count completed in last hour)
    completed_last_hour = 0
    now = datetime.utcnow()

    for job_data in all_jobs.values():
        data = json.loads(job_data)
        if data.get("status") == "completed":
            try:
                updated = datetime.fromisoformat(data.get("updated_at", ""))
                if (now - updated).total_seconds() < 3600:
                    completed_last_hour += 1
            except Exception:
                pass

    processing_rate = completed_last_hour / 60.0  # per minute

    return SystemStats(
        queue_depth=queue_depth,
        dlq_depth=dlq_depth,
        total_jobs=len(all_jobs),
        jobs_by_status=dict(jobs_by_status),
        processing_rate_per_minute=processing_rate
    )


@app.get("/queue", response_model=QueueInfo)
async def get_queue_info():
    """Get detailed queue information."""
    API_REQUESTS.labels(endpoint="/queue", method="GET", status="200").inc()

    try:
        stream_length = await redis_client.xlen(PROCESSING_STREAM)
    except Exception:
        stream_length = 0

    # Get consumer group info
    consumer_groups = []
    try:
        groups = await redis_client.xinfo_groups(PROCESSING_STREAM)
        for group in groups:
            consumer_groups.append({
                "name": group.get("name"),
                "consumers": group.get("consumers"),
                "pending": group.get("pending"),
                "last_delivered_id": group.get("last-delivered-id")
            })
    except Exception:
        pass

    # Calculate total pending
    total_pending = sum(g.get("pending", 0) for g in consumer_groups)

    return QueueInfo(
        stream_length=stream_length,
        consumer_groups=consumer_groups,
        pending_messages=total_pending
    )


@app.get("/dlq")
async def get_dead_letter_queue(limit: int = 50):
    """Get messages from the dead letter queue."""
    API_REQUESTS.labels(endpoint="/dlq", method="GET", status="200").inc()

    try:
        messages = await redis_client.xrange(
            DEAD_LETTER_STREAM,
            count=limit
        )

        return {
            "messages": [
                {"id": msg_id, "data": data}
                for msg_id, data in messages
            ],
            "total": await redis_client.xlen(DEAD_LETTER_STREAM)
        }
    except Exception:
        return {"messages": [], "total": 0}


@app.post("/dlq/{message_id}/retry")
async def retry_dlq_message(message_id: str):
    """Retry a message from the dead letter queue."""
    API_REQUESTS.labels(endpoint="/dlq/{message_id}/retry", method="POST", status="200").inc()

    try:
        # Get the message from DLQ
        messages = await redis_client.xrange(
            DEAD_LETTER_STREAM,
            min=message_id,
            max=message_id
        )

        if not messages:
            raise HTTPException(status_code=404, detail="Message not found in DLQ")

        msg_id, data = messages[0]
        job_id = data.get("job_id")

        # Get original job data
        job_data = await redis_client.hget(STATUS_HASH, job_id)
        if job_data:
            job = json.loads(job_data)

            # Re-queue the job
            await redis_client.xadd(
                PROCESSING_STREAM,
                {
                    "job_id": job_id,
                    "file_path": job.get("file_path", ""),
                    "filename": job.get("filename", ""),
                    "retry": "true"
                }
            )

            # Update job status
            job["status"] = "queued"
            job["error"] = None
            job["updated_at"] = datetime.utcnow().isoformat()
            await redis_client.hset(STATUS_HASH, job_id, json.dumps(job))

            # Remove from DLQ
            await redis_client.xdel(DEAD_LETTER_STREAM, message_id)

            return {"message": "Job re-queued", "job_id": job_id}

        raise HTTPException(status_code=404, detail="Original job not found")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
