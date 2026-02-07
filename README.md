# Scalable Video Processing Pipeline

A containerized video ingestion and processing pipeline with message queuing, failure handling, and observability.

## Architecture

```
┌──────────────┐     ┌─────────────┐     ┌──────────────────┐
│   Ingestion  │────▶│   Redis     │────▶│  Worker Pool     │
│   Service    │     │   Streams   │     │  (FFmpeg jobs)   │
└──────────────┘     └─────────────┘     └────────┬─────────┘
                                                  │
                     ┌─────────────┐     ┌────────▼─────────┐
                     │  Prometheus │◀────│   Output Store   │
                     │  + Grafana  │     │   (Local/S3)     │
                     └─────────────┘     └──────────────────┘
```

## Features

- **REST API for Video Upload**: Accept video files or URLs
- **Async Processing Queue**: Redis Streams for reliable message delivery
- **Multi-Format Transcoding**: Generate 720p, 480p, and thumbnails
- **Retry Logic with DLQ**: Automatic retries with dead letter queue
- **Processing Status Tracking**: Real-time job status API
- **Structured Logging**: JSON logging for easy parsing
- **Prometheus Metrics**: Processing throughput, latency, error rates

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.10+ (for running tests/benchmarks locally)

### Starting the Pipeline

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

### Service Endpoints

| Service | Port | Description |
|---------|------|-------------|
| Ingestion API | 8000 | Upload videos |
| Status API | 8001 | Query job status |
| Prometheus | 9090 | Metrics collection |
| Grafana | 3000 | Metrics dashboard (admin/admin) |
| Redis | 6379 | Message queue |

## API Usage

### Upload a Video

```bash
# Upload from file
curl -X POST http://localhost:8000/upload \
  -F "file=@/path/to/video.mp4"

# Upload from URL
curl -X POST http://localhost:8000/upload-url \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com/video.mp4"}'
```

Response:
```json
{
  "job_id": "abc123-def456",
  "filename": "video.mp4",
  "status": "queued",
  "message": "Video uploaded and queued for processing",
  "created_at": "2024-01-01T00:00:00Z"
}
```

### Check Job Status

```bash
curl http://localhost:8001/jobs/{job_id}
```

Response:
```json
{
  "job_id": "abc123-def456",
  "status": "completed",
  "filename": "video.mp4",
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-01T00:01:30Z",
  "outputs": [
    {"profile": "720p", "path": "/output/abc123/720p.mp4"},
    {"profile": "480p", "path": "/output/abc123/480p.mp4"},
    {"profile": "thumbnail", "path": "/output/abc123/thumbnail.jpg"}
  ]
}
```

### System Statistics

```bash
curl http://localhost:8001/stats
```

Response:
```json
{
  "queue_depth": 5,
  "dlq_depth": 0,
  "total_jobs": 100,
  "jobs_by_status": {
    "completed": 95,
    "failed": 2,
    "processing": 3
  },
  "processing_rate_per_minute": 2.5
}
```

## Project Structure

```
video-processing-pipeline/
├── docker-compose.yml
├── services/
│   ├── ingestion/
│   │   ├── Dockerfile
│   │   ├── main.py              # FastAPI ingestion service
│   │   └── requirements.txt
│   ├── worker/
│   │   ├── Dockerfile
│   │   ├── processor.py         # FFmpeg worker
│   │   ├── tasks.py             # Task definitions
│   │   └── requirements.txt
│   └── api/
│       ├── Dockerfile
│       ├── main.py              # Status API
│       └── requirements.txt
├── config/
│   └── prometheus.yml
├── scripts/
│   └── benchmark.py             # Performance benchmarking
├── tests/
│   └── test_pipeline.py
└── README.md
```

## Transcoding Profiles

| Profile | Resolution | Video Bitrate | Codec |
|---------|------------|---------------|-------|
| 720p | 1280x720 | 2500 kbps | H.264 |
| 480p | 854x480 | 1500 kbps | H.264 |
| 360p | 640x360 | 800 kbps | H.264 |
| thumbnail | 320x180 | N/A | JPEG |
| webm_720p | 1280x720 | 2000 kbps | VP9 |

## Scaling

### Horizontal Scaling

Increase worker replicas in docker-compose.yml:

```yaml
worker:
  deploy:
    replicas: 4
```

### Configuration

Environment variables for tuning:

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_CONCURRENCY` | 2 | Jobs per worker |
| `MAX_FILE_SIZE` | 500MB | Max upload size |
| `REDIS_URL` | redis://redis:6379 | Redis connection |

## Monitoring

### Prometheus Metrics

- `video_uploads_total`: Total uploads by status
- `video_upload_size_bytes`: Upload size histogram
- `video_jobs_processed_total`: Jobs processed by status/profile
- `video_processing_duration_seconds`: Processing time histogram
- `video_active_jobs`: Currently processing jobs
- `processing_queue_size`: Queue depth

### Grafana Dashboard

Access Grafana at http://localhost:3000 (admin/admin)

Recommended panels:
1. Upload rate (uploads/min)
2. Processing throughput (jobs/min)
3. Queue depth over time
4. Error rate by profile
5. P95 processing latency

## Benchmarking

Run the benchmark script:

```bash
# Install dependencies
pip install httpx

# Run benchmark
python scripts/benchmark.py \
  --video /path/to/test.mp4 \
  --jobs 20 \
  --concurrent 4 \
  --output results.json
```

Sample output:
```
============================================================
VIDEO PROCESSING PIPELINE BENCHMARK REPORT
============================================================

JOB SUMMARY
  Total Jobs:      20
  Successful:      20
  Failed:          0
  Success Rate:    100.0%

PERFORMANCE
  Total Duration:  180.50s
  Throughput:      6.65 jobs/min

UPLOAD LATENCY
  Min:    45.23ms
  Max:    892.15ms
  Mean:   234.67ms
  P50:    189.45ms
  P95:    756.32ms

PROCESSING LATENCY
  Min:    5234.12ms
  Max:    15678.90ms
  Mean:   8456.78ms
  P50:    7890.12ms
  P95:    14567.89ms
============================================================
```

## Testing

```bash
# Run unit tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=services --cov-report=html
```

## Error Handling

### Dead Letter Queue

Failed jobs are moved to a DLQ for investigation:

```bash
# View DLQ messages
curl http://localhost:8001/dlq

# Retry a failed job
curl -X POST http://localhost:8001/dlq/{message_id}/retry
```

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `File too large` | Exceeds MAX_FILE_SIZE | Increase limit or compress video |
| `Invalid file type` | Unsupported format | Use mp4, mov, avi, mkv, or webm |
| `FFmpeg timeout` | Video too long/complex | Increase timeout or reduce quality |

## Development

### Local Development

```bash
# Start Redis only
docker-compose up redis -d

# Run services locally
cd services/ingestion && uvicorn main:app --reload --port 8000
cd services/api && uvicorn main:app --reload --port 8001
cd services/worker && python processor.py
```

### Adding New Profiles

Edit `services/worker/tasks.py`:

```python
TRANSCODE_PROFILES[OutputFormat.CUSTOM] = TranscodeProfile(
    name="custom",
    format=OutputFormat.CUSTOM,
    height=1080,
    video_bitrate="5000k",
    crf=20,
)
```

## License

MIT License - see LICENSE file for details.
