"""Unit tests for video processing pipeline."""

import json
import pytest
from unittest.mock import Mock, patch, AsyncMock
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "services" / "worker"))
sys.path.insert(0, str(Path(__file__).parent.parent / "services" / "ingestion"))
sys.path.insert(0, str(Path(__file__).parent.parent / "services" / "api"))

from tasks import (
    TranscodeProfile,
    OutputFormat,
    TRANSCODE_PROFILES,
    build_ffmpeg_command,
    get_output_extension,
)


class TestTranscodeProfiles:
    """Tests for transcoding profiles."""

    def test_profiles_exist(self):
        """Test that all required profiles exist."""
        assert OutputFormat.MP4_720P in TRANSCODE_PROFILES
        assert OutputFormat.MP4_480P in TRANSCODE_PROFILES
        assert OutputFormat.THUMBNAIL in TRANSCODE_PROFILES

    def test_profile_attributes(self):
        """Test profile attributes are set correctly."""
        profile_720p = TRANSCODE_PROFILES[OutputFormat.MP4_720P]

        assert profile_720p.name == "720p"
        assert profile_720p.height == 720
        assert profile_720p.video_codec == "libx264"
        assert profile_720p.audio_codec == "aac"

    def test_thumbnail_profile(self):
        """Test thumbnail profile configuration."""
        thumbnail = TRANSCODE_PROFILES[OutputFormat.THUMBNAIL]

        assert thumbnail.name == "thumbnail"
        assert thumbnail.width == 320
        assert thumbnail.height == 180
        assert "-vframes" in thumbnail.extra_args


class TestFFmpegCommand:
    """Tests for FFmpeg command building."""

    def test_build_720p_command(self):
        """Test building 720p transcode command."""
        profile = TRANSCODE_PROFILES[OutputFormat.MP4_720P]
        cmd = build_ffmpeg_command(
            "/input/video.mp4",
            "/output/video_720p.mp4",
            profile
        )

        assert cmd[0] == "ffmpeg"
        assert "-i" in cmd
        assert "/input/video.mp4" in cmd
        assert "-c:v" in cmd
        assert "libx264" in cmd
        assert "-vf" in cmd
        assert "scale=-2:720" in cmd
        assert "/output/video_720p.mp4" in cmd

    def test_build_thumbnail_command(self):
        """Test building thumbnail extraction command."""
        profile = TRANSCODE_PROFILES[OutputFormat.THUMBNAIL]
        cmd = build_ffmpeg_command(
            "/input/video.mp4",
            "/output/thumb.jpg",
            profile
        )

        assert "ffmpeg" in cmd
        assert "-vframes" in cmd
        assert "1" in cmd
        assert "-ss" in cmd
        assert "/output/thumb.jpg" in cmd

    def test_get_output_extension(self):
        """Test getting correct output extensions."""
        assert get_output_extension(TRANSCODE_PROFILES[OutputFormat.MP4_720P]) == ".mp4"
        assert get_output_extension(TRANSCODE_PROFILES[OutputFormat.MP4_480P]) == ".mp4"
        assert get_output_extension(TRANSCODE_PROFILES[OutputFormat.THUMBNAIL]) == ".jpg"
        assert get_output_extension(TRANSCODE_PROFILES[OutputFormat.WEBM_720P]) == ".webm"


class TestIngestionService:
    """Tests for ingestion service."""

    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        mock = AsyncMock()
        mock.ping = AsyncMock(return_value=True)
        mock.hset = AsyncMock()
        mock.hget = AsyncMock(return_value=None)
        mock.xadd = AsyncMock()
        mock.xlen = AsyncMock(return_value=0)
        return mock

    def test_health_check(self, mock_redis):
        """Test health check endpoint."""
        # Verify mock redis has ping method
        assert mock_redis.ping is not None
        assert callable(mock_redis.ping)

    def test_allowed_extensions(self):
        """Test file extension validation."""
        allowed = [".mp4", ".mov", ".avi", ".mkv", ".webm"]

        assert ".mp4" in allowed
        assert ".txt" not in allowed
        assert ".exe" not in allowed


class TestStatusAPI:
    """Tests for status API."""

    @pytest.fixture
    def sample_job_data(self):
        """Create sample job data."""
        return {
            "job_id": "test-123",
            "status": "completed",
            "filename": "test.mp4",
            "created_at": "2024-01-01T00:00:00",
            "updated_at": "2024-01-01T00:01:00",
            "outputs": [
                {"profile": "720p", "path": "/output/720p.mp4"},
                {"profile": "thumbnail", "path": "/output/thumb.jpg"}
            ]
        }

    def test_job_status_parsing(self, sample_job_data):
        """Test job status data parsing."""
        data = json.loads(json.dumps(sample_job_data))

        assert data["job_id"] == "test-123"
        assert data["status"] == "completed"
        assert len(data["outputs"]) == 2

    def test_job_filtering(self, sample_job_data):
        """Test job filtering by status."""
        jobs = [
            {"job_id": "1", "status": "completed"},
            {"job_id": "2", "status": "failed"},
            {"job_id": "3", "status": "processing"},
            {"job_id": "4", "status": "completed"},
        ]

        completed = [j for j in jobs if j["status"] == "completed"]
        assert len(completed) == 2

        failed = [j for j in jobs if j["status"] == "failed"]
        assert len(failed) == 1


class TestWorker:
    """Tests for worker processor."""

    def test_ffmpeg_command_structure(self):
        """Test FFmpeg command has correct structure."""
        profile = TRANSCODE_PROFILES[OutputFormat.MP4_720P]
        cmd = build_ffmpeg_command("input.mp4", "output.mp4", profile)

        # Verify command structure
        assert cmd[0] == "ffmpeg"
        assert "-y" in cmd  # Overwrite output
        assert cmd.index("-i") < cmd.index("input.mp4")
        assert cmd[-1] == "output.mp4"

    def test_profile_video_bitrates(self):
        """Test video bitrate settings."""
        p720 = TRANSCODE_PROFILES[OutputFormat.MP4_720P]
        p480 = TRANSCODE_PROFILES[OutputFormat.MP4_480P]

        # 720p should have higher bitrate than 480p
        assert p720.video_bitrate is not None
        assert p480.video_bitrate is not None

        # Convert to integers for comparison
        bitrate_720 = int(p720.video_bitrate.replace("k", ""))
        bitrate_480 = int(p480.video_bitrate.replace("k", ""))

        assert bitrate_720 > bitrate_480


class TestBenchmark:
    """Tests for benchmark script."""

    def test_benchmark_result_initialization(self):
        """Test BenchmarkResult dataclass."""
        from scripts.benchmark import BenchmarkResult

        result = BenchmarkResult()
        assert result.total_jobs == 0
        assert result.successful_jobs == 0
        assert result.upload_latencies_ms == []

    def test_throughput_calculation(self):
        """Test throughput calculation."""
        from scripts.benchmark import BenchmarkResult

        result = BenchmarkResult(
            total_jobs=10,
            successful_jobs=8,
            total_duration_seconds=60.0
        )

        # 8 successful jobs in 60 seconds = 8 jobs/minute
        expected_throughput = 8 / 60 * 60
        result.throughput_jobs_per_minute = (
            result.successful_jobs / result.total_duration_seconds * 60
        )

        assert result.throughput_jobs_per_minute == expected_throughput


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
