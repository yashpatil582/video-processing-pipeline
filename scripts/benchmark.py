#!/usr/bin/env python3
"""Performance benchmarking script for the video processing pipeline."""

import argparse
import asyncio
import json
import statistics
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import httpx


@dataclass
class BenchmarkResult:
    """Stores benchmark results."""
    total_jobs: int = 0
    successful_jobs: int = 0
    failed_jobs: int = 0
    total_duration_seconds: float = 0.0
    upload_latencies_ms: list = field(default_factory=list)
    processing_latencies_ms: list = field(default_factory=list)
    throughput_jobs_per_minute: float = 0.0


class PipelineBenchmark:
    """Benchmark the video processing pipeline."""

    def __init__(
        self,
        ingestion_url: str = "http://localhost:8000",
        api_url: str = "http://localhost:8001",
    ):
        self.ingestion_url = ingestion_url
        self.api_url = api_url

    async def upload_video(
        self,
        client: httpx.AsyncClient,
        video_path: Path
    ) -> tuple[Optional[str], float]:
        """Upload a video and return job_id and latency."""
        start = time.time()

        try:
            with open(video_path, "rb") as f:
                files = {"file": (video_path.name, f, "video/mp4")}
                response = await client.post(
                    f"{self.ingestion_url}/upload",
                    files=files,
                    timeout=60.0
                )

            latency = (time.time() - start) * 1000

            if response.status_code == 200:
                data = response.json()
                return data["job_id"], latency
            else:
                print(f"Upload failed: {response.status_code} - {response.text}")
                return None, latency

        except Exception as e:
            print(f"Upload error: {e}")
            return None, (time.time() - start) * 1000

    async def wait_for_job(
        self,
        client: httpx.AsyncClient,
        job_id: str,
        timeout: int = 300
    ) -> tuple[str, float]:
        """Wait for a job to complete and return status and processing time."""
        start = time.time()

        while time.time() - start < timeout:
            try:
                response = await client.get(f"{self.api_url}/jobs/{job_id}")

                if response.status_code == 200:
                    data = response.json()
                    status = data["status"]

                    if status in ["completed", "failed"]:
                        return status, (time.time() - start) * 1000

                await asyncio.sleep(1)

            except Exception as e:
                print(f"Error checking job status: {e}")
                await asyncio.sleep(1)

        return "timeout", (time.time() - start) * 1000

    async def run_benchmark(
        self,
        video_path: Path,
        num_jobs: int = 10,
        concurrent: int = 2
    ) -> BenchmarkResult:
        """
        Run the benchmark.

        Args:
            video_path: Path to test video
            num_jobs: Number of jobs to submit
            concurrent: Number of concurrent uploads

        Returns:
            BenchmarkResult with metrics
        """
        result = BenchmarkResult(total_jobs=num_jobs)
        start_time = time.time()

        async with httpx.AsyncClient() as client:
            # Upload videos in batches
            job_ids = []
            semaphore = asyncio.Semaphore(concurrent)

            async def upload_with_semaphore():
                async with semaphore:
                    job_id, latency = await self.upload_video(client, video_path)
                    result.upload_latencies_ms.append(latency)
                    return job_id

            print(f"Uploading {num_jobs} videos...")
            tasks = [upload_with_semaphore() for _ in range(num_jobs)]
            job_ids = await asyncio.gather(*tasks)
            job_ids = [jid for jid in job_ids if jid is not None]

            print(f"Uploaded {len(job_ids)} videos successfully")

            # Wait for all jobs to complete
            print("Waiting for jobs to complete...")

            async def wait_with_semaphore(job_id):
                async with semaphore:
                    status, latency = await self.wait_for_job(client, job_id)
                    result.processing_latencies_ms.append(latency)
                    return status

            tasks = [wait_with_semaphore(jid) for jid in job_ids]
            statuses = await asyncio.gather(*tasks)

            for status in statuses:
                if status == "completed":
                    result.successful_jobs += 1
                else:
                    result.failed_jobs += 1

        result.total_duration_seconds = time.time() - start_time
        result.throughput_jobs_per_minute = (
            result.successful_jobs / result.total_duration_seconds * 60
            if result.total_duration_seconds > 0 else 0
        )

        return result

    def print_report(self, result: BenchmarkResult):
        """Print a formatted benchmark report."""
        print("\n" + "=" * 60)
        print("VIDEO PROCESSING PIPELINE BENCHMARK REPORT")
        print("=" * 60)

        print(f"\nJOB SUMMARY")
        print(f"  Total Jobs:      {result.total_jobs}")
        print(f"  Successful:      {result.successful_jobs}")
        print(f"  Failed:          {result.failed_jobs}")
        print(f"  Success Rate:    {result.successful_jobs / result.total_jobs * 100:.1f}%")

        print(f"\nPERFORMANCE")
        print(f"  Total Duration:  {result.total_duration_seconds:.2f}s")
        print(f"  Throughput:      {result.throughput_jobs_per_minute:.2f} jobs/min")

        if result.upload_latencies_ms:
            print(f"\nUPLOAD LATENCY")
            print(f"  Min:    {min(result.upload_latencies_ms):.2f}ms")
            print(f"  Max:    {max(result.upload_latencies_ms):.2f}ms")
            print(f"  Mean:   {statistics.mean(result.upload_latencies_ms):.2f}ms")
            print(f"  P50:    {statistics.median(result.upload_latencies_ms):.2f}ms")
            if len(result.upload_latencies_ms) >= 20:
                sorted_lat = sorted(result.upload_latencies_ms)
                p95_idx = int(len(sorted_lat) * 0.95)
                print(f"  P95:    {sorted_lat[p95_idx]:.2f}ms")

        if result.processing_latencies_ms:
            print(f"\nPROCESSING LATENCY")
            print(f"  Min:    {min(result.processing_latencies_ms):.2f}ms")
            print(f"  Max:    {max(result.processing_latencies_ms):.2f}ms")
            print(f"  Mean:   {statistics.mean(result.processing_latencies_ms):.2f}ms")
            print(f"  P50:    {statistics.median(result.processing_latencies_ms):.2f}ms")
            if len(result.processing_latencies_ms) >= 20:
                sorted_lat = sorted(result.processing_latencies_ms)
                p95_idx = int(len(sorted_lat) * 0.95)
                print(f"  P95:    {sorted_lat[p95_idx]:.2f}ms")

        print("\n" + "=" * 60)


async def main():
    parser = argparse.ArgumentParser(description="Benchmark video processing pipeline")
    parser.add_argument(
        "--video",
        type=Path,
        required=True,
        help="Path to test video file"
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=10,
        help="Number of jobs to submit"
    )
    parser.add_argument(
        "--concurrent",
        type=int,
        default=2,
        help="Number of concurrent uploads"
    )
    parser.add_argument(
        "--ingestion-url",
        default="http://localhost:8000",
        help="Ingestion service URL"
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:8001",
        help="Status API URL"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output file for JSON results"
    )

    args = parser.parse_args()

    if not args.video.exists():
        print(f"Error: Video file not found: {args.video}")
        return

    benchmark = PipelineBenchmark(
        ingestion_url=args.ingestion_url,
        api_url=args.api_url
    )

    print(f"Starting benchmark with {args.jobs} jobs...")
    result = await benchmark.run_benchmark(
        args.video,
        num_jobs=args.jobs,
        concurrent=args.concurrent
    )

    benchmark.print_report(result)

    if args.output:
        with open(args.output, "w") as f:
            json.dump({
                "total_jobs": result.total_jobs,
                "successful_jobs": result.successful_jobs,
                "failed_jobs": result.failed_jobs,
                "total_duration_seconds": result.total_duration_seconds,
                "throughput_jobs_per_minute": result.throughput_jobs_per_minute,
                "upload_latencies_ms": result.upload_latencies_ms,
                "processing_latencies_ms": result.processing_latencies_ms,
            }, f, indent=2)
        print(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    asyncio.run(main())
