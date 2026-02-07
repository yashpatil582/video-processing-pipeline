"""Task definitions for video processing."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class OutputFormat(Enum):
    """Supported output formats."""
    MP4_720P = "720p"
    MP4_480P = "480p"
    MP4_360P = "360p"
    THUMBNAIL = "thumbnail"
    WEBM_720P = "webm_720p"
    HLS = "hls"


@dataclass
class TranscodeProfile:
    """Transcoding profile configuration."""
    name: str
    format: OutputFormat
    video_codec: str = "libx264"
    audio_codec: str = "aac"
    video_bitrate: Optional[str] = None
    audio_bitrate: str = "128k"
    width: Optional[int] = None
    height: Optional[int] = None
    fps: Optional[int] = None
    preset: str = "medium"
    crf: int = 23
    extra_args: list = field(default_factory=list)


# Default transcoding profiles
TRANSCODE_PROFILES = {
    OutputFormat.MP4_720P: TranscodeProfile(
        name="720p",
        format=OutputFormat.MP4_720P,
        height=720,
        video_bitrate="2500k",
        crf=23,
    ),
    OutputFormat.MP4_480P: TranscodeProfile(
        name="480p",
        format=OutputFormat.MP4_480P,
        height=480,
        video_bitrate="1500k",
        crf=24,
    ),
    OutputFormat.MP4_360P: TranscodeProfile(
        name="360p",
        format=OutputFormat.MP4_360P,
        height=360,
        video_bitrate="800k",
        crf=25,
    ),
    OutputFormat.THUMBNAIL: TranscodeProfile(
        name="thumbnail",
        format=OutputFormat.THUMBNAIL,
        width=320,
        height=180,
        extra_args=["-vframes", "1", "-ss", "00:00:01"],
    ),
    OutputFormat.WEBM_720P: TranscodeProfile(
        name="webm_720p",
        format=OutputFormat.WEBM_720P,
        video_codec="libvpx-vp9",
        audio_codec="libopus",
        height=720,
        video_bitrate="2000k",
        crf=30,
    ),
}


def build_ffmpeg_command(
    input_path: str,
    output_path: str,
    profile: TranscodeProfile,
) -> list[str]:
    """
    Build FFmpeg command from a transcoding profile.

    Args:
        input_path: Path to input video
        output_path: Path for output file
        profile: Transcoding profile to use

    Returns:
        List of command arguments
    """
    cmd = ["ffmpeg", "-y", "-i", input_path]

    # Handle thumbnail separately
    if profile.format == OutputFormat.THUMBNAIL:
        cmd.extend(["-vf", f"scale={profile.width}:{profile.height}"])
        cmd.extend(profile.extra_args)
        cmd.append(output_path)
        return cmd

    # Video codec and settings
    cmd.extend(["-c:v", profile.video_codec])

    # Scale filter for resolution
    if profile.height:
        cmd.extend(["-vf", f"scale=-2:{profile.height}"])

    # Video bitrate or CRF
    if profile.video_bitrate:
        cmd.extend(["-b:v", profile.video_bitrate])
    if profile.crf:
        cmd.extend(["-crf", str(profile.crf)])

    # Preset
    if profile.preset and profile.video_codec in ["libx264", "libx265"]:
        cmd.extend(["-preset", profile.preset])

    # Audio settings
    cmd.extend(["-c:a", profile.audio_codec])
    cmd.extend(["-b:a", profile.audio_bitrate])

    # FPS if specified
    if profile.fps:
        cmd.extend(["-r", str(profile.fps)])

    # Extra arguments
    cmd.extend(profile.extra_args)

    # Output
    cmd.append(output_path)

    return cmd


def get_output_extension(profile: TranscodeProfile) -> str:
    """Get the output file extension for a profile."""
    if profile.format == OutputFormat.THUMBNAIL:
        return ".jpg"
    elif profile.format == OutputFormat.WEBM_720P:
        return ".webm"
    elif profile.format == OutputFormat.HLS:
        return ".m3u8"
    else:
        return ".mp4"
