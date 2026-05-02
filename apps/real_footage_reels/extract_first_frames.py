import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Extract sampled frames from local video files.")
    parser.add_argument("--input", required=True, help="Path to the JSON manifest containing downloaded videos.")
    parser.add_argument("--output-dir", required=True, help="Directory where JPEG frames will be written.")
    parser.add_argument("--output-manifest", required=True, help="Path to write the frame manifest JSON.")
    parser.add_argument("--ffmpeg", default="ffmpeg", help="Path to ffmpeg executable.")
    parser.add_argument("--shots", type=int, default=3, help="Number of sampled frames to extract per clip.")
    parser.add_argument(
        "--skip-start-seconds",
        type=float,
        default=3.0,
        help="Avoid sampling from the first N seconds of each source clip when possible.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_manifest_path = Path(args.output_manifest)

    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = json.loads(input_path.read_text(encoding="utf-8"))
    videos = manifest.get("videos", [])
    output_videos = []

    for item in videos:
        video_path = Path(item["videoPath"])
        sample_offsets = build_sample_offsets(
            probe_duration(resolve_ffprobe_path(args.ffmpeg), video_path),
            max(args.shots, 1),
            max(args.skip_start_seconds, 0.0),
        )
        frame_paths = []

        for index, offset_seconds in enumerate(sample_offsets):
            candidate_path = output_dir / f"{video_path.stem}-{index + 1:02d}.jpg"
            extract_frame(args.ffmpeg, video_path, candidate_path, offset_seconds)
            frame_paths.append(candidate_path)

        enriched = dict(item)
        preview_index = len(frame_paths) // 2
        enriched["framePath"] = str(frame_paths[preview_index])
        enriched["framePaths"] = [str(frame_path) for frame_path in frame_paths]
        enriched["sampledAtSeconds"] = sample_offsets
        output_videos.append(enriched)

        print(f"Extracted {len(frame_paths)} sampled frame(s): {video_path.name}")

    output_manifest = {
        "createdAt": manifest.get("createdAt"),
        "videos": output_videos,
    }
    output_manifest_path.write_text(
        json.dumps(output_manifest, indent=2) + "\n",
        encoding="utf-8",
    )


def extract_frame(ffmpeg_path: str, video_path: Path, frame_path: Path, offset_seconds: float):
    command = [
        ffmpeg_path,
        "-y",
        "-ss",
        f"{offset_seconds:.3f}",
        "-i",
        str(video_path),
        "-frames:v",
        "1",
        "-q:v",
        "2",
        str(frame_path),
    ]

    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if completed.returncode != 0:
        raise RuntimeError(
            f"ffmpeg failed for {video_path}\n{completed.stderr or completed.stdout}"
        )


def resolve_ffprobe_path(ffmpeg_path: str) -> str:
    ffmpeg_candidate = Path(ffmpeg_path)
    if ffmpeg_candidate.name:
        probe_name = "ffprobe.exe" if ffmpeg_candidate.suffix.lower() == ".exe" else "ffprobe"
        probe_path = ffmpeg_candidate.with_name(probe_name)
        if probe_path.exists():
            return str(probe_path)
    return "ffprobe"


def probe_duration(ffprobe_path: str, video_path: Path) -> float:
    command = [
        ffprobe_path,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(video_path),
    ]
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.returncode != 0:
        return 0.0

    try:
        return max(float(completed.stdout.strip() or "0"), 0.0)
    except ValueError:
        return 0.0


def build_sample_offsets(duration_seconds: float, shots: int, skip_start_seconds: float):
    if duration_seconds <= 0.2:
        return [0.0]
    effective_end = max(duration_seconds - min(0.15, duration_seconds * 0.1), 0.0)
    if effective_end <= 0.0:
        return [0.0]

    effective_start = min(max(skip_start_seconds, 0.0), effective_end)
    if shots <= 1 or abs(effective_end - effective_start) < 0.05:
        return [round(effective_start, 3)]

    offsets = []
    for index in range(shots):
        raw_offset = effective_start + (effective_end - effective_start) * index / max(shots - 1, 1)
        rounded_offset = round(max(raw_offset, 0.0), 3)
        if not offsets or abs(offsets[-1] - rounded_offset) >= 0.05:
            offsets.append(rounded_offset)

    if not offsets:
        return [0.0]

    return offsets


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # pragma: no cover
        print(str(error), file=sys.stderr)
        sys.exit(1)
