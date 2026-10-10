"""Original local media workload replicas; optional dependencies, no network.

Only pass tasks produced by prepare(). Tasks contain immutable fixture paths and
JSON values. Each invocation owns temporary outputs, so backends can reuse tasks.
"""
from __future__ import annotations

from contextlib import ExitStack, closing
import hashlib
import importlib.util
import io
import os
from pathlib import Path
import queue
import random
import shutil
import subprocess
import tempfile
import threading
import time

NAMES = ("images", "ocr", "video")
MAX_COUNT = 256
MAX_INPUT_BYTES = 16 * 1024 * 1024
MAX_IMAGE_PIXELS = 4_000_000
MAX_OCR_PAGES = 3
MAX_RENDER_SIDE = 2400
MAX_VIDEO_FRAMES = 48
MAX_VIDEO_BYTES = 64 * 1024 * 1024
VIDEO_READ_BYTES = 64 * 1024
VIDEO_STDERR_BYTES = 2000
SCALES = ("smoke", "representative")
IMAGE_PROFILES = ((640, 480, "JPEG", 1), (1600, 1200, "JPEG", 6),
                  (2048, 1536, "PNG", 3), (2560, 1440, "BMP", 1),
                  (1200, 1800, "JPEG", 8), (1920, 1080, "PNG", 1))
OCR_PROFILES = ((1200, 900, 1, 6), (1275, 1650, 2, 10),
                (1654, 2339, 3, 16), (1650, 1275, 2, 8))
OCR_FONT_NAME = "DejaVuSans-Bold.ttf"
OCR_FIXTURE_REVISION = "bold42-exact-text-v2"
VIDEO_PROFILES = ((320, 180, 12, "ffv1"), (640, 360, 24, "png"),
                  (640, 360, 48, "ffv1"), (480, 270, 36, "png"))


class MissingDependency(RuntimeError):
    """The workload should be reported as skipped, not simulated."""


def _require(name: str) -> None:
    if name not in NAMES:
        raise ValueError(f"unknown media workload: {name}")
    if importlib.util.find_spec("PIL") is None:
        raise MissingDependency("Pillow is required; install Pillow in a virtualenv")
    for binary in {"images": (), "ocr": ("pdftoppm", "pdfinfo", "tesseract"), "video": ("ffmpeg",)}[name]:
        if shutil.which(binary) is None:
            raise MissingDependency(f"{binary} is required for {name}; see docs/workloads-media.md")
    if name == "ocr":
        languages = _command(["tesseract", "--list-langs"]).decode().splitlines()
        if "eng" not in languages:
            raise MissingDependency("Tesseract English traineddata (eng) is required")


def _command(args: list[str]) -> bytes:
    env = dict(os.environ, OMP_THREAD_LIMIT="1", OMP_NUM_THREADS="1", LC_ALL="C")
    completed = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               timeout=60, check=False, env=env)
    if completed.returncode:
        raise RuntimeError(f"{args[0]} failed ({completed.returncode}): "
                           + completed.stderr.decode(errors="replace")[-2000:])
    return completed.stdout


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _ffmpeg() -> list[str]:
    return ["ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
            "-threads", "1", "-filter_threads", "1", "-filter_complex_threads", "1"]


def _stream_video(args: list[str], frame_bytes: int, frames: int,
                  expected_frames: list[str], consume_frame, timeout: float = 60) -> str:
    """Hash decoded RGB in order, retaining one frame and bounded pipe chunks.

    ``consume_frame`` must finish using the borrowed frame buffer before it
    returns. Both pipes are drained concurrently, including on Windows where
    selectors cannot monitor subprocess pipes. A full queue is interruptible
    so consumer errors cannot strand a reader during cleanup.

    Only use this with the controlled FFmpeg command: cleanup assumes that no
    descendant inherits a pipe writer. The deadline is cooperative around the
    frame callback and cleanup, not a hard wall-time limit for the whole task.
    """
    if (not 1 <= frames <= MAX_VIDEO_FRAMES or frame_bytes < 1
            or frame_bytes * (frames + 1) > MAX_VIDEO_BYTES):
        raise ValueError("video fixture dimensions/frame cap mismatch")
    env = dict(os.environ, OMP_THREAD_LIMIT="1", OMP_NUM_THREADS="1", LC_ALL="C")
    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               bufsize=0, env=env)
    deadline = time.monotonic() + timeout
    events = queue.Queue(maxsize=2)
    stopped = threading.Event()
    readers = []

    def emit(event):
        while not stopped.is_set():
            try:
                events.put(event, timeout=0.05)
                return
            except queue.Full:
                pass

    def read_pipe(kind, pipe):
        try:
            while not stopped.is_set():
                chunk = pipe.read(VIDEO_READ_BYTES)
                emit((kind, chunk))
                if not chunk:
                    return
        except BaseException as exc:
            emit((kind, exc))

    def remaining():
        duration = deadline - time.monotonic()
        if duration <= 0:
            raise subprocess.TimeoutExpired(args, timeout)
        return duration

    try:
        for kind, pipe in (("stdout", process.stdout), ("stderr", process.stderr)):
            reader = threading.Thread(target=read_pipe, args=(kind, pipe),
                                      name=f"media-video-{kind}", daemon=True)
            reader.start()
            readers.append(reader)
        frame = bytearray(frame_bytes)
        frame_view = memoryview(frame)
        filled = total = 0
        decoded = []
        digest = hashlib.sha256()
        stderr = bytearray()
        open_pipes = 2
        while open_pipes:
            try:
                kind, chunk = events.get(timeout=remaining())
            except queue.Empty:
                raise subprocess.TimeoutExpired(args, timeout) from None
            if isinstance(chunk, BaseException):
                raise chunk
            if not chunk:
                open_pipes -= 1
                continue
            if kind == "stderr":
                stderr.extend(chunk)
                del stderr[:-VIDEO_STDERR_BYTES]
                continue
            total += len(chunk)
            if total > frame_bytes * frames or total > MAX_VIDEO_BYTES:
                raise ValueError("decoded video frame count mismatch")
            digest.update(chunk)
            chunk_view = memoryview(chunk)
            offset = 0
            while offset < len(chunk):
                take = min(frame_bytes - filled, len(chunk) - offset)
                frame_view[filled:filled + take] = chunk_view[offset:offset + take]
                offset += take
                filled += take
                if filled == frame_bytes:
                    frame_digest = _sha(frame)
                    consume_frame(len(decoded), frame, frame_digest)
                    decoded.append(frame_digest)
                    filled = 0
        returncode = process.wait(timeout=remaining())
        if returncode:
            raise RuntimeError(f"{args[0]} failed ({returncode}): "
                               + stderr.decode(errors="replace"))
        if filled or len(decoded) != frames:
            raise ValueError("decoded video frame count mismatch")
        if decoded != expected_frames:
            raise ValueError("lossless decoded video pixels differ from fixture oracle")
        return digest.hexdigest()
    finally:
        stopped.set()
        try:
            if process.poll() is None:
                process.kill()
            process.wait()
        finally:
            for reader in readers:
                reader.join()
            process.stdout.close()
            process.stderr.close()


def prepare(name: str, root: Path, count: int, seed: int,
            scale: str = "smoke") -> list[dict]:
    """Generate bounded, seeded fixtures outside the timed executor region.

    Representative profiles vary per-task costs; smoke preserves the original
    fast compatibility fixtures. Neither is a production data distribution.
    """
    if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= MAX_COUNT:
        raise ValueError(f"count must be an integer in [1, {MAX_COUNT}]")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("seed must be an integer")
    if scale not in SCALES:
        raise ValueError(f"scale must be one of {SCALES}")
    _require(name)
    from PIL import Image, ImageDraw, ImageFont
    root = Path(root).resolve() / name
    root.mkdir(parents=True, exist_ok=True)
    tasks = []
    for index in range(count):
        rng = random.Random(f"media-v1:{seed}:{index}")
        task = {"name": name, "index": index, "scale": scale}
        if name == "images":
            width, height, fmt, orientation = ((1024, 768, "JPEG", 6) if scale == "smoke"
                else IMAGE_PROFILES[(index + seed) % len(IMAGE_PROFILES)])
            image = Image.frombytes("RGB", (width, height), rng.randbytes(width * height * 3))
            path = root / f"image-{scale}-{seed}-{index}.{fmt.lower()}"
            exif = Image.Exif()
            exif[274] = orientation
            options = {"quality": 92, "exif": exif} if fmt == "JPEG" else {}
            if fmt == "PNG":
                options["exif"] = exif
            image.save(path, format=fmt, **options)
            oriented = (height, width) if orientation in (5, 6, 7, 8) else (width, height)
            ratio = min(1.0, 512 / max(oriented))
            expected_size = [round(side * ratio) for side in oriented]
            task.update(source_size=[width, height], orientation=orientation,
                        source_format=fmt, expected_size=expected_size)
        elif name == "ocr":
            width, height, page_count, line_count = ((1200, 600, 1, 4) if scale == "smoke"
                else OCR_PROFILES[(index + seed) % len(OCR_PROFILES)])
            try:
                font = ImageFont.truetype(OCR_FONT_NAME, 42)
            except OSError as exc:
                raise MissingDependency(f"{OCR_FONT_NAME} is required for OCR fixtures") from exc
            pages, expected_pages = [], []
            for page in range(page_count):
                words = rng.sample(["ALPHA", "BRAVO", "DELTA", "ECHO", "HOTEL", "NOVEMBER"], 3)
                lines = ["LOCAL DOCUMENT", " ".join(words), "THE QUICK BROWN FOX", "READ EVERY WORD"]
                for _ in range(line_count - 4):
                    lines.append(" ".join(rng.sample(["GREEN", "BLUE", "NORTH", "SOUTH", "EAST", "WEST"], 4)))
                image = Image.new("RGB", (width, height), "white")
                draw = ImageDraw.Draw(image)
                spacing = min(115, (height - 120) // line_count)
                for n, line in enumerate(lines):
                    draw.text((70, 60 + n * spacing), line, fill="black", font=font)
                pages.append(image)
                expected_pages.append(" ".join(lines))
            path = root / f"page-{scale}-{seed}-{index}.pdf"
            pages[0].save(path, "PDF", resolution=150.0, save_all=True, append_images=pages[1:])
            # Keep small representative pages at least as well resolved as the
            # smoke fixture. Native-size 42px glyphs can yield DELTA -> DELIA
            # after PDF rasterization (seed 12011); no OCR text is corrected.
            render_side = max(1800, width, height)
            task.update(expected_text=" ".join(expected_pages), expected_pages=expected_pages,
                        fixture_revision=OCR_FIXTURE_REVISION, font_name=OCR_FONT_NAME,
                        font_sha256=_sha(Path(font.path).read_bytes()), font_size=42,
                        page_count=page_count, source_size=[width, height], render_side=render_side,
                        expected_render_size=[round(side * render_side / max(width, height))
                                              for side in (width, height)])
        else:
            width, height, frames, codec = ((320, 180, 24, "ffv1") if scale == "smoke"
                else VIDEO_PROFILES[(index + seed) % len(VIDEO_PROFILES)])
            expected = []
            path = root / f"clip-{scale}-{seed}-{index}.mkv"
            with tempfile.TemporaryDirectory(prefix="media-encode-") as temp:
                raw_path = Path(temp) / "frames.rgb"
                with raw_path.open("wb") as output:
                    for frame in range(frames):
                        color = tuple(rng.randrange(256) for _ in range(3))
                        image = Image.new("RGB", (width, height), color)
                        draw = ImageDraw.Draw(image)
                        x = (frame * 8) % (width - 45)
                        draw.rectangle((x, 20, x + 45, 80), fill=(255, 255, 255))
                        data = image.tobytes()
                        output.write(data)
                        expected.append(_sha(data))
                codec_args = (["-level", "3", "-pix_fmt", "bgr0"] if codec == "ffv1"
                              else ["-pix_fmt", "rgb24"])
                _command(_ffmpeg() + ["-f", "rawvideo", "-pix_fmt", "rgb24", "-s", f"{width}x{height}",
                         "-r", "12", "-i", str(raw_path), "-an", "-c:v", codec,
                         *codec_args, "-threads", "1", str(path)])
            task.update(width=width, height=height, frames=frames, codec=codec, expected_frames=expected)
        if path.stat().st_size > MAX_INPUT_BYTES:
            raise ValueError("generated fixture exceeds input cap")
        task.update(path=str(path), input_sha256=_sha(path.read_bytes()))
        tasks.append(task)
    return tasks


def run_task(task: dict) -> dict:
    """Perform real media work and fail on corrupt input or wrong output."""
    name = task["name"]
    _require(name)
    from PIL import Image, ImageOps
    path = Path(task["path"])
    if not path.is_absolute() or not path.is_file():
        raise ValueError("task requires an existing absolute local fixture path")
    if path.stat().st_size > MAX_INPUT_BYTES:
        raise ValueError("fixture exceeds the 16 MiB input cap")
    if _sha(path.read_bytes()) != task["input_sha256"]:
        raise ValueError("fixture checksum mismatch")
    result = {"name": name, "index": task["index"]}
    if name == "images":
        with ExitStack() as images:
            source = images.enter_context(closing(Image.open(path)))
            expected_source = tuple(task.get("source_size", [1024, 768]))
            if source.size != expected_source or source.width * source.height > MAX_IMAGE_PIXELS:
                raise ValueError("unexpected image dimensions or pixel cap exceeded")
            if source.format != task.get("source_format", "JPEG"):
                raise ValueError("image source format mismatch")
            if source.getexif().get(274, 1) != task.get("orientation", 6):
                raise ValueError("image EXIF orientation mismatch")
            # Each operation returns an independent decoded image. Release the
            # previous full-size core before allocating the next one; preserve
            # the original EXIF -> RGB -> resize -> encode operation order.
            oriented = images.enter_context(closing(ImageOps.exif_transpose(source)))
            source.close()
            transformed = images.enter_context(closing(oriented.convert("RGB")))
            oriented.close()
            transformed.thumbnail((512, 512), Image.Resampling.LANCZOS)
            if list(transformed.size) != task["expected_size"]:
                raise ValueError("image orientation/resize failed")
            encoded = {}
            for fmt, options in (("JPEG", {"quality": 90, "optimize": True, "progressive": True}),
                                 ("PNG", {"optimize": True})):
                buffer = io.BytesIO()
                transformed.save(buffer, format=fmt, **options)
                data = buffer.getvalue()
                with closing(Image.open(io.BytesIO(data))) as check:
                    check.load()
                    if check.size != transformed.size:
                        raise ValueError("encoded image dimensions differ")
                    if fmt == "PNG" and check.tobytes() != transformed.tobytes():
                        raise ValueError("lossless PNG pixel roundtrip failed")
                encoded[fmt] = {"bytes": len(data), "sha256": _sha(data)}
            result.update(size=list(transformed.size), pixels_sha256=_sha(transformed.tobytes()),
                          encodings=encoded)
    elif name == "ocr":
        pages = task.get("page_count", 1)
        render_side = task.get("render_side", 1800)
        if not 1 <= pages <= MAX_OCR_PAGES or not 1 <= render_side <= MAX_RENDER_SIDE:
            raise ValueError("OCR page/render cap exceeded")
        info = _command(["pdfinfo", str(path)]).decode("utf-8")
        actual_pages = next((int(line.split(":", 1)[1]) for line in info.splitlines()
                             if line.startswith("Pages:")), None)
        if actual_pages != pages:
            raise ValueError("PDF page count mismatch")
        texts = []
        with tempfile.TemporaryDirectory(prefix="media-ocr-") as temp:
            prefix = Path(temp) / "render"
            _command(["pdftoppm", "-f", "1", "-l", str(pages), "-r", "150",
                      "-scale-to", str(render_side), "-gray", "-png", str(path), str(prefix)])
            rendered = sorted(Path(temp).glob("render-*.png"))
            if len(rendered) != pages:
                raise ValueError("rendered PDF page count mismatch")
            for page in rendered:
                with Image.open(page) as check:
                    if max(check.size) > render_side:
                        raise ValueError("rendered PDF dimension cap exceeded")
                    expected_size = task.get("expected_render_size", [1800, 900])
                    # PDF point rounding can shift a raster edge by one pixel.
                    if any(abs(actual - expected) > 1
                           for actual, expected in zip(check.size, expected_size)):
                        raise ValueError("rendered PDF dimension oracle mismatch")
                raw_text = _command(["tesseract", str(page), "stdout", "-l", "eng",
                                     "--psm", "6", "--oem", "1"]).decode("utf-8")
                texts.append(" ".join(raw_text.split()).upper())
        normalized = " ".join(texts)
        if normalized != task["expected_text"] or texts != task.get("expected_pages", [task["expected_text"]]):
            raise ValueError(f"OCR correctness mismatch: {normalized!r}")
        result.update(text=normalized, words=len(normalized.split()), sha256=_sha(normalized.encode()))
    else:
        width, height, frames = task["width"], task["height"], task["frames"]
        if (not 1 <= width <= 640 or not 1 <= height <= 360
                or not 1 <= frames <= MAX_VIDEO_FRAMES
                or width * height * 3 * (frames + 1) > MAX_VIDEO_BYTES):
            raise ValueError("video fixture dimensions/frame cap mismatch")
        frame_bytes = width * height * 3
        selected = []
        with tempfile.TemporaryDirectory(prefix="media-frames-") as temp:
            def extract_frame(index, pixels, frame_digest):
                if index % 3:
                    return
                target = Path(temp) / f"frame-{index:04}.png"
                with closing(Image.frombytes("RGB", (width, height), pixels)) as image:
                    image.save(target, "PNG")
                with closing(Image.open(target)) as check:
                    if _sha(check.tobytes()) != frame_digest:
                        raise ValueError("extracted frame roundtrip mismatch")
                selected.append(_sha(target.read_bytes()))
            decoded_sha256 = _stream_video(
                _ffmpeg() + ["-protocol_whitelist", "file,pipe", "-i", str(path),
                             "-map", "0:v:0", "-an", "-frames:v", str(frames + 1),
                             "-threads", "1", "-pix_fmt", "rgb24", "-f", "rawvideo", "pipe:1"],
                frame_bytes, frames, task["expected_frames"], extract_frame)
        result.update(frames=frames, extracted=len(selected), decoded_sha256=decoded_sha256,
                      png_sha256=selected)
    return result
