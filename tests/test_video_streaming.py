"""Bounded video decoding without running FFmpeg or requiring large fixtures."""
from collections import deque
import hashlib
import io
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from benchmarks.real_world import media


class ChunkPipe:
    def __init__(self, chunks=(), *, block=False, error=None):
        self.chunks = deque(chunks)
        self.block = block
        self.error = error
        self.eof = False
        self.closed = False
        self.read_sizes = []
        self.released = threading.Event()

    def read(self, size):
        self.read_sizes.append(size)
        if self.block and not self.released.wait(2):
            raise AssertionError("reader was not released by subprocess cleanup")
        if self.error is not None:
            error, self.error = self.error, None
            raise error
        if self.chunks:
            chunk = self.chunks.popleft()
            if len(chunk) > size:
                self.chunks.appendleft(chunk[size:])
                chunk = chunk[:size]
            return chunk
        self.eof = True
        return b""

    def close(self):
        self.closed = True


class FakeProcess:
    def __init__(self, stdout=(), stderr=(), *, returncode=0, block=False,
                 stay_alive=False, stdout_error=None):
        self.stdout = ChunkPipe(stdout, block=block, error=stdout_error)
        self.stderr = ChunkPipe(stderr, block=block)
        self.returncode = returncode
        self.stay_alive = stay_alive
        self.killed = False
        self.wait_calls = []

    def poll(self):
        if self.killed:
            return -9
        if not self.stay_alive and self.stdout.eof and self.stderr.eof:
            return self.returncode
        return None

    def kill(self):
        self.killed = True
        self.stdout.released.set()
        self.stderr.released.set()

    def wait(self, timeout=None):
        self.wait_calls.append(timeout)
        code = self.poll()
        if code is None:
            raise subprocess.TimeoutExpired(["fake-decoder"], timeout)
        return code


class VideoStreamingTests(unittest.TestCase):
    def setUp(self):
        self.frames = [b"abcdef", b"ghijkl", b"mnopqr", b"stuvwx"]
        self.raw = b"".join(self.frames)
        self.expected = [hashlib.sha256(frame).hexdigest() for frame in self.frames]

    def run_stream(self, process, *, expected=None, consume=None, timeout=2):
        seen = []
        if consume is None:
            def consume(index, pixels, digest):
                seen.append((index, bytes(pixels), digest))
        with patch.object(media.subprocess, "Popen", return_value=process) as popen:
            digest = media._stream_video(
                ["fake-decoder"], 6, 4,
                self.expected if expected is None else expected, consume, timeout)
        self.assertEqual(popen.call_args.kwargs["bufsize"], 0)
        self.assertEqual(popen.call_args.kwargs["env"]["OMP_THREAD_LIMIT"], "1")
        self.assertEqual(popen.call_args.kwargs["env"]["OMP_NUM_THREADS"], "1")
        self.assertEqual(popen.call_args.kwargs["env"]["LC_ALL"], "C")
        return digest, seen

    def assert_cleaned(self, process, *, killed=None):
        self.assertTrue(process.stdout.closed)
        self.assertTrue(process.stderr.closed)
        self.assertTrue(process.wait_calls)
        self.assertIsNone(process.wait_calls[-1], "cleanup must reap the process")
        if killed is not None:
            self.assertEqual(process.killed, killed)
        self.assertFalse(any(thread.name.startswith("media-video-")
                             for thread in threading.enumerate()))
        for pipe in (process.stdout, process.stderr):
            self.assertTrue(all(size == media.VIDEO_READ_BYTES for size in pipe.read_sizes))

    def test_success_matches_full_stream_and_frame_hashes(self):
        process = FakeProcess([self.raw])
        digest, seen = self.run_stream(process)
        self.assertEqual(digest, hashlib.sha256(self.raw).hexdigest())
        self.assertEqual(seen, list(zip(range(4), self.frames, self.expected)))
        self.assert_cleaned(process, killed=False)

    def test_arbitrary_partial_reads_preserve_order_and_reuse_one_frame(self):
        chunks = [self.raw[:1], self.raw[1:4], self.raw[4:15], self.raw[15:23], self.raw[23:]]
        process = FakeProcess(chunks)
        seen, identities = [], []
        def consume(index, pixels, digest):
            identities.append(id(pixels))
            seen.append((index, bytes(pixels), digest))
        digest, _ = self.run_stream(process, consume=consume)
        self.assertEqual(digest, hashlib.sha256(self.raw).hexdigest())
        self.assertEqual(seen, list(zip(range(4), self.frames, self.expected)))
        self.assertEqual(len(set(identities)), 1)
        self.assert_cleaned(process)

    def test_empty_missing_frame_and_short_final_frame_are_rejected(self):
        for raw in (b"", self.raw[:-6], self.raw[:-1]):
            with self.subTest(length=len(raw)):
                process = FakeProcess([raw] if raw else [])
                with self.assertRaisesRegex(ValueError, "frame count mismatch"):
                    self.run_stream(process)
                self.assert_cleaned(process)

    def test_one_extra_byte_or_frame_is_rejected(self):
        for extra in (b"x", self.frames[0]):
            with self.subTest(extra=len(extra)):
                process = FakeProcess([self.raw, extra])
                with self.assertRaisesRegex(ValueError, "frame count mismatch"):
                    self.run_stream(process)
                self.assert_cleaned(process)

    def test_wrong_hash_order_or_expected_count_is_rejected(self):
        for expected in (["0" * 64] + self.expected[1:], list(reversed(self.expected)),
                         self.expected[:-1], self.expected + [self.expected[0]]):
            with self.subTest(expected=expected):
                process = FakeProcess([self.raw])
                with self.assertRaisesRegex(ValueError, "pixels differ"):
                    self.run_stream(process, expected=expected)
                self.assert_cleaned(process)

    def test_failed_subprocess_preserves_bounded_stderr_tail(self):
        tail = b"decoder failed at final packet"
        process = FakeProcess([self.raw], [b"x" * (media.VIDEO_READ_BYTES * 8), tail],
                              returncode=7)
        with self.assertRaisesRegex(RuntimeError, "fake-decoder failed \\(7\\)") as raised:
            self.run_stream(process)
        self.assertTrue(str(raised.exception).endswith(tail.decode()))
        self.assertLessEqual(len(str(raised.exception)), media.VIDEO_STDERR_BYTES + 40)
        self.assert_cleaned(process, killed=False)

    def test_timeout_kills_reaps_and_unblocks_both_pipe_readers(self):
        process = FakeProcess(block=True)
        with self.assertRaises(subprocess.TimeoutExpired):
            self.run_stream(process, timeout=0.02)
        self.assert_cleaned(process, killed=True)

    def test_timeout_after_pipe_eof_still_kills_and_reaps_process(self):
        process = FakeProcess([self.raw], stay_alive=True)
        with self.assertRaises(subprocess.TimeoutExpired):
            self.run_stream(process)
        self.assert_cleaned(process, killed=True)

    def test_pipe_read_failure_kills_reaps_and_closes(self):
        process = FakeProcess(stdout_error=OSError("planned read error"))
        with self.assertRaisesRegex(OSError, "planned read error"):
            self.run_stream(process)
        self.assert_cleaned(process, killed=True)

    def test_second_reader_start_failure_cleans_up_first_reader(self):
        process = FakeProcess(block=True)
        real_start = threading.Thread.start
        def start(reader):
            if reader.name == "media-video-stderr":
                raise RuntimeError("planned thread startup failure")
            return real_start(reader)
        with patch.object(media.threading.Thread, "start", start):
            with self.assertRaisesRegex(RuntimeError, "planned thread startup failure"):
                self.run_stream(process)
        self.assert_cleaned(process, killed=True)

    def test_consumer_failure_with_full_queue_cannot_strand_readers(self):
        real_queue = queue.Queue
        observed = []
        class ObservedQueue(real_queue):
            def put(self, item, block=True, timeout=None):
                result = super().put(item, block=block, timeout=timeout)
                if self.full():
                    full.set()
                return result
        full = threading.Event()
        def make_queue(*args, **kwargs):
            result = ObservedQueue(*args, **kwargs)
            observed.append(result)
            return result
        process = FakeProcess([self.frames[0]] * 1000, [b"stderr"] * 1000)
        def fail(index, pixels, digest):
            self.assertTrue(full.wait(1), "test must fill the bounded queue")
            raise RuntimeError("planned consumer failure")
        with patch.object(media.queue, "Queue", side_effect=make_queue):
            with self.assertRaisesRegex(RuntimeError, "planned consumer failure"):
                self.run_stream(process, consume=fail)
        self.assertEqual(observed[0].maxsize, 2)
        self.assert_cleaned(process, killed=True)

    def test_caps_fail_before_starting_a_subprocess(self):
        for frame_bytes, frames in ((0, 4), (6, 0), (6, media.MAX_VIDEO_FRAMES + 1),
                                    (media.MAX_VIDEO_BYTES, 1)):
            with self.subTest(frame_bytes=frame_bytes, frames=frames):
                with patch.object(media.subprocess, "Popen") as popen:
                    with self.assertRaisesRegex(ValueError, "cap mismatch"):
                        media._stream_video(["fake"], frame_bytes, frames, [], lambda *args: None)
                popen.assert_not_called()

    def test_real_pipes_drain_stderr_larger_than_pipe_capacity(self):
        # A tiny Python child exercises real OS pipes without running FFmpeg.
        args = [sys.executable, "-c", "import sys; "
                "sys.stderr.buffer.write(b'x' * (256 * 1024)); sys.stderr.flush(); "
                f"sys.stdout.buffer.write({self.raw!r})"]
        seen = []
        digest = media._stream_video(args, 6, 4, self.expected,
                                     lambda index, pixels, sha: seen.append(bytes(pixels)),
                                     timeout=5)
        self.assertEqual(digest, media._sha(self.raw))
        self.assertEqual(seen, self.frames)
        self.assertFalse(any(thread.name.startswith("media-video-")
                             for thread in threading.enumerate()))

    def test_real_subprocess_timeout_reaps_child_and_closes_pipes(self):
        args = [sys.executable, "-c", "import time; time.sleep(30)"]
        real_popen = subprocess.Popen
        processes = []
        def start(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            processes.append(process)
            return process
        with patch.object(media.subprocess, "Popen", side_effect=start):
            with self.assertRaises(subprocess.TimeoutExpired):
                media._stream_video(args, 6, 4, self.expected, lambda *args: None,
                                    timeout=0.03)
        self.assertEqual(len(processes), 1)
        self.assertIsNotNone(processes[0].poll())
        self.assertTrue(processes[0].stdout.closed)
        self.assertTrue(processes[0].stderr.closed)
        self.assertFalse(any(thread.name.startswith("media-video-")
                             for thread in threading.enumerate()))

    def test_run_task_preserves_exact_png_and_result_oracles(self):
        try:
            from PIL import Image
        except ImportError:
            self.skipTest("Pillow required")
        expected_pngs = []
        for frame in self.frames[::3]:
            buffer = io.BytesIO()
            with Image.frombytes("RGB", (2, 1), frame) as image:
                image.save(buffer, "PNG")
            expected_pngs.append(hashlib.sha256(buffer.getvalue()).hexdigest())
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "fake.mkv"
            path.write_bytes(b"fixture")
            task = dict(name="video", index=7, path=str(path),
                        input_sha256=media._sha(b"fixture"), width=2, height=1, frames=4,
                        expected_frames=self.expected)
            process = FakeProcess([self.raw[:5], self.raw[5:]])
            with patch.object(media, "_require"), \
                 patch.object(media.subprocess, "Popen", return_value=process) as popen:
                result = media.run_task(task)
            self.assertEqual(result, dict(name="video", index=7, frames=4, extracted=2,
                                         decoded_sha256=media._sha(self.raw), png_sha256=expected_pngs))
            args = popen.call_args.args[0]
            self.assertEqual(args[args.index("-frames:v") + 1], "5")
            self.assertEqual(args[args.index("-protocol_whitelist") + 1], "file,pipe")
            self.assert_cleaned(process)

    def test_png_failure_closes_image_cleans_temp_and_reaps_decoder(self):
        try:
            from PIL import Image
        except ImportError:
            self.skipTest("Pillow required")
        real_frombytes, real_temp = Image.frombytes, tempfile.TemporaryDirectory
        images, temporary_paths = [], []
        def frombytes(*args, **kwargs):
            image = real_frombytes(*args, **kwargs)
            image.close = Mock(wraps=image.close)
            images.append(image)
            return image
        def make_temp(*args, **kwargs):
            directory = real_temp(*args, **kwargs)
            temporary_paths.append(Path(directory.name))
            return directory
        with real_temp() as temp:
            path = Path(temp) / "fake.mkv"
            path.write_bytes(b"fixture")
            task = dict(name="video", index=7, path=str(path),
                        input_sha256=media._sha(b"fixture"), width=2, height=1, frames=4,
                        expected_frames=self.expected)
            process = FakeProcess([self.raw])
            with patch.object(media, "_require"), \
                 patch.object(media.subprocess, "Popen", return_value=process), \
                 patch.object(Image, "frombytes", side_effect=frombytes), \
                 patch.object(Image.Image, "save", side_effect=OSError("planned PNG failure")), \
                 patch.object(media.tempfile, "TemporaryDirectory", side_effect=make_temp):
                with self.assertRaisesRegex(OSError, "planned PNG failure"):
                    media.run_task(task)
            self.assertEqual(len(images), 1)
            images[0].close.assert_called_once()
            self.assertEqual(len(temporary_paths), 1)
            self.assertFalse(temporary_paths[0].exists())
            self.assert_cleaned(process)

    def test_png_roundtrip_mismatch_remains_an_error(self):
        try:
            from PIL import Image
        except ImportError:
            self.skipTest("Pillow required")
        check = Mock()
        check.tobytes.return_value = b"wrong pixels"
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "fake.mkv"
            path.write_bytes(b"fixture")
            task = dict(name="video", index=7, path=str(path),
                        input_sha256=media._sha(b"fixture"), width=2, height=1, frames=4,
                        expected_frames=self.expected)
            process = FakeProcess([self.raw])
            with patch.object(media, "_require"), \
                 patch.object(media.subprocess, "Popen", return_value=process), \
                 patch.object(Image, "open", return_value=check):
                with self.assertRaisesRegex(ValueError, "extracted frame roundtrip mismatch"):
                    media.run_task(task)
            check.close.assert_called_once()
            self.assert_cleaned(process)


if __name__ == "__main__":
    unittest.main()
