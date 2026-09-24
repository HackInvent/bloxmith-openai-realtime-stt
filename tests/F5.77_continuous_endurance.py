#!/usr/bin/env python3
"""FB2/FB3/FB4/FB7: accelerated 2 h 15 min source audio, real Opus decoder and local WebSockets.

This is audio-clock endurance, not a two-hour wall-clock/live-provider certification.
Connection age follows decoded audio; IO deadlines stay on the real monotonic clock.
No complete recording/transcript is retained and no microphone or API key is used.
"""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("endurance_stt_fixtures", Path(__file__).with_name("F5.76_continuous_sessions.py"))
FIX = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIX)
STT = FIX.STT
from block_test_artifacts import artifact_path


def peak_rss_bytes():
    """Read process high-water memory with platform-correct units, without host-specific paths."""
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


def test_accelerated_endurance():
    """FB2/FB3/FB4/FB7: repeated speech boundaries past two hours, silence, bounded queues and cleanup."""
    duration = 8100
    captures, turns = [], []
    source_frames = source_bytes = finals = partials = next_begin = 0
    metrics = {"audio_duration_sec": duration, "clock": "accelerated decoded audio; real IO timeouts",
               "max_encoded_queue_bytes": 0, "max_encoded_queue_frames": 0, "memory_samples": []}
    original_capture, original_turns = STT._Capture, STT.ExternalPcmTurns

    class Capture(original_capture):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # Also exercise removal of the old listener's wall-clock capture guard.
            self.started -= duration
            captures.append(self)

    class Turns(original_turns):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            turns.append(self)

    generator = subprocess.Popen(["ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin",
        "-f", "lavfi", "-i", "aevalsrc=if(lt(mod(t\\,30)\\,1)\\,0.1*sin(2*PI*440*t)\\,0):s=48000",
        "-t", str(duration), "-ac", "1", "-c:a", "libopus", "-b:a", "24000", "-f", "ogg",
        "-page_duration", "100000", "pipe:1"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
    os.set_blocking(generator.stdout.fileno(), False)
    began = time.monotonic()
    completed = None
    stopped = False
    next_memory_sample = 600
    try:
        with patch.object(STT, "_Capture", Capture), patch.object(STT, "ExternalPcmTurns", Turns), \
                FIX.decoder_probe() as decoder, \
                patch.object(STT._Connections, "clock", staticmethod(lambda: decoder.decoded_bytes / 48000)), \
                FIX.rotation_server(collect=False) as server, FIX.FIX.active_listener({
                    "segmentation": "external", "drain_timeout_sec": 5, "final_timeout_sec": 5}) as live:
            live.command("start")
            while completed is None:
                assert time.monotonic() - began < 300, "Accelerated endurance did not finish within the test budget."
                capture = captures[0] if captures else None
                decoded_ms = decoder.decoded_bytes // 48
                if capture is not None:
                    metrics["max_encoded_queue_bytes"] = max(metrics["max_encoded_queue_bytes"], capture.queued_bytes)
                    metrics["max_encoded_queue_frames"] = max(metrics["max_encoded_queue_frames"], len(capture.frames))
                    if decoded_ms >= next_begin and not stopped and decoded_ms + 1000 < duration * 1000:
                        live.command("begin", audio_start_ms=decoded_ms)
                        live.command("commit", audio_end_ms=decoded_ms + 1000)
                        next_begin = decoded_ms + 30000
                    if not stopped and len(capture.frames) < 8 and source_frames - capture.frame_count < 4:
                        try:
                            chunk = os.read(generator.stdout.fileno(), 1024)
                        except BlockingIOError:
                            chunk = None
                        if chunk:
                            live.frame(chunk, channels=1)
                            source_frames += 1
                            source_bytes += len(chunk)
                        elif chunk == b"":
                            assert generator.wait(timeout=3) == 0, generator.stderr.read().decode()
                            live.command("stop", frame_count=source_frames, byte_count=source_bytes)
                            stopped = True
                for result in live.host.pop_results():
                    assert result.status != "failed", result.error
                    metadata = result.metadata.get("openai_realtime_stt", {})
                    assert metadata.get("state") != "warning", result.last_message
                    finals += sum(output.port_name == "final_out" for output in result.outputs)
                    partials += sum(output.port_name == "partial_out" for output in result.outputs)
                    if metadata.get("state") == "completed":
                        completed = metadata
                if decoded_ms >= next_memory_sample * 1000:
                    metrics["memory_samples"].append({"audio_sec": decoded_ms / 1000, "peak_rss_bytes": peak_rss_bytes()})
                    next_memory_sample += 600
                time.sleep(0.001)
            assert not live.observed("error")
            assert decoder.created == decoder.closed == 1
            assert decoder.decoded_bytes == duration * 48000
            assert server.maximum_live[0] <= 2 and len(server.records) >= 3
            assert completed["segments"] == finals == sum(record.commits for record in server.records)
            assert finals >= 200 and partials > 0
            assert completed["frames_received"] == source_frames and completed["bytes_received"] == source_bytes
            assert capture.latest_boundary["begin"] > 7_200_000
            assert len(turns) == 1 and len(turns[0].history) <= FIX.HISTORY_BYTES
            assert metrics["max_encoded_queue_frames"] <= 12, metrics
            samples = metrics["memory_samples"]
            growth = samples[-1]["peak_rss_bytes"] - samples[0]["peak_rss_bytes"]
            assert growth < 32 * 1024 * 1024, f"Unexpected retained memory growth: {growth} bytes."
            metrics.update(wall_duration_sec=round(time.monotonic() - began, 3), decoded_bytes=decoder.decoded_bytes,
                decoder_count=decoder.created, connections=len(server.records), maximum_live_connections=server.maximum_live[0],
                source_frames=source_frames, source_bytes=source_bytes, final_segments=finals,
                last_speech_start_ms=capture.latest_boundary["begin"], retained_pcm_bytes=len(turns[0].history),
                peak_rss_growth_bytes=growth)
    finally:
        if generator.poll() is None:
            generator.kill()
        generator.wait(timeout=2)
        generator.stdout.close()
        generator.stderr.close()
    Path(artifact_path("stt-continuous-endurance.json")).write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(metrics, indent=2))
    print("[ok] F5.77 accelerated 2 h 15 min: bounded memory, one decoder, multiple sessions, same stream and absolute clock")


if __name__ == "__main__":
    test_accelerated_endurance()
