#!/usr/bin/env python3
"""FB2/FB3/FB4/FB7: long captures, ordered renewal, unchanged audio and cancellable handover.

Only a loopback WebSocket and generated audio are used. Connection age can be
advanced independently of real IO/command deadlines; no real provider is called.
"""
from __future__ import annotations

import asyncio
import base64
from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import subprocess
from threading import Event, Thread
import time
from types import SimpleNamespace
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("continuous_stt_fixtures", Path(__file__).with_name("F5.46_openai_realtime_stt_block.py"))
FIX = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIX)

from blocs.openai_realtime_stt import block as STT
from blocs.openai_realtime_stt.external_turns import ExternalPcmTurns, HISTORY_BYTES, SPEECH_PREFIX_BYTES


@contextmanager
def rotation_server(*, hold_first=False, replacement_mode="normal", collect=True):
    """Serve distinct generations with deliberately reused item IDs and optional late finals."""
    from websockets.sync.server import serve
    from websockets.asyncio.client import connect
    from websockets.exceptions import ConnectionClosed

    records = []
    release = Event()
    held = Event()
    maximum_live = [0]

    def handler(ws):
        record = SimpleNamespace(number=len(records) + 1, ready=False, closed=False,
                                 pcm=bytearray(), pcm_bytes=0, commits=0, completed=0)
        records.append(record)
        maximum_live[0] = max(maximum_live[0], sum(not r.closed for r in records))
        spoke = False
        try:
            for raw in ws:
                event = json.loads(raw)
                kind = event["type"]
                item_id = f"item-{record.commits + 1}"
                if kind == "session.update":
                    if record.number > 1 and replacement_mode == "no_ready":
                        continue
                    if record.number > 1 and replacement_mode == "refuse":
                        ws.send(json.dumps({"type": "error", "error": {"code": "invalid_api_key"}}))
                        continue
                    ws.send(json.dumps({"type": "session.updated"}))
                    record.ready = True
                elif kind == "input_audio_buffer.append":
                    pcm = base64.b64decode(event["audio"], validate=True)
                    record.pcm_bytes += len(pcm)
                    if collect:
                        record.pcm.extend(pcm)
                    if not spoke:
                        ws.send(json.dumps({"type": "conversation.item.input_audio_transcription.delta",
                                           "item_id": item_id, "delta": f"preview {record.number}"}))
                        spoke = True
                elif kind == "input_audio_buffer.commit":
                    record.commits += 1
                    ws.send(json.dumps({"type": "input_audio_buffer.committed", "item_id": item_id}))
                    if hold_first and record.number == 1:
                        held.set()
                        if not release.wait(6):
                            return
                    completed = {"type": "conversation.item.input_audio_transcription.completed", "item_id": item_id,
                                 "transcript": f"final {record.number}:{record.commits}"}
                    ws.send(json.dumps(completed))
                    ws.send(json.dumps(completed))  # A duplicate must remain harmless within each generation.
                    record.completed += 1
                    spoke = False
        except ConnectionClosed:
            pass
        finally:
            record.closed = True

    server = serve(handler, "127.0.0.1", 0, compression=None)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.socket.getsockname()[1]

    async def local_connect(_self, api_key, config):
        assert api_key == FIX.KEY
        return await connect(f"ws://127.0.0.1:{port}", close_timeout=0.1, proxy=None, compression=None)

    try:
        with patch.object(STT.OpenAIRealtimeSttBlock, "_connect", local_connect):
            yield SimpleNamespace(records=records, release=release, held=held, maximum_live=maximum_live)
    finally:
        release.set()
        server.shutdown()
        thread.join(2)
        assert not thread.is_alive()
        FIX.until(lambda: all(r.closed for r in records), "All replacement/retiring sockets must close.")


@contextmanager
def decoder_probe():
    """Count real decoders and decoded source bytes without replacing the codec implementation."""
    original = STT._Decoder
    stats = SimpleNamespace(created=0, closed=0, decoded_bytes=0)

    class Decoder(original):
        def __init__(self, header):
            stats.created += 1
            super().__init__(header)

        def read(self):
            value = super().read()
            stats.decoded_bytes += len(value or b"")
            return value

        def close(self):
            super().close()
            stats.closed += 1

    with patch.object(STT, "_Decoder", Decoder):
        yield stats


def send_chunks(live, raw, *, size=1200):
    """Send bounded frames through the real runtime transport; return exact stop counters."""
    chunks = [raw[offset:offset + size] for offset in range(0, len(raw), size)]
    for chunk in chunks:
        live.frame(chunk)
        time.sleep(0.002)
    return len(chunks), len(raw)


def test_rotation_keeps_audio_and_final_order(segmentation):
    """FB3/FB4/FB7: keep one decoder, no missing/replayed PCM, live previews and late final ordering."""
    clock = [0.0]
    encoded = FIX.encoded_audio("webm")
    expected = subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", "pipe:0",
        "-ac", "1", "-ar", "24000", "-f", "s16le", "pipe:1"], input=encoded, capture_output=True, check=True).stdout
    with patch.object(STT._Connections, "clock", staticmethod(lambda: clock[0])), decoder_probe() as decoder, \
            rotation_server(hold_first=True) as server, FIX.active_listener({"segmentation": segmentation}) as live:
        live.command("start")
        if segmentation == "external":
            live.command("begin", audio_start_ms=0)
        cut = len(encoded) // 5
        first_frames, first_bytes = send_chunks(live, encoded[:cut])
        FIX.until(lambda: server.records and server.records[0].pcm_bytes > 4800, "First connection receives live audio.")
        assert not live.observed(port="final_out"), "A preview must never be a final."
        assert live.observed(port="partial_out")
        clock[0] = 3601 if segmentation == "external" else 3301
        FIX.until(lambda: len(server.records) == 2 and server.records[1].ready, "Preconnect while decoding continues.")
        rest_frames, rest_bytes = send_chunks(live, encoded[cut:])
        live.command("stop", frame_count=first_frames + rest_frames, byte_count=first_bytes + rest_bytes)
        FIX.until(lambda: server.held.is_set(), "The previous session waits for a delayed final.")
        FIX.until(lambda: server.records[1].completed > 0, "The new connection transcribes before the old final arrives.")
        assert not live.observed(port="final_out"), "New finals wait for the earlier generation, not source Stop."
        server.release.set()
        FIX.until(lambda: live.observed("completed") or live.observed("error"), "The whole capture finishes.")
        assert not live.observed("error"), [r.error for r in live.results if r.error]
        finals = [r for r in live.results if any(o.port_name == "final_out" for o in r.outputs)]
        assert [r.outputs[0].value for r in finals] == [
            f"final {record.number}:{index}" for record in server.records for index in range(1, record.commits + 1)]
        assert b"".join(record.pcm for record in server.records) == expected, "No sample lost, repeated or restarted."
        assert len({(r.metadata["openai_realtime_stt"]["connection_generation"],
                     r.metadata["openai_realtime_stt"]["item_id"]) for r in finals}) == len(finals)
        assert decoder.created == decoder.closed == 1
        assert server.maximum_live[0] == 2
        completed = next(r.metadata["openai_realtime_stt"] for r in live.results
                         if r.metadata.get("openai_realtime_stt", {}).get("state") == "completed")
        assert completed["frames_received"] == first_frames + rest_frames
        assert completed["bytes_received"] == len(encoded) and completed["connections"] == 2
        if segmentation == "external":
            assert any(r.metadata.get("openai_realtime_stt", {}).get("reason") == "renewal" for r in live.results)


def test_silence_renewal_and_source_stop():
    """FB2/FB7: idle renewal emits neither audio nor text, and Stop cancels an unready replacement."""
    clock = [0.0]
    with patch.object(STT._Connections, "clock", staticmethod(lambda: clock[0])), \
            rotation_server() as server, FIX.active_listener({"segmentation": "external"}) as live:
        live.command("start")
        FIX.until(lambda: server.records and server.records[0].ready, "Initial connection ready.")
        frames, size = send_chunks(live, FIX.encoded_audio("ogg"))
        for generation in range(2, 5):
            clock[0] += 3600
            FIX.until(lambda: len(server.records) == generation and server.records[-2].closed, "Idle session renewed.")
        assert not live.observed("error") and not live.observed(port="final_out")
        assert not live.observed(port="partial_out") and all(r.pcm_bytes == 0 for r in server.records)
        live.command("stop", frame_count=frames, byte_count=size)
        FIX.until(lambda: live.observed("completed"), "Silent capture finishes across multiple hour boundaries.")
        assert server.maximum_live[0] <= 2
    for runtime_stop in (False, True):
        clock[0] = 0
        with patch.object(STT._Connections, "clock", staticmethod(lambda: clock[0])), \
                rotation_server(replacement_mode="no_ready") as server:
            with FIX.active_listener({"segmentation": "external"}) as live:
                live.command("start")
                FIX.until(lambda: server.records and server.records[0].ready, "Initial ready.")
                clock[0] = 3301
                FIX.until(lambda: len(server.records) == 2, "Replacement is waiting for configuration.")
                if not runtime_stop:
                    live.command("stop", frame_count=0, byte_count=0)
                    FIX.until(lambda: live.observed("completed"), "Source Stop must cancel useless reconnection.", timeout=1)
                assert not live.observed("error") and not live.observed(port="final_out")
            assert not live.observed("error"), "Runtime Stop during reconnection is not an error."


def test_real_renewal_failure_stays_visible():
    """FB5/FB7: failed credentials on renewal are not reported as a successful transcription."""
    clock = [0.0]
    with patch.object(STT._Connections, "clock", staticmethod(lambda: clock[0])), \
            rotation_server(replacement_mode="refuse") as server, FIX.active_listener() as live:
        live.command("start")
        FIX.until(lambda: server.records and server.records[0].ready, "Initial connection is ready.")
        clock[0] = 3301
        FIX.until(lambda: live.observed("error"), "A genuine renewal failure remains visible.")
        assert any("refuses the API key" in (r.error or "") for r in live.results)
        assert not live.observed("completed") and not live.observed(port="final_out")


def test_long_offsets_and_checkpoint_preserve_prefix():
    """FB2/FB7: >2-hour VAD offsets remain valid; renewal keeps prefix/history and sent high-water mark."""
    async def scenario():
        start_ms = 7_200_000
        capture = STT._Capture("long-capture")
        config = STT._config({"segmentation": "external"})
        for action, key, offset in (("begin", "audio_start_ms", start_ms), ("commit", "audio_end_ms", start_ms + 1000)):
            command = STT._command({"action": action, "stream_id": capture.stream_id, key: offset})
            capture.request_boundary(command, config)
        assert len(capture.boundaries) == 2
        audio, cuts, warnings = bytearray(), [], []
        async def append(raw):
            audio.extend(raw)
        async def commit(**metadata):
            cuts.append(metadata)
        turns = ExternalPcmTurns(capture.boundaries, append, commit, warnings.append, timeout_sec=5)
        turns.decoded_bytes = start_ms * 48
        turns.history = bytearray(b"\x12\x34" * (HISTORY_BYTES // 2))
        await turns.tick()
        assert len(audio) == SPEECH_PREFIX_BYTES
        await turns.checkpoint()
        assert turns.active and turns.opened_at == start_ms * 48
        assert len(turns.history) == HISTORY_BYTES and cuts[0]["reason"] == "renewal"
        await turns.feed(b"\x56\x78" * 24000)
        assert not turns.active and not warnings
        assert audio == b"\x12\x34" * (SPEECH_PREFIX_BYTES // 2) + b"\x56\x78" * 24000
        assert [cut["reason"] for cut in cuts] == ["renewal", "external"]
        assert cuts[1]["committed_audio_start_ms"] == start_ms
        assert cuts[1]["committed_audio_end_ms"] == start_ms + 1000
    asyncio.run(scenario())


def main():
    """Run deterministic multi-generation cases without internet, a microphone or credentials."""
    for mode in ("duration", "external"):
        test_rotation_keeps_audio_and_final_order(mode)
    test_silence_renewal_and_source_stop()
    test_real_renewal_failure_stays_visible()
    test_long_offsets_and_checkpoint_preserve_prefix()
    print("[ok] F5.76 continuous sessions: exact PCM, ordered finals, long offsets, silence, Stop and failures")


if __name__ == "__main__":
    main()
