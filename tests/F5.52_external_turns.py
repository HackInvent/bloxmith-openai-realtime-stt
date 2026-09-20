#!/usr/bin/env python3
"""FB1–FB6: externally delimited speech, bounded idle PCM and local-only API/graph integration."""
from __future__ import annotations

import asyncio
from array import array
import importlib.util
import json
import math
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Event
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location("external_stt_fixtures", Path(__file__).with_name("F5.46_openai_realtime_stt_block.py"))
assert SPEC is not None and SPEC.loader is not None
FIX = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIX)

from blocs.openai_realtime_stt.block import _commands
from blocs.openai_realtime_stt.external_turns import ExternalPcmTurns, HISTORY_BYTES
from bloxsmith_app.graph_compile import compile_runtime_graph_document
from bloxsmith_app.graph_document import GraphDocument
from block_test_artifacts import artifact_path
from block_test_fixtures import instance_scope


def boundary(action, offset, stream_id="capture"):
    """Build the explicit source-clock command emitted by the VAD-to-STT mapper."""
    return {"action": action, "stream_id": stream_id,
            "audio_start_ms" if action == "begin" else "audio_end_ms": offset}


def test_command_validation_batches_and_modes():
    """FB1/FB2/FB5: strict clock values, atomic bounded batches, duration compatibility and no-IO simulation."""
    assert FIX._config({})["segmentation"] == "duration"
    assert FIX._config({"segmentation": "external"})["segment_seconds"] == 15
    for invalid in ("auto", None, True):
        try:
            FIX._config({"segmentation": invalid})
        except FIX.RealtimeSttError:
            pass
        else:
            raise AssertionError("Invalid segmentation mode accepted.")
    for action in ("begin", "commit"):
        assert FIX._command(boundary(action, 0)) == boundary(action, 0)
        for offset in (-1, True, 1.2, "100", 3_600_001):
            try:
                FIX._command(boundary(action, offset))
            except FIX.RealtimeSttError:
                pass
            else:
                raise AssertionError("Offset must be an integer on the source audio clock.")
    for raw in ([], [boundary("begin", 0)] * 65, [boundary("begin", 0), {"action": "bad"}],
                json.dumps(boundary("begin", 0)) + json.dumps(boundary("commit", 100)), " " * 65537):
        try:
            _commands(raw)
        except FIX.RealtimeSttError:
            pass
        else:
            raise AssertionError("Invalid/oversized/concatenated command batch accepted.")
    block = FIX.OpenAIRealtimeSttBlock()
    assert block.model["version"] == "0.1.0"
    assert block.model["tested_with_bloxsmith"] == (FIX.ROOT / "VERSION").read_text().strip()
    assert block.model["bloxsmith_compatibility"] == [block.model["tested_with_bloxsmith"]]
    mailbox = Mock()
    context = FIX.context_for(config={"segmentation": "external"}, services={"runtime_listener": mailbox})
    commands = [boundary("begin", 0), boundary("commit", 1000)]
    context.input_events = tuple(SimpleNamespace(input_port_id=2, value=json.dumps(command)) for command in commands)
    context.input_attribute("command_in").update("deliberately invalid concatenated aggregate")
    assert block.execute_runtime(context).status == "success"
    mailbox.send.assert_called_once_with(commands)
    context.input_events = ()
    context.input_attribute("command_in").update(json.dumps([*commands, {"action": "bad"}]))
    assert block.execute_runtime(context).status == "failed" and mailbox.send.call_count == 1
    for segmentation in ("duration", "external"):
        simulated = FIX.context_for("centralized", config={"segmentation": segmentation}, services={
            "resolve_secret": Mock(side_effect=AssertionError("No secret in simulation"))})
        with patch("subprocess.Popen", side_effect=AssertionError("No decoder in simulation")), \
                patch.object(block, "_connect", side_effect=AssertionError("No API in simulation")):
            assert not block.prepare_runtime(simulated).listen_on_run
            assert block.initialize_runtime(simulated).status == "success"
            assert block.execute_runtime(simulated).status == "skipped"
    capture = FIX._Capture("capture")
    config = FIX._config({"segmentation": "external"})
    capture.request_boundary(boundary("begin", 0), config)
    capture.request_boundary(boundary("begin", 0), config)
    assert len(capture.boundaries) == 1
    for index in range(1, 64):
        capture.request_boundary(boundary("begin", index), config)
    try:
        capture.request_boundary(boundary("commit", 100), config)
    except FIX.RealtimeSttError:
        pass
    else:
        raise AssertionError("External boundary queue must be bounded.")


def source_pcm(milliseconds):
    """Give each source sample a deterministic value so missing/replayed ranges are detectable."""
    samples = array("h", ((index * 173 % 60001) - 30000 for index in range(milliseconds * 24)))
    if sys.byteorder != "little":
        samples.byteswap()
    return samples.tobytes()


def test_onset_prefix_preserves_buffered_audio_without_idle_publication():
    """FB2/FB3: a confirmed begin recovers 1.5s of earlier PCM, without replaying previous turns."""
    async def scenario():
        source = source_pcm(7000)
        commands, sent, commits, warnings = [], bytearray(), [], []
        async def append(raw):
            sent.extend(raw)
        async def commit(**metadata):
            commits.append(metadata)
        gate = ExternalPcmTurns(commands, append, commit, warnings.append, timeout_sec=5)
        await gate.feed(source[:2000 * 48])
        assert not sent and not commits, "Retaining audio never opens a turn without explicit speech."
        commands.extend([(1800, "begin", time.monotonic()), (3000, "commit", time.monotonic())])
        await gate.tick()
        assert sent == source[300 * 48:2000 * 48], "Keep the weak first word preceding VAD confirmation."
        await gate.feed(source[2000 * 48:3500 * 48])
        assert sent == source[300 * 48:3000 * 48] and not gate.active
        commands.extend([(3600, "begin", time.monotonic()), (4000, "commit", time.monotonic())])
        await gate.feed(source[3500 * 48:4500 * 48])
        assert sent == source[300 * 48:4000 * 48], "Close phrases may share context, never sent samples."
        await gate.feed(source[4500 * 48:6500 * 48])
        assert len(commits) == 2 and not gate.active
        commands.extend([(6500, "begin", time.monotonic()), (6800, "commit", time.monotonic())])
        await gate.feed(source[6500 * 48:])
        assert sent == source[300 * 48:4000 * 48] + source[5000 * 48:6800 * 48]
        assert not warnings and not gate.active
    asyncio.run(scenario())


def test_delayed_begin_recovers_instance_offsets_without_duplicate_pcm():
    """FB2/FB3/FB4: reproduce the 40448ms begin arriving at 43672ms after a late prior commit."""
    async def scenario(previous_turn):
        source = source_pcm(45000)
        commands, sent, commits, warnings = [], bytearray(), [], []
        async def append(raw):
            sent.extend(raw)
        async def commit(**metadata):
            commits.append(metadata)
        gate = ExternalPcmTurns(commands, append, commit, warnings.append, timeout_sec=5)
        async def feed_until(end_ms):
            for offset in range(gate.decoded_bytes, end_ms * 48, 4800):
                await gate.feed(source[offset:min(offset + 4800, end_ms * 48)])
        await feed_until(39000)
        if previous_turn:
            commands.append((38000, "begin", time.monotonic()))
            await gate.tick()
            await feed_until(41072)
            commands.append((40416, "commit", time.monotonic()))
            await gate.tick()
            assert commits[-1]["committed_audio_end_ms"] == 41072
        await feed_until(43672)
        commands.extend([(40448, "begin", time.monotonic()), (44192, "commit", time.monotonic())])
        await gate.tick()
        assert gate.active, "A retained begin is not rejected just because it is late or partly sent."
        await feed_until(45000)
        expected_start = 36500 if previous_turn else 38948
        assert sent == source[expected_start * 48:44192 * 48], "Source samples are recovered exactly once."
        assert len(commits) == (2 if previous_turn else 1) and not gate.active
        assert not any("trop ancien" in warning for warning in warnings), warnings
        assert len(gate.history) <= 8000 * 48
    asyncio.run(scenario(False))
    asyncio.run(scenario(True))


def test_reordered_boundaries_and_already_sent_turns():
    """FB2/FB3/FB4: reordered commits and fully covered turns neither lose a following begin nor replay audio."""
    async def scenario():
        source = source_pcm(15000)
        commands, sent, commits, warnings = [], bytearray(), [], []
        async def append(raw):
            sent.extend(raw)
        async def commit(**metadata):
            commits.append(metadata)
        gate = ExternalPcmTurns(commands, append, commit, warnings.append, timeout_sec=5)
        commands.append((200, "begin", time.monotonic()))
        await gate.feed(source[:4000 * 48])
        commands.append((2100, "begin", time.monotonic()))
        await gate.tick()
        assert commands and gate.opened_at == 200 * 48, "Keep the next begin until the preceding commit arrives."
        commands.extend([(2000, "commit", time.monotonic()), (2500, "commit", time.monotonic())])
        await gate.tick()
        assert not commands and not gate.active and len(commits) == 1
        assert sent == source[:4000 * 48], "An entirely sent turn causes no duplicate PCM or empty commit."
        commands.extend([(3000, "begin", time.monotonic()), (4500, "commit", time.monotonic())])
        await gate.feed(source[4000 * 48:5000 * 48])
        assert sent == source[:4500 * 48] and len(commits) == 2
        assert commits[-1]["audio_start_ms"] == 3000 and commits[-1]["committed_audio_start_ms"] == 4000
        # A valid begin at the history edge may recover less than 1.5s, but never fabricates lost PCM.
        for offset in range(5000 * 48, 14000 * 48, 4800):
            await gate.feed(source[offset:offset + 4800])
        commands.extend([(6100, "begin", time.monotonic()), (6300, "commit", time.monotonic())])
        await gate.tick()
        assert commits[-1]["committed_audio_start_ms"] == 6000
        assert sent == source[:4500 * 48] + source[6000 * 48:6300 * 48]
        assert not warnings and len(gate.history) <= HISTORY_BYTES
    asyncio.run(scenario())


def decode_pcm(data, *, input_rate=None, rate=24000):
    """Decode or resample test-owned bytes with the same native Opus/PCM path as the blocks."""
    options = ["-f", "s16le", "-ar", str(input_rate), "-ac", "1"] if input_rate else ["-c:a", "opus"]
    return subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", *options, "-i", "pipe:0",
                           "-ar", str(rate), "-ac", "1", "-c:a", "pcm_s16le", "-f", "s16le", "pipe:1"],
                          input=data, capture_output=True, check=True, timeout=10).stdout


def vad_fixtures():
    """Reuse the unchanged VAD's local speech/noise fixtures only for cross-block regression tests."""
    path = FIX.ROOT / "blocs/voice_activity_detection/tests/F5.51_voice_activity_detection.py"
    spec = importlib.util.spec_from_file_location("stt_onset_vad_fixtures", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_strict_vad_noise_and_weak_speech_prefix():
    """FB2/FB3: unchanged Silero rejects sound-only fixtures; weak confirmed speech keeps its actual onset."""
    vad = vad_fixtures()
    tone = array("h", (round(10000 * math.sin(2 * math.pi * 440 * i / 16000)) for i in range(32000)))
    if sys.byteorder != "little":
        tone.byteswap()
    noises = {"silence": bytes(64000), "quiet noise": vad.noise_pcm(seconds=2),
              "clicks": vad.noise_pcm(12000, clicks=True, seconds=2), "DC": b"\xe8\x03" * 32000,
              "tone": tone.tobytes()}
    async def check_noise(pcm, confirmation):
        events, commands, sent, commits = [], [], bytearray(), []
        detector = vad._Detector({**vad.DEFAULTS, "speech_start_ms": confirmation}, "noise", events.append)
        detector.feed(pcm)
        detector.finish()
        assert not events, "The onset fix must not relax noise rejection."
        async def append(raw):
            sent.extend(raw)
        async def commit(**metadata):
            commits.append(metadata)
        gate = ExternalPcmTurns(commands, append, commit, lambda message: None, timeout_sec=5)
        await gate.feed(decode_pcm(pcm, input_rate=16000))
        await gate.finish()
        assert not sent and not commits, "The longer history never sends sound without a confirmed begin."
    for pcm in noises.values():
        for confirmation in (60, 160):
            asyncio.run(check_noise(pcm, confirmation))

    async def check_voice(phrase, gain):
        values = array("h", vad.pcm_speech(phrase))
        if sys.byteorder != "little":
            values.byteswap()
        values = array("h", (round(value * gain) for value in values))
        audible = []
        for index in range(0, len(values) - 160, 160):
            window = values[index:index + 160]
            mean = sum(window) / 160
            if sum((value - mean) ** 2 for value in window) / 160 > (32768 * 10 ** (-48 / 20)) ** 2:
                audible.append(index // 16)
        if sys.byteorder != "little":
            values.byteswap()
        pcm = values.tobytes()
        events = []
        detector = vad._Detector(vad.DEFAULTS, "voice", events.append)
        detector.feed(pcm)
        detector.finish()
        assert [event["event"] for event in events] == ["speech_started", "speech_stopped"]
        start, end = events
        assert start["detection"]["speech_threshold"] == .7 and start["detection"]["confirmation_ms"] == 160
        source = decode_pcm(pcm, input_rate=16000) + bytes(3200 * 48)
        commands, sent, commits, warnings = [], bytearray(), [], []
        async def append(raw):
            sent.extend(raw)
        async def commit(**metadata):
            commits.append(metadata)
        gate = ExternalPcmTurns(commands, append, commit, warnings.append, timeout_sec=5)
        pending = list(events)
        for offset in range(0, len(source), 4800):
            await gate.feed(source[offset:offset + 4800])
            while pending and pending[0]["audio_end_ms"] + 3200 <= gate.decoded_bytes // 48:
                event = pending.pop(0)
                action = "begin" if event["event"] == "speech_started" else "commit"
                commands.append((event["audio_start_ms" if action == "begin" else "audio_end_ms"], action, time.monotonic()))
            await gate.tick()
        await gate.finish()
        assert not pending and len(commits) == 1 and not warnings
        first = commits[0]["committed_audio_start_ms"]
        last = commits[0]["committed_audio_end_ms"]
        assert first <= audible[0] < start["audio_start_ms"], "Retain the actual phoneme onset before strict detection."
        assert sent == source[first * 48:last * 48], "The prefix must contain original PCM, not silence or duplicated bytes."
    for phrase, gain in (("Hello can you hear me", 1), ("I think we should check this", .1),
                         ("I think we should check this", .03)):
        asyncio.run(check_voice(phrase, gain))


def test_real_opus_listener_preserves_weak_first_word():
    """FB1–FB5: actual Opus decoding and WebSocket appends retain the waveform preceding a strict VAD begin."""
    vad = vad_fixtures()
    raw = decode_pcm(vad.pcm_speech("I think we should check this"), input_rate=16000, rate=16000)
    encoded = subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "s16le", "-ar", "16000",
        "-ac", "1", "-i", "pipe:0", "-af", "volume=0.03,apad=pad_dur=3", "-ar", "48000",
        "-c:a", "libopus", "-b:a", "128000", "-f", "ogg", "-page_duration", "100000", "pipe:1"],
        input=raw, capture_output=True, check=True, timeout=10).stdout
    pcm16, reference = decode_pcm(encoded, rate=16000), decode_pcm(encoded)
    events = []
    detector = vad._Detector(vad.DEFAULTS, "capture", events.append)
    detector.feed(pcm16)
    detector.finish()
    assert [event["event"] for event in events] == ["speech_started", "speech_stopped"]
    start, end = events
    assert start["audio_start_ms"] < 2000 and end["audio_end_ms"] < len(reference) // 48
    ready = Event()
    feed = ExternalPcmTurns.feed
    async def observe_decoded(gate, data):
        """Observe the actual decoded source clock, without replacing audio or blocking the listener."""
        await feed(gate, data)
        if gate.decoded_bytes >= (start["audio_end_ms"] + 3200) * 48:
            ready.set()
    with patch.object(ExternalPcmTurns, "feed", observe_decoded), FIX.fake_openai() as api, \
            FIX.active_listener(config={"segmentation": "external", "drain_timeout_sec": 2}) as client:
        client.command("start")
        FIX.until(lambda: api.opened == 1, "Open only the explicit capture session.")
        client.frame(encoded[:-100], channels=1)
        assert ready.wait(5), "The audio must actually precede its speech command by more than two seconds."
        assert not api.pcm and not api.completed_items
        client.command("begin", audio_start_ms=start["audio_start_ms"])
        client.command("commit", audio_end_ms=end["audio_end_ms"])
        FIX.until(lambda: client.observed(port="final_out"), "A real locally acknowledged phrase must finalize before Stop.")
        cuts = [result.metadata["openai_realtime_stt"] for result in client.results
                if result.metadata.get("openai_realtime_stt", {}).get("state") == "segment_committed"]
        assert len(cuts) == 1
        first, last = cuts[0]["committed_audio_start_ms"], cuts[0]["committed_audio_end_ms"]
        assert first == max(0, start["audio_start_ms"] - 1500)
        # Native reads may end between milliseconds; metadata rounds down, PCM must remain exact.
        expected = reference[first * 48:first * 48 + len(api.pcm)]
        assert (first * 48 + len(api.pcm)) // 48 == last
        assert api.pcm == expected, f"Native decoded PCM differs: sent={len(api.pcm)}, reference={len(expected)}, cuts={cuts}"
        assert len(api.completed_items) == 1 and api.closed == 0
        client.command("stop", frame_count=2, byte_count=len(encoded), aborted=False)
        client.frame(encoded[-100:], channels=1)
        FIX.until(lambda: client.observed(state="completed"), "Stop must finalize only the capture, not append its idle tail.")
        assert len(api.completed_items) == 1 and api.pcm == expected


def test_pcm_idle_preroll_offsets_and_safety():
    """FB2/FB3/FB4: no idle append, exact early commits, honest late cuts, expiry and 60-second cap."""
    async def scenario():
        """Advance decoded audio time directly, without real-time sleeps or provider connections."""
        commands, output, commits, warnings = [], bytearray(), [], []
        now = [0.0]

        async def append(raw):
            """Record PCM actually admitted to the fake remote buffer."""
            output.extend(raw)

        async def commit(**metadata):
            """Record turn closure metadata without fabricating a transcription result."""
            commits.append(metadata)

        gate = ExternalPcmTurns(commands, append, commit, warnings.append, timeout_sec=5, clock=lambda: now[0])

        async def feed(milliseconds):
            """Feed bounded PCM reads while maintaining a deterministic source clock."""
            for _ in range(milliseconds // 100):
                await gate.feed(b"\x01\x00" * 2400)
                assert len(gate.history) <= HISTORY_BYTES

        await feed(120_000)
        assert not output and not commits, "Two minutes of idle audio must not create remote turns."
        commands.append((118_500, "begin", now[0]))
        commands.append((120_500, "commit", now[0]))
        await gate.tick()
        assert len(output) == 3000 * 48
        await feed(1000)
        assert len(output) == 3500 * 48 and not gate.active
        assert commits[-1] == {"reason": "external", "audio_start_ms": 118_500,
                               "committed_audio_start_ms": 117_000,
                               "audio_end_ms": 120_500, "committed_audio_end_ms": 120_500}
        commands.append((119_000, "begin", now[0]))
        await gate.tick()
        assert warnings and not gate.active, "Old or already transcribed beginnings are never truncated silently."
        commands.extend([(121_000, "begin", now[0]), (121_500, "commit", now[0])])
        await feed(1000)
        assert len(commits) == 2 and len(output) == 4500 * 48
        commands.append((122_000, "begin", now[0]))
        await gate.tick()
        await feed(400)
        commands.append((122_200, "commit", now[0]))
        await gate.tick()
        assert commits[-1]["committed_audio_end_ms"] == 122_400
        assert commits[-1]["audio_end_ms"] == 122_200, "Late commits cannot retrospectively remove appended audio."
        commands.extend([(122_400, "begin", now[0]), (120_500, "commit", now[0])])
        await gate.tick()
        assert gate.active, "A duplicate old commit cannot close a newer turn."
        commands.append((130_000, "begin", now[0]))
        now[0] = 6
        await gate.tick()
        assert not commands and "expired" in warnings[-1]
        await feed(60_000)
        assert commits[-1]["reason"] == "safety" and gate.active
        commands.append((182_400, "commit", now[0]))
        await gate.tick()
        assert not gate.active, "A commit at the safety cut still closes the active speech turn."
        count = len(commits)
        await feed(100)
        assert len(commits) == count
        commands.append((182_500, "begin", now[0]))
        await feed(100)
        await gate.finish()
        assert commits[-1]["reason"] == "stop" and not gate.active
        count = len(commits)
        await gate.finish()
        assert len(commits) == count, "Repeated finish cannot commit an empty remote buffer."

    asyncio.run(scenario())


def test_real_listener_external_phrases_before_stop():
    """FB1–FB5: commands can precede start/audio, each phrase finalizes on one open WebSocket."""
    audio = FIX.encoded_audio()
    with FIX.fake_openai() as api, FIX.active_listener(config={"segmentation": "external", "drain_timeout_sec": 2}) as client:
        client.command("begin", audio_start_ms=0)
        client.command("commit", audio_end_ms=1000)
        client.command("begin", audio_start_ms=1500)
        client.command("commit", audio_end_ms=2500)
        client.command("start")
        FIX.until(lambda: api.opened == 1, "Queued boundaries cannot replace the required source start.")
        assert not api.pcm and not api.completed_items
        client.frame(audio[:-100])
        FIX.until(lambda: len(api.completed_items) == 2, "Two externally ended phrases must finalize without source stop.")
        FIX.until(lambda: client.observed(port="final_out"), "Confirmed phrase must leave final_out.")
        assert api.opened == 1 and api.closed == 0
        assert len(api.pcm) == 2500 * 48, "The second confirmed turn recovers its prefix, without replaying the first."
        assert [o.value for r in client.results for o in r.outputs if o.port_name == "final_out"] == ["final 1", "final 2"]
        client.command("commit", audio_end_ms=2500)
        client.command("begin", audio_start_ms=1500)
        client.command("stop", frame_count=2, byte_count=len(audio), aborted=False)
        client.frame(audio[-100:])
        FIX.until(lambda: client.observed(state="completed"), "Stop must close the capture without committing idle tail.")
        assert len(api.completed_items) == 2 and len(api.pcm) == 2500 * 48
        assert not client.observed(state="error") and not client.host.failure
        assert FIX.KEY not in str(client.results)


def test_rejected_begin_never_poison_next_utterances():
    """FB2/FB3/FB4: speech outside the 8s history cannot poison healthy following utterances."""
    async def scenario():
        """A lost old begin must not expire healthy subsequent speech, even in one command batch."""
        commands, output, commits, warnings = [], bytearray(), [], []
        async def append(raw):
            output.extend(raw)
        async def commit(**metadata):
            commits.append(metadata)
        gate = ExternalPcmTurns(commands, append, commit, warnings.append, timeout_sec=5, clock=lambda: 1)
        for _ in range(100):
            await gate.feed(bytes(4800))
        commands.extend([(0, "begin", 1), (1000, "commit", 1), (9500, "begin", 1)])
        await gate.tick()
        assert gate.active and not commands, "An old orphan commit must not block a healthy begin."
        assert len(output) == 2000 * 48 and len(warnings) == 1
        assert "start=0ms" in warnings[0] and "decoded=10000ms" in warnings[0]
        assert "prebuffer_since=2000ms" in warnings[0]
        commands.append((10000, "commit", 1))
        await gate.tick()
        assert len(commits) == 1 and not gate.active
        # No begin at all: wait only until the next begin disambiguates the orphan.
        for _ in range(10):
            await gate.feed(bytes(4800))
        commands.append((10500, "commit", 1))
        await gate.tick()
        assert len(commands) == 1
        commands.append((10800, "begin", 1))
        await gate.tick()
        assert gate.active and not commands and len(output) == 2500 * 48
        assert "without an accepted start" in warnings[-1]
        assert len(gate.history) <= HISTORY_BYTES
    asyncio.run(scenario())


def test_external_graph_modes():
    """FB1/FB4/FB5: real compiled command batch/audio edges publish final Display before source stop."""
    import zmq
    payload = FIX.document()
    payload["nodes"][1]["config"].update(segmentation="external", drain_timeout_sec=2)
    document, graph = compile_runtime_graph_document(GraphDocument.from_payload(payload))
    with TemporaryDirectory(prefix="stt-external-graph-") as directory, FIX.fake_openai() as api:
        root = Path(directory)
        wallet = FIX.SecretManager(root / "secrets")
        wallet.initialize("test-only-wallet-password")
        wallet.set_secret(ref=FIX.REF, value=FIX.KEY)
        engine = FIX.WorkflowOrchestrator(root_dir=root, runs_dir=root / "runs", secret_manager=wallet)
        run = engine.prepare_active_run(graph, document=document, run_data_scope=instance_scope(root, payload))
        assert run.status == "prepared", run.logs
        session = engine._active_sessions[run.run_id]
        source = run.plan.worker_configs["micro"]
        audio_client = session.controller.runtime_audio_stream_service.client_for("micro", port_routes=source.runtime_audio_stream_port_routes)
        topic = next(port.topic for port in source.outputs if port.port_id == 2)
        publisher = zmq.Context.instance().socket(zmq.PUB)
        publisher.setsockopt(zmq.LINGER, 0)
        publisher.connect(session.pub_endpoint)
        try:
            time.sleep(.2)  # Attach only the extra test publisher; production Run readiness remains framework-owned.
            commands = [{"action": "start", "stream_id": "capture"}, boundary("begin", 0), boundary("commit", 1000)]
            envelope = FIX.MessageEnvelope(run_id=run.run_id, source_node_id="micro", source_port_id=2,
                payload=json.dumps(commands), content_type="application/json", sequence=1)
            publisher.send_multipart([topic.encode(), envelope.to_json().encode()])
            FIX.until(lambda: api.opened == 1, "Real graph must accept the explicit batch.")
            audio = FIX.encoded_audio()
            audio_client.publish_port("audio_out", audio[:-100], codec="opus", sample_rate_hz=48000, channels=2, stream_id="capture")
            FIX.until(lambda: run.output_values.get("stt:2", {}).get("value") == "final 1"
                      and run.node_statuses.get("final") == "success", "Final Display must execute before microphone stop.")
            assert api.closed == 0 and len(api.completed_items) == 1
            stop = {"action": "stop", "stream_id": "capture", "frame_count": 2, "byte_count": len(audio), "aborted": False}
            envelope = FIX.MessageEnvelope(run_id=run.run_id, source_node_id="micro", source_port_id=2,
                payload=json.dumps(stop), content_type="application/json", sequence=2)
            publisher.send_multipart([topic.encode(), envelope.to_json().encode()])
            audio_client.publish_port("audio_out", audio[-100:], codec="opus", sample_rate_hz=48000, channels=2, stream_id="capture")
            FIX.until(lambda: run.results.get("stt", {}).get("openai_realtime_stt", {}).get("state") == "completed", "Capture must finish normally.")
            assert len(api.pcm) == 48_000 and len(api.completed_items) == 1
        finally:
            publisher.close(0)
            engine.stop_active_run(run.run_id)
        opened = api.opened
        simulation = engine.create_run(graph, document=document, runtime_mode="centralized", auto_start=False)
        engine._execute_run(simulation)
        assert simulation.status == "success" and api.opened == opened
        assert "stt:1" not in simulation.output_values and "stt:2" not in simulation.output_values


def test_external_properties_browser(page, server, blocking_errors):
    """FB6: save the mode in the actual editor shell and inspect desktop/narrow opaque properties."""
    from ui_smoke_common import create_project_api, project_editor_url, wait_for_app_ready
    payload = FIX.document()
    for index, node in enumerate(payload["nodes"]):
        node["position"] = {"x": 80 + index * 300, "y": 190}
    project = create_project_api(server, title="STT tours externes", document=payload)["project"]
    graph_id = project.get("graph_id") or project["project_id"]
    editor_url = project_editor_url(server.base_url, graph_id, workspace_project_id=project["workspace_project_id"])
    wait_for_app_ready(page, editor_url)
    page.locator('.canvas-node[data-node-id="stt"] [data-openai-realtime-stt-node-card] strong').dblclick()
    modal = page.locator("[data-openai-realtime-stt-modal-root]")
    modal.wait_for()
    choice = modal.locator('[data-stt-setting="segmentation"]')
    assert choice.input_value() == "duration"
    assert "8 s audio memory" in modal.inner_text() and "1.5 s before the detected start" in modal.inner_text()
    choice.focus()
    page.keyboard.press("ArrowDown")
    page.keyboard.press("Tab")
    assert choice.input_value() == "external"
    modal.locator("[data-stt-apply]").click()
    FIX.until(lambda: modal.locator("[data-stt-apply]").is_disabled(), "The mode must save through the normal UI action.")
    for width, height in ((1440, 1000), (390, 780), (320, 640)):
        page.set_viewport_size({"width": width, "height": height})
        modal.locator(".stt-body").evaluate("element => element.scrollTop = 0")
        assert modal.evaluate("element => getComputedStyle(element).backgroundColor") == "rgb(255, 255, 255)"
        assert modal.evaluate("element => element.scrollWidth <= element.clientWidth + 1")
        assert modal.locator(".stt-body").evaluate("element => element.scrollWidth <= element.clientWidth + 1")
        bounds = modal.locator("[data-stt-apply]").bounding_box()
        assert bounds and 0 <= bounds["y"] and bounds["y"] + bounds["height"] <= height
        page.screenshot(path=artifact_path(f"stt-external-mode-{width}.png"))
        choice.locator("..").evaluate("element => element.scrollIntoView({block: 'start'})")
        assert modal.locator(".stt-body").evaluate("element => element.scrollWidth <= element.clientWidth + 1")
        page.screenshot(path=artifact_path(f"stt-external-help-{width}.png"))
    modal.locator("[data-close-block-modal]").click()
    page.set_viewport_size({"width": 1440, "height": 1000})
    wait_for_app_ready(page, editor_url)
    page.locator('.canvas-node[data-node-id="stt"] [data-openai-realtime-stt-node-card] strong').dblclick()
    assert page.locator('[data-openai-realtime-stt-modal-root] [data-stt-setting="segmentation"]').input_value() == "external"
    assert not blocking_errors, blocking_errors


if __name__ == "__main__":
    if "--ui" in sys.argv:
        from ui_smoke_common import run_playwright_smoke
        run_playwright_smoke("F5.52 STT external properties", test_external_properties_browser)
    else:
        for test in (test_command_validation_batches_and_modes,
                     test_onset_prefix_preserves_buffered_audio_without_idle_publication,
                     test_delayed_begin_recovers_instance_offsets_without_duplicate_pcm,
                     test_reordered_boundaries_and_already_sent_turns,
                     test_strict_vad_noise_and_weak_speech_prefix,
                     test_real_opus_listener_preserves_weak_first_word,
                     test_pcm_idle_preroll_offsets_and_safety,
                     test_rejected_begin_never_poison_next_utterances,
                     test_real_listener_external_phrases_before_stop, test_external_graph_modes):
            test()
            print(f"[ok] {test.__name__}", flush=True)
