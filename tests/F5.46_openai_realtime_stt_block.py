#!/usr/bin/env python3
"""F5.46: Real decoding, fake OpenAI, real listener/graph services and both modes.

FB1/FB2: independent ports, start/stop integrity, prebuffer, cancellation and reuse.
FB3/FB4: streaming PCM/previews, real WebSocket, confirmed segment outputs before stop.
FB5/FB6: safe secrets, validated settings, block-owned UI, discovery and mini-graphs.
No real credential or paid OpenAI call is used by this test file.
"""

from __future__ import annotations

import asyncio
import base64
from contextlib import contextmanager
from functools import lru_cache
import json
from html.parser import HTMLParser
from pathlib import Path
import subprocess
import sys
import tempfile
from threading import Event, Thread
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from blocs.openai_realtime_stt.block import (
    BOUNDS, DEFAULTS, MODEL, REALTIME_URL, MAX_BUFFER, MAX_TEXT, OpenAIRealtimeSttBlock,
    RealtimeSttError, _Capture, _Transcripts, _command, _config, _failure,
)
from blocs.microphone_stream.block import MicrophoneStreamBlock
from blocs.display.block import DisplayBlock
from blocs.registry import get_block_definition
from bloxsmith_app.block_api import (
    BlockRuntimeContext, BlockRuntimePreparation, RuntimeAudioStreamBinding,
    RuntimeAudioStreamPortRoute,
)
from bloxsmith_app.block_runtime import BlockRuntimePreparationContext
from bloxsmith_app.active_runtime.listener import RuntimeListenerHost
from bloxsmith_app.runtime_audio_streams import RuntimeAudioStreamService
from bloxsmith_app.graph import WorkflowGraph
from bloxsmith_app.messages import MessageEnvelope
from bloxsmith_app.orchestrator import WorkflowOrchestrator
from block_test_fixtures import instance_scope
from bloxsmith_app.secrets import SecretManager
from ui_smoke_common import (
    create_project_api, create_run_api, expect, graph_payload, http_json,
    isolated_server, wait_for_run_terminal,
)
from urllib.parse import quote
from block_test_packages import install_test_package, release_key, surface_payload

KEY = "test-only-secret-STT-73"
REF = "secret://workspace/openai-test"


def until(predicate, message, timeout=6):
    """Wait for an observable asynchronous condition with an explicit test deadline."""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(message)


def context_for(mode="zeromq_active", config=None, services=None):
    """Create a public context using exactly the manifest's static port contract."""
    block = OpenAIRealtimeSttBlock()
    return BlockRuntimeContext(
        run_id="run-stt-test", node_id="stt", kind=block.kind, title=block.default_title(),
        config={**DEFAULTS, "api_key_ref": REF, "segment_seconds": 1,
                "drain_timeout_sec": 0.25, "final_timeout_sec": 2, **(config or {})},
        input_ports=tuple(SimpleNamespace(**port) for port in block.model["ports"]["inputs"]),
        output_ports=tuple(SimpleNamespace(**port) for port in block.model["ports"]["outputs"]),
        runtime_mode=mode, services=services or {}, root_dir=ROOT,
    )


@lru_cache(maxsize=3)
def encoded_audio(container="webm"):
    """Generate deterministic browser-like encoded mono/stereo audio only in memory."""
    options = {
        "webm": ["-ac", "2", "-c:a", "libopus", "-b:a", "64000", "-f", "webm", "-cluster_time_limit", "100"],
        "ogg": ["-ac", "1", "-c:a", "libopus", "-b:a", "32000", "-f", "ogg", "-page_duration", "100000"],
        "mp4": ["-ac", "1", "-c:a", "aac", "-b:a", "64000", "-f", "mp4", "-movflags", "frag_keyframe+empty_moov+default_base_moof", "-frag_duration", "100000"],
    }
    return subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
        "sine=frequency=440:sample_rate=48000", "-t", "3.3", *options[container], "pipe:1"],
        check=True, capture_output=True, timeout=10).stdout


@contextmanager
def fake_openai(mode="normal"):
    """Serve a real local WebSocket; only the test patches the production URL boundary."""
    from websockets.sync.server import serve
    from websockets.exceptions import ConnectionClosed
    from websockets.asyncio.client import connect

    observations = SimpleNamespace(messages=[], pcm=bytearray(), opened=0, closed=0, headers=[],
                                   waiting_for_final=Event(), release_finals=Event(), completed_items=[])

    def handler(ws):
        """Return realistic deltas, commit acknowledgments and optional faults/reordering."""
        observations.opened += 1
        observations.headers.append(ws.request.headers.get("Authorization"))
        index = 1
        spoke = False
        held = None
        try:
            for raw in ws:
                event = json.loads(raw)
                observations.messages.append(event)
                kind = event["type"]
                if kind == "session.update":
                    if mode == "api_error":
                        ws.send(json.dumps({"type": "error", "error": {"code": "invalid_api_key", "message": KEY}}))
                    elif mode != "no_ready":
                        ws.send(json.dumps({"type": "session.updated", "session": event["session"]}))
                elif kind == "input_audio_buffer.append":
                    observations.pcm.extend(base64.b64decode(event["audio"], validate=True))
                    if mode == "disconnect":
                        ws.close()
                        return
                    if not spoke:
                        ws.send(json.dumps({"type": "conversation.item.input_audio_transcription.delta",
                            "item_id": f"item-{index}", "delta": f"provisoire {index}"}))
                        spoke = True
                elif kind == "input_audio_buffer.commit":
                    ws.send(json.dumps({"type": "input_audio_buffer.committed", "item_id": f"item-{index}"}))
                    completed = {"type": "conversation.item.input_audio_transcription.completed",
                                 "item_id": f"item-{index}", "transcript": f"final {index}"}
                    if mode == "hold_final" or (mode == "hold_last" and index == 4):
                        observations.waiting_for_final.set()
                        if not observations.release_finals.wait(timeout=5):
                            return
                    if mode == "reorder":
                        if index % 2:
                            held = completed
                        else:
                            ws.send(json.dumps(completed))
                            ws.send(json.dumps(held))
                            observations.completed_items.extend((completed["item_id"], held["item_id"]))
                            held = None
                    elif mode != "no_final":
                        ws.send(json.dumps(completed))
                        observations.completed_items.append(completed["item_id"])
                    index += 1
                    spoke = False
        except ConnectionClosed:
            pass
        finally:
            observations.closed += 1

    server = serve(handler, "127.0.0.1", 0, compression=None)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.socket.getsockname()[1]

    async def local_connect(_self, api_key, config):
        """Exercise the real client against a test-only URL, never a configurable production URL."""
        return await connect(f"ws://127.0.0.1:{port}", additional_headers={"Authorization": f"Bearer {api_key}"},
                             close_timeout=0.1, compression=None, proxy=None, max_size=1_048_576)

    try:
        with patch.object(OpenAIRealtimeSttBlock, "_connect", local_connect):
            yield observations
    finally:
        observations.release_finals.set()
        server.shutdown()
        thread.join(timeout=2)
        expect(not thread.is_alive(), "Mock OpenAI server must stop.")


@contextmanager
def active_listener(config=None, resolver=None):
    """Run the real audio service and supervised listener without Play or graph mutation."""
    block = OpenAIRealtimeSttBlock()
    service = RuntimeAudioStreamService(run_id="run-stt-test")
    topic = "test/stt"
    binding = RuntimeAudioStreamBinding(topic=topic, subscribe=True, codecs=("opus", "aac"), channels=(1, 2))
    service.register_preparations({"stt": BlockRuntimePreparation()}, additional_bindings={"stt": (binding,)})
    service.open()
    receiver = service.client_for("stt", port_routes=(RuntimeAudioStreamPortRoute(
        port_id=1, port_name="audio_in", direction="input", topic=topic, codecs=("opus", "aac"), channels=(1, 2)),))
    publisher = service.external_client("micro-test", (RuntimeAudioStreamBinding(topic=topic, publish=True,
                        codecs=("opus", "aac"), channels=(1, 2)),))
    context = context_for(config=config, services={"runtime_audio_streams": receiver, "resolve_secret": resolver or (lambda ref: KEY)})
    gate = Event()
    gate.set()
    host = RuntimeListenerHost(context=BlockRuntimePreparationContext.from_context(context), services=context.services,
        hook=block.listen_runtime, stop_event=Event(), ready_gate=gate)
    context.services["runtime_listener"] = host.client
    results = []

    def command(action, stream_id="capture", **fields):
        """Publish JSON through ordinary execute_runtime; execution must remain short."""
        context.input_attribute("command_in").update(json.dumps({"action": action, "stream_id": stream_id, **fields}))
        started = time.monotonic()
        result = block.execute_runtime(context)
        expect(result.status == "success", f"Command failed: {result.error}")
        expect(time.monotonic() - started < 0.1, "The ordinary worker must not wait for audio/OpenAI.")

    def frame(payload, stream_id="capture", codec="opus", channels=2, **metadata):
        """Deliver real encoded bytes on audio independently from data commands."""
        publisher.publish(topic, payload, codec=codec, sample_rate_hz=48000, channels=channels,
                          stream_id=stream_id, **metadata)

    def observed(state=None, port=None):
        """Drain real listener results while retaining assertions across observations."""
        results.extend(host.pop_results())
        return any((state and result.metadata.get("openai_realtime_stt", {}).get("state") == state)
                   or (port and any(output.port_name == port for output in result.outputs)) for result in results)

    host.start()
    try:
        yield SimpleNamespace(command=command, frame=frame, observed=observed, results=results, host=host)
    finally:
        began = time.monotonic()
        host.request_stop()
        service.close()
        host.close()
        expect(not host._thread.is_alive() and time.monotonic() - began < 1, "Stop must join the listener within one second.")


def test_contract_secrets_and_simulation():
    """FB1/FB5/FB6: validate every setting family and ensure simulation has no side effects."""
    block = OpenAIRealtimeSttBlock()
    expect(get_block_definition(block.kind).kind == block.kind, "Discover the autonomous package.")
    context = context_for("centralized", config={"api_key_ref": ""})
    with patch("subprocess.Popen", side_effect=AssertionError("No subprocess in simulation")), patch.object(block, "_connect", side_effect=AssertionError("No OpenAI in simulation")):
        expect(block.prepare_runtime(context).listen_on_run is False, "No listener in simulation.")
        expect(block.initialize_runtime(context).status == "success", "Simulation does not need credentials/tools.")
        result = block.execute_runtime(context)
        expect(result.status == "skipped" and not result.outputs, "No fabricated simulation transcript.")
    active = context_for()
    expect(block.prepare_runtime(active).listen_on_run, "Run must opt into ready audio listening.")
    expect(block.initialize_runtime(active).status == "failed", "Missing resolver must fail before networking.")
    active.services["resolve_secret"] = Mock(side_effect=RuntimeError(KEY))
    result = block.initialize_runtime(active)
    expect("unlock the wallet" in result.error and KEY not in str(result), "Wallet failures must not echo secrets.")
    active.services["resolve_secret"] = lambda ref: KEY
    expect(block.initialize_runtime(active).status == "success", "Configured active startup should validate.")
    for value in ("", "\r\ninvalid"):
        active.services["resolve_secret"] = lambda ref, value=value: value
        expect(block.initialize_runtime(active).status == "failed", "Empty/header-injecting credentials are invalid.")
    invalids = [{"api_key_ref": "sk-test-raw"}, {"api_key": KEY}, {"model": "whisper-1"},
        {"languages": "en,invalid-language-code"}, {"languages": ",".join(["fr"] * 9)},
        {"prompt": "a" * 1025}, {"delay": "fast"}, {"segment_seconds": 1.5}]
    for key, (low, high) in BOUNDS.items():
        invalids.extend({key: value} for value in (low - 1, high + 1, True, "NaN", "bad", None))
    for config in invalids:
        try:
            _config(config)
        except RealtimeSttError:
            pass
        else:
            raise AssertionError(f"Invalid config accepted: {list(config)}")
    expect(_config({"languages": "FR, en,fr"})["languages"] == "fr,en", "Normalize language hints.")
    for raw in ({}, {"action": []}, {"action": "start"}, "not-json", "x" * 5000,
                {"action": "stop", "stream_id": "s", "frame_count": True, "byte_count": 0},
                {"action": "stop", "stream_id": "s", "frame_count": 0, "byte_count": 0, "aborted": "false"}):
        try:
            _command(raw)
        except RealtimeSttError:
            pass
        else:
            raise AssertionError("Invalid command was accepted.")
    active.input_ports[0].transport = "message"
    expect(block.execute_runtime(active).status == "failed", "No data disguised as audio.")
    expect(KEY not in str(_failure(RuntimeError(KEY))), "Unexpected errors must not echo raw exceptions.")


def test_streaming_containers_and_reuse():
    """FB2/FB3/FB4: preview before stop, drain late chunks, emit ordered segment finals and reuse."""
    with fake_openai("reorder") as api, active_listener() as listener:
        for index, container in enumerate(("webm", "ogg", "mp4")):
            payload = encoded_audio(container)
            chunks = [payload[offset:offset + 1200] for offset in range(0, len(payload), 1200)]
            stream = f"stream-{container}"
            codec = "aac" if container == "mp4" else "opus"
            channels = 2 if container == "webm" else 1
            previous_bytes = len(api.pcm)
            # The first encoded chunk can arrive before its separate data start.
            listener.frame(chunks[0], stream, codec, channels)
            time.sleep(0.05)
            expect(api.opened == index, "Audio alone must never open OpenAI.")
            listener.command("start", stream)
            listener.command("start", stream)  # Duplicate start is idempotent.
            for chunk in chunks[1:-1]:
                listener.frame(chunk, stream, codec, channels)
                time.sleep(0.003)
            until(lambda: len(api.pcm) > previous_bytes, f"{container}: decoding must stream BEFORE stop, not wait for EOF.")
            until(lambda: listener.observed(port="partial_out") and any(
                r.metadata.get("openai_realtime_stt", {}).get("stream_id") == stream
                and any(o.port_name == "partial_out" for o in r.outputs) for r in listener.results),
                f"{container}: provisional text must stream before stop.")
            listener.command("stop", stream, frame_count=len(chunks), byte_count=len(payload))
            time.sleep(0.03)
            listener.frame(chunks[-1], stream, codec, channels)
            until(lambda: listener.observed("completed") and any(r.metadata.get("openai_realtime_stt", {}).get("stream_id") == stream
                and r.metadata.get("openai_realtime_stt", {}).get("state") == "completed" for r in listener.results),
                f"{container} did not complete: {listener.results}")
            results = [r for r in listener.results if r.metadata.get("openai_realtime_stt", {}).get("stream_id") == stream]
            finals = [o.value for r in results for o in r.outputs if o.port_name == "final_out"]
            expect(finals == ["final 1", "final 2", "final 3", "final 4"], f"One final per segment, in audio order: {finals}")
            expect(all(r.metadata["openai_realtime_stt"]["is_final"] == (o.port_name == "final_out")
                       for r in results for o in r.outputs), "Preview metadata must never claim to be final.")
            count = len(api.pcm) - previous_bytes
            expect(150000 < count < 170000 and count % 2 == 0, f"Expected PCM16 mono 24 kHz for 3.3 s, got {count}.")
            listener.command("stop", stream, frame_count=len(chunks), byte_count=len(payload))
            until(lambda: api.closed == index + 1, "Business stop must close its connection, retaining the listener.")
        expect(all(value == f"Bearer {KEY}" for value in api.headers), "Keys go only in the server authorization header.")
        update = next(e for e in api.messages if e["type"] == "session.update")
        audio = update["session"]["audio"]["input"]
        expect(update["session"]["type"] == "transcription" and audio["transcription"]["model"] == MODEL, "Correct STT API session/model.")
        expect(audio["turn_detection"] is None and audio["format"] == {"type": "audio/pcm", "rate": 24000}, "Explicit turns and PCM input.")
        expect(KEY not in str(listener.results), "Results/logs must never carry the key.")


def test_final_waits_for_last_completion():
    """FB4: publish finished segments before stop; the unfinished tail alone waits for its final."""
    with fake_openai("hold_last") as api, active_listener() as listener:
        payload = encoded_audio()
        listener.command("start")
        listener.frame(payload[:-100])
        until(lambda: listener.observed(port="partial_out") and bool(api.completed_items),
              "Provisional output and completed segments must exist before stop.")
        until(lambda: listener.observed(port="final_out"), "Confirmed segments must publish before microphone stop.")
        listener.command("stop", frame_count=2, byte_count=len(payload))
        listener.frame(payload[-100:])
        until(api.waiting_for_final.is_set, "The final audio segment must reach OpenAI after decoder EOF.")
        expect(len(api.completed_items) == 3, "The first three segments are already finalized.")
        until(lambda: listener.observed(port="final_out") and len([
            o for r in listener.results for o in r.outputs if o.port_name == "final_out"]) == 3,
            "Publish all three confirmed segments while the fourth still waits.")
        expect(not listener.observed("completed"), "Session completion still waits for its final tail.")
        api.release_finals.set()
        until(lambda: listener.observed("completed"), "Last OpenAI completion must finish the session.")
        expect([o.value for r in listener.results for o in r.outputs if o.port_name == "final_out"]
               == ["final 1", "final 2", "final 3", "final 4"], "Publish the last segment once, with no aggregate or duplicate on stop.")


def test_errors_empty_and_shutdown():
    """FB2/FB3/FB5: no false success on data loss, transport errors, timeouts or cancellation."""
    payload = encoded_audio()
    for mode in ("missing", "aborted", "gap", "bad_header", "invalid_container", "api_error", "disconnect", "no_final", "hold_last", "no_ready"):
        with fake_openai(mode) as api, active_listener(config={"connect_timeout_sec": 1, "final_timeout_sec": 1}) as listener:
            listener.command("start")
            if mode == "gap":
                listener.frame(payload[:1000], sequence=1)
                listener.frame(payload[1000:2000], sequence=3)
            elif mode == "bad_header":
                listener.frame(b"not-a-container")
            elif mode == "invalid_container":
                broken = b"\x1aE\xdf\xa3broken-webm"
                listener.frame(broken)
                listener.command("stop", frame_count=1, byte_count=len(broken))
            elif mode == "missing":
                listener.command("stop", frame_count=1, byte_count=10)
            elif mode == "aborted":
                listener.command("stop", frame_count=0, byte_count=0, aborted=True)
            elif mode in {"disconnect", "no_final", "hold_last"}:
                listener.frame(payload)
                listener.command("stop", frame_count=1, byte_count=len(payload))
            until(lambda: listener.observed("error"), f"{mode} must fail explicitly.")
            expect(not listener.observed("completed"), f"{mode} must not falsely complete.")
            finals = [o.value for r in listener.results for o in r.outputs if o.port_name == "final_out"]
            expect(finals == (["final 1", "final 2", "final 3"] if mode == "hold_last" else []),
                   "An error must not promote unfinished text; already confirmed segments remain valid outputs.")
            if mode == "hold_last":
                expect(len(api.completed_items) == 3, "Timeout fixture must have three confirmed segments and one missing final.")
            expect(KEY not in str(listener.results), "Never expose a key even in malicious error messages.")
    with fake_openai("no_final") as api, active_listener() as listener:
        listener.command("start", "empty")
        listener.command("stop", "empty", frame_count=0, byte_count=0)
        until(lambda: listener.observed("completed"), "Empty capture must finish without a commit.")
        expect(not any(r.outputs for r in listener.results), "An empty capture must not fabricate text.")
        listener.command("start", "cancelled")
        listener.frame(payload, "cancelled")
        until(lambda: listener.observed(port="partial_out"), "Cancellation fixture must already expose live previews.")
    until(lambda: api.closed == api.opened, "Runtime Stop must close external connections too.")
    listener.observed()
    expect(not any(o.port_name == "final_out" for r in listener.results for o in r.outputs),
           "Runtime cancellation must not manufacture a final from pending previews.")

    async def stalled_connect(*args):
        """Model a stalled DNS/TLS/open operation without any real network side effect."""
        await asyncio.sleep(60)

    with patch.object(OpenAIRealtimeSttBlock, "_connect", stalled_connect), active_listener() as listener:
        listener.command("start")
        until(lambda: listener.observed("connecting"), "Stalled connection must still be cancellable.")

    with patch.object(OpenAIRealtimeSttBlock, "_connect", stalled_connect), active_listener() as listener:
        for index in range(5):
            listener.command("start", f"limit-{index}")
        until(lambda: listener.observed("error"), "Bound concurrent session count.")


def test_audio_bounds_and_connection_contract():
    """FB2/FB3/FB5: enforce byte/frame limits and protect the real OpenAI connection boundary."""
    def frame(sequence, payload=b"x", source="micro"):
        """Build a minimal immutable-frame-shaped input for admission checks."""
        return SimpleNamespace(stream_id="s", codec="opus", sample_rate_hz=48000,
                               channels=1, source_id=source, sequence=sequence, payload=payload)

    def rejected(operation):
        """Assert that an admission operation raises a block-owned safe error."""
        try:
            operation()
        except RealtimeSttError:
            return
        raise AssertionError("Invalid audio admission was accepted.")

    capture = _Capture("s")
    for sequence in range(1, 129):
        capture.feed(frame(sequence))
    rejected(lambda: capture.feed(frame(129)))
    rejected(lambda: _Capture("s").feed(frame(1, b"x" * (MAX_BUFFER + 1))))
    capture = _Capture("s")
    capture.feed(frame(1))
    rejected(lambda: capture.feed(frame(2, source="different-publisher")))
    capture = _Capture("s")
    capture.finish(_command({"action": "stop", "stream_id": "s", "frame_count": 0, "byte_count": 0}))
    rejected(lambda: capture.feed(frame(1)))
    rejected(lambda: capture.finish(_command({"action": "stop", "stream_id": "s", "frame_count": 2, "byte_count": 2})))

    async def connect_contract():
        """Mock just the library call, preserving the production URL/auth/error implementation."""
        from websockets.exceptions import InvalidStatus
        from websockets.http11 import Response
        from websockets.datastructures import Headers
        block = OpenAIRealtimeSttBlock()
        connection = object()
        with patch("websockets.asyncio.client.connect", AsyncMock(return_value=connection)) as connect:
            expect(await block._connect(KEY, _config({})) is connection, "Return the connected client.")
            args, kwargs = connect.call_args
            expect(args == (REALTIME_URL,) and REALTIME_URL.startswith("wss://api.openai.com/"), "Only the official TLS endpoint is allowed.")
            expect(KEY not in REALTIME_URL and kwargs["additional_headers"] == {"Authorization": f"Bearer {KEY}"}, "Authenticate only through the header.")
            expect(kwargs["max_size"] == 1_048_576 and kwargs["max_queue"] == 16, "Bound incoming WebSocket buffers.")
        for error, expected in ((ConnectionError(KEY), "network"),
                                (InvalidStatus(Response(401, "Unauthorized", Headers(), KEY.encode())), "API key"),
                                (InvalidStatus(Response(429, "Limit", Headers(), KEY.encode())), "Quota")):
            with patch("websockets.asyncio.client.connect", AsyncMock(side_effect=error)):
                try:
                    await block._connect(KEY, _config({}))
                except RealtimeSttError as exc:
                    expect(KEY not in str(exc) and expected in str(exc), "Use safe actionable connection diagnostics.")
                else:
                    raise AssertionError("Connection failures must propagate safely.")
    asyncio.run(connect_contract())


def test_final_before_stop():
    """FB4: emit a segment only on its OpenAI final event, without waiting for microphone stop."""
    async def scenario():
        """Replay one preview, its commit and a corrected final while the capture remains open."""
        results = []
        capture = _Capture("still-recording")
        transcripts = _Transcripts(SimpleNamespace(emit_result=results.append, stop_requested=lambda: False), capture)
        transcripts.event({"type": "conversation.item.input_audio_transcription.delta", "item_id": "1", "delta": "Bon"})
        expect([(o.port_name, o.value) for r in results for o in r.outputs] == [("partial_out", "Bon")],
               "Only partial_out may publish unfinished words.")
        transcripts.event({"type": "input_audio_buffer.committed", "item_id": "1"})
        expect(not any(o.port_name == "final_out" for r in results for o in r.outputs),
               "A commit alone is not a final transcription.")
        completed = {"type": "conversation.item.input_audio_transcription.completed", "item_id": "1", "transcript": "Bonjour !"}
        transcripts.event(completed)
        transcripts.event(completed)
        expect(capture.stop is None, "Do not send a stop just to release finalized speech.")
        expect([o.value for r in results for o in r.outputs if o.port_name == "final_out"] == ["Bonjour !"],
               "Publish the corrected final immediately and exactly once, while the microphone stays open.")
        expect(results[-1].metadata["openai_realtime_stt"]["is_final"], "The final segment must be explicitly identified.")
    asyncio.run(scenario())


def test_output_bounds_and_reconciliation():
    """FB4/FB5: keep previews distinct, publish ordered segment finals once and bound text."""
    async def scenario():
        """Use an async context for the transcript reducer's synchronization events."""
        results = []
        context = SimpleNamespace(emit_result=results.append, stop_requested=lambda: False)
        capture = _Capture("s", frame_count=1, byte_count=4)
        transcripts = _Transcripts(context, capture)
        transcripts.event({"type": "conversation.item.input_audio_transcription.delta", "item_id": "1", "delta": "Bon"})
        transcripts.event({"type": "conversation.item.input_audio_transcription.delta", "item_id": "1", "delta": "jour"})
        expect([(o.port_id, o.value) for r in results for o in r.outputs] == [(1, "Bon")],
               "The provisional port must stream while final_out stays silent; rapid deltas coalesce.")
        transcripts.last_partial_at = 0
        transcripts.flush_partial()
        expect(results[-1].outputs[0].value == "Bonjour", "A preview replaces the current segment snapshot.")
        transcripts.event({"type": "conversation.item.input_audio_transcription.completed", "item_id": "1", "transcript": "Bonjour !"})
        expect(not any(o.port_id == 2 for r in results for o in r.outputs), "Wait for audio order, not microphone stop.")
        transcripts.event({"type": "input_audio_buffer.committed", "item_id": "1"})
        transcripts.event({"type": "conversation.item.input_audio_transcription.completed", "item_id": "1", "transcript": "duplicate"})
        transcripts.event({"type": "input_audio_buffer.committed", "item_id": "2"})
        transcripts.event({"type": "conversation.item.input_audio_transcription.delta", "item_id": "2", "delta": "unfinished"})
        transcripts.event({"type": "input_audio_buffer.committed", "item_id": "3"})
        transcripts.event({"type": "conversation.item.input_audio_transcription.completed", "item_id": "3", "transcript": "Third"})
        expect([o.value for r in results for o in r.outputs if o.port_id == 2] == ["Bonjour !"],
               "The first final publishes immediately; item 3 waits only for item 2 to preserve audio order.")
        transcripts.event({"type": "conversation.item.input_audio_transcription.completed", "item_id": "2", "transcript": "  Second  "})
        finals = [o.value for r in results for o in r.outputs if o.port_id == 2]
        expect(capture.stop is None and finals == ["Bonjour !", "  Second  ", "Third"],
               f"Keep each API final verbatim and ordered, with no aggregate or stop requirement: {finals}")
        expect(results[-1].metadata["openai_realtime_stt"]["state"] == "transcribing", "Final segments do not close the capture.")
        expect(all(r.metadata["openai_realtime_stt"]["is_final"] == (o.port_id == 2)
                   for r in results for o in r.outputs), "Provisional and final metadata remain distinct.")
        for kind, key in (("delta", "delta"), ("completed", "transcript")):
            bounded = _Transcripts(context, _Capture("bounded"))
            try:
                bounded.event({"type": f"conversation.item.input_audio_transcription.{kind}",
                               "item_id": "1", key: "x" * (MAX_TEXT + 1)})
            except RealtimeSttError:
                pass
            else:
                raise AssertionError("Bound both provisional and finalized segment text.")

        # Long captures release each segment independently, without accumulating a full-capture buffer.
        bounded_results = []
        bounded = _Transcripts(SimpleNamespace(emit_result=bounded_results.append), _Capture("bounded"))
        for index in range(3):
            bounded.event({"type": "input_audio_buffer.committed", "item_id": str(index)})
            bounded.event({"type": "conversation.item.input_audio_transcription.completed",
                           "item_id": str(index), "transcript": "\U0001f600" * MAX_TEXT})
        expect(len(bounded_results) == 3 and not bounded.items, "Per-segment text limits must not become a capture-wide limit.")
        bounded.event({"type": "input_audio_buffer.committed", "item_id": "empty"})
        bounded.event({"type": "conversation.item.input_audio_transcription.completed", "item_id": "empty", "transcript": ""})
        expect(bounded_results[-1].outputs[0].value == "", "Preserve an explicitly empty API final as one empty segment output.")
    asyncio.run(scenario())


def document():
    """Wire independent preview and final sinks to verify their distinct execution timing."""
    nodes = [MicrophoneStreamBlock().build_node_payload(node_id="micro"),
             OpenAIRealtimeSttBlock().build_node_payload(node_id="stt", config_overrides={"api_key_ref": REF, "segment_seconds": 1}),
             DisplayBlock().build_node_payload(node_id="partial"), DisplayBlock().build_node_payload(node_id="final")]
    edges = [{"id": name, "from": {"node": src, "port": sp}, "to": {"node": dst, "port": dp}, "kind": "data"}
             for name, src, sp, dst, dp in (("audio", "micro", 1, "stt", 1), ("commands", "micro", 2, "stt", 2),
                  ("partial", "stt", 1, "partial", 1), ("final", "stt", 2, "final", 1))]
    return graph_payload("STT integration", nodes, edges)


def test_graph_active():
    """FB1-FB6: real workers execute preview/final Display sinks before microphone stop, without Play."""
    import zmq
    with tempfile.TemporaryDirectory(prefix="stt-graph-") as tmp, fake_openai("hold_final") as api:
        root = Path(tmp)
        wallet = SecretManager(root / "secrets")
        wallet.initialize("test-wallet-password")
        wallet.set_secret(ref=REF, value=KEY)
        engine = WorkflowOrchestrator(root_dir=root, runs_dir=root / "runs", secret_manager=wallet)
        payload = document()
        graph = WorkflowGraph.from_payload({**payload, "edges": [
            {"id": e["id"], "kind": e["kind"], "fromNodeId": e["from"]["node"], "fromPortId": e["from"]["port"],
             "toNodeId": e["to"]["node"], "toPortId": e["to"]["port"]} for e in payload["edges"]]})
        run = engine.prepare_active_run(graph, run_data_scope=instance_scope(root, payload))
        expect(run.status == "prepared", f"Prepare failed: {run.logs}")
        session = engine._active_sessions[run.run_id]
        controller = session.controller
        pub = zmq.Context.instance().socket(zmq.PUB)
        pub.setsockopt(zmq.LINGER, 0)
        pub.connect(session.pub_endpoint)
        try:
            expect(not controller.health_snapshot()["played"], "Listener cannot start a Play wave.")
            config = run.plan.worker_configs["micro"]
            client = controller.runtime_audio_stream_service.client_for("micro", port_routes=config.runtime_audio_stream_port_routes)
            topic = next(port.topic for port in config.outputs if port.port_id == 2)
            time.sleep(0.2)  # Test publisher attachment; production startup has its own ready gate.
            sequence = 0

            def command(action, **fields):
                """Exercise the actual microphone command producer then normal graph routing."""
                nonlocal sequence
                sequence += 1
                result = MicrophoneStreamBlock().handle_ui_action(node=payload["nodes"][0], action="publish_capture_command",
                    values={"action": action, "stream_id": "graph-capture", **fields})
                value = result["active_runtime_actions"][0]["value"]
                envelope = MessageEnvelope(run_id=run.run_id, source_node_id="micro", source_port_id=2,
                    payload=value, content_type="application/json", sequence=sequence)
                pub.send_multipart([topic.encode(), envelope.to_json().encode()])

            command("start")
            audio = encoded_audio()
            client.publish_port("audio_out", audio[:-100], codec="opus", sample_rate_hz=48000, channels=2, stream_id="graph-capture")
            until(api.waiting_for_final.is_set, "Audio must stream and commit without requiring microphone stop.")
            until(lambda: bool(run.output_values.get("stt:1")) and run.node_statuses.get("partial") == "success",
                  "Provisional text must travel through the graph and execute its Display before stop.")
            expect(not run.output_values.get("stt:2"), "Provisional text never belongs on final_out.")
            api.release_finals.set()
            until(lambda: bool(api.completed_items), "OpenAI must finalize segments before the capture ends.")
            until(lambda: bool(run.output_values.get("stt:2")) and run.node_statuses.get("final") == "success",
                  "A finalized segment must travel through final_out and execute its Display BEFORE microphone stop.")
            command("stop", frame_count=2, byte_count=len(audio))
            client.publish_port("audio_out", audio[-100:], codec="opus", sample_rate_hz=48000, channels=2, stream_id="graph-capture")
            until(lambda: run.output_values.get("stt:2", {}).get("value") == "final 4", f"Graph final missing: {run.logs}")
            until(lambda: run.node_statuses.get("final") == "success", "Final Display must execute on its input.")
            expect(run.output_values["stt:1"]["value"].startswith("provisoire"), "The preview port must retain its distinct content.")
            expect(not controller.health_snapshot()["played"], "STT must not start unrelated sources.")
            expect(KEY not in str(run.results) and KEY not in str(run.logs), "Graph persistence/logs cannot expose the key.")
        finally:
            pub.close(0)
            engine.stop_active_run(run.run_id)


def test_ui_and_simulation_graph():
    """FB5/FB6: serve owned assets/surfaces and complete a real centralized mini-graph."""
    block = OpenAIRealtimeSttBlock()
    node = block.build_node_payload(node_id="ui-stt")
    for render in (block.render_modal, block.render_inspector_panel, block.render_node_card):
        html = render(node=node)["html"]
        expect("{{" not in html, "All owned template fields must be resolved.")
        expect(MODEL in html, "Show the configured model.")
    applied = block.handle_ui_action(node=node, action="save_settings", values={"api_key_ref": REF, "languages": "FR,en"})
    expect(applied["node_patch"]["config"]["languages"] == "fr,en", "Normalize UI settings on the server.")
    expect("error" in block.handle_ui_action(node=node, action="save_settings", values={"api_key": KEY}), "Reject raw-key config.")
    node["config"]["prompt"] = '<script>alert("x")</script>'
    expect("<script>" not in block.render_modal(node=node)["html"], "Escape configuration HTML.")
    result = block.handle_ui_action(node=node, action="save_properties", values={
        "title": "Meeting transcription", "config": {"api_key_ref": REF, "segment_seconds": "20"}})
    expect(result["node_patch"]["title"] == "Meeting transcription", "Save title and settings in one patch.")
    expect(result["node_patch"]["config"]["segment_seconds"] == 20, "Keep server-side config validation.")
    for values in ({"title": "Must not save", "config": {"segment_seconds": 0}},
                   {"title": "Must not save", "config": {"api_key": KEY}},
                   {"title": "Must not save", "config": {"api_key_ref": "openai"}},
                   {"config": {}}, {"title": "Must not save", "config": None}):
        rejected = block.handle_ui_action(node=node, action="save_properties", values=values)
        expect("error" in rejected and "node_patch" not in rejected, "Reject the entire invalid title/settings edit.")
    with isolated_server() as server:
        # Surfaces are release assets: a bundled kind serves none of them.
        model = install_test_package(server, "openai_realtime_stt")
        key = quote(release_key(model), safe="")
        served = lambda payload, suffix: next(
            asset["path"] for asset in payload["assets"] if asset["path"].endswith(suffix))
        applied = http_json(server.base_url, f"/api/blocks/{block.kind}/ui-action", method="POST",
            payload={"node": node, "action": "save_settings", "values": {"api_key_ref": REF, "segment_seconds": "20"}})
        expect(applied["node_patch"]["config"]["segment_seconds"] == 20, "The real settings API must persist normalized values.")
        for surface in ("modal", "inspector_panel", "node_card"):
            rendered = surface_payload(server, model, {**node, "block_version": model["version"]}, surface)
            expect(bool(rendered.get("html")), f"Serve owned {surface}.")
            if surface != "node_card":
                expect(any(asset["path"].endswith(".js") for asset in rendered.get("assets", [])), "Declare surface-owned JS.")
        for asset in block.ui_assets("modal") + block.ui_assets("inspector_panel"):
            from urllib.request import urlopen
            with urlopen(server.base_url + f"/api/blocks/{block.kind}/assets/" + asset["path"]) as response:
                expect(response.status == 200 and response.read(), "Serve every owned asset.")
        graph = document()
        graph["nodes"][1]["config"]["api_key_ref"] = ""
        project = create_project_api(server, title="STT simulation", document=graph)
        graph_id = project["project"].get("graph_id") or project["project"]["project_id"]
        run = create_run_api(server, graph, project_id=graph_id, runtime_mode="centralized")
        finished = wait_for_run_terminal(server, run["run_id"])
        expect(finished["status"] == "success", f"Simulation must complete without key or audio: {finished}")
        expect(not finished.get("output_values", {}).get("stt:2"), "No fabricated STT output in simulation.")
        expect(not finished.get("output_values", {}).get("stt:1"), "Simulation must not fabricate provisional text either.")


def test_settings_javascript():
    """FB6: cover atomic edits, dirty reversion, validation, read-only, races and cleanup."""
    script = r'''
const assert = require("assert");
const url = require("url");
class Control {
  constructor(value, key) { this.value = value; this.dataset = { sttSetting: key }; this.handlers = new Map(); this.disabled = true; }
  addEventListener(name, fn) { this.handlers.set(name, fn); }
  removeEventListener(name) { this.handlers.delete(name); }
  fire(name) { return this.handlers.get(name)?.(); }
  checkValidity() { return this.valid !== false; }
  closest() { return this.disclosure; }
  reportValidity() { this.reported = true; }
}
const ref = new Control("secret://workspace/test", "api_key_ref");
const duration = new Control("15", "segment_seconds");
const button = new Control();
const feedback = new Control();
const title = new Control("STT");
const root = {
  querySelectorAll: () => [ref, duration],
  querySelector: (selector) => ({ "[data-stt-apply]": button, "[data-stt-feedback]": feedback, "[data-stt-title]": title })[selector],
};
(async () => {
  // Release module: it is imported by URL instead of being evaluated in a global scope.
  const surface = await import(url.pathToFileURL(process.argv[1]).href);
  let resolve;
  let calls = [];
  let readOnly = false;
  const api = { isReadOnly: () => readOnly, applyAction: (action, values) => { calls.push({ action, values }); return new Promise((done) => { resolve = done; }); } };
  const dispose = surface.mountSettings(root, api);
  assert(button.disabled);
  title.value = "Meeting";
  title.fire("input");
  assert(!button.disabled, "Title uses the same Apply button");
  title.value = "STT";
  title.fire("input");
  assert(button.disabled, "Reverting edits clears dirty state");
  duration.value = "20";
  duration.fire("input");
  assert(!button.disabled);
  const pending = button.fire("click");
  assert(button.disabled);
  duration.value = "25";
  duration.fire("change");
  resolve({ node_patch: {} });
  await pending;
  assert(!button.disabled, "Do not lose changes made during an in-flight apply");
  assert.equal(calls[0].action, "save_properties");
  assert.equal(calls[0].values.config.segment_seconds, "20");
  assert.equal(calls[0].values.title, "STT");
  let next = button.fire("click");
  resolve({ error: "Setting refused" });
  await next;
  assert(!button.disabled);
  assert.equal(feedback.textContent, "Setting refused");
  assert.equal(feedback.dataset.error, "true");
  next = button.fire("click");
  resolve({ node_patch: {} });
  await next;
  assert(button.disabled);
  assert.equal(calls[2].values.config.segment_seconds, "25");
  duration.value = "30";
  duration.fire("input");
  duration.valid = false;
  duration.disclosure = { open: false };
  await button.fire("click");
  assert(duration.disclosure.open && duration.reported, "Reveal invalid collapsed advanced fields");
  assert.equal(calls.length, 3);
  duration.valid = true;
  readOnly = true;
  duration.fire("input");
  assert(button.disabled);
  await button.fire("click");
  assert.equal(calls.length, 3, "Read-only surfaces cannot apply");
  readOnly = false;
  dispose();
  assert.equal(button.handlers.size, 0);
  assert.equal(ref.handlers.size, 0);
  assert.equal(title.handlers.size, 0);
  const unmount = surface.mountSettings(root, api);
  title.value = "Updated inspector";
  title.fire("input");
  next = button.fire("click");
  assert.equal(calls[3].action, "save_properties");
  assert.equal(calls[3].values.title, "Updated inspector");
  unmount();
  const oldFeedback = feedback.textContent;
  resolve({ node_patch: {} });
  await next;
  assert.equal(feedback.textContent, oldFeedback, "No feedback on an unmounted surface");
})().catch((error) => { console.error(error); process.exitCode = 1; });
'''
    # Node only loads an ES module with the .js extension inside a package scope;
    # a throwaway .mjs copy avoids inventing one in the block sources.
    source = ROOT / "blocs/openai_realtime_stt/assets/js/common.js"
    with tempfile.TemporaryDirectory() as directory:
        module = Path(directory) / "common.mjs"
        module.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        subprocess.run(["node", "-e", script, str(module)], check=True, timeout=10)


def test_properties_structure():
    """FB6: one Apply, unique accessible labels/help and initially collapsed advanced fields."""
    class Elements(HTMLParser):
        """Collect tags and attributes from rendered properties for accessibility assertions."""

        def __init__(self, html):
            """Parse the rendered surface without adding a third-party test dependency."""
            super().__init__()
            self.elements = []
            self.feed(html)

        def handle_starttag(self, tag, attrs):
            """Record a start tag's attributes, retaining boolean markers."""
            self.elements.append((tag, dict(attrs)))

    block = OpenAIRealtimeSttBlock()
    node = block.build_node_payload(node_id="properties")
    surfaces = [block.render_modal(node=node)["html"], block.render_inspector_panel(node=node)["html"]]
    all_ids = []
    for html in surfaces:
        elements = Elements(html).elements
        expect(sum("data-stt-apply" in attrs for _, attrs in elements) == 1, "Only one properties Apply.")
        expect(not any("data-block-apply" in attrs for _, attrs in elements), "No competing generic title Apply.")
        settings = [attrs for _, attrs in elements if "data-stt-setting" in attrs]
        expect({attrs["data-stt-setting"] for attrs in settings} == set(DEFAULTS) - {"model"}, "All editable settings remain available.")
        labels = {attrs["for"] for tag, attrs in elements if tag == "label" and "for" in attrs}
        ids = [attrs["id"] for _, attrs in elements if "id" in attrs]
        expect(all(attrs["id"] in labels for attrs in settings), "Every setting has an accessible label.")
        expect(all(attrs["aria-describedby"] in ids for attrs in settings if "aria-describedby" in attrs), "Help targets exist.")
        expect(all("open" not in attrs for tag, attrs in elements if tag == "details"), "Optional sections start collapsed without runtime errors.")
        all_ids.extend(ids)
    expect(len(all_ids) == len(set(all_ids)), "Modal and inspector may coexist without duplicate IDs.")
    error_html = block.render_modal(node=node, payload={"runtime": {"error": "Test failure"}})["html"]
    expect('stt-diagnostics" open' in error_html and "Test failure" in error_html, "Reveal runtime diagnostics on error.")


def test_properties_browser(page, server, _blocking_errors):
    """FB6: exercise real framework mounting/API, responsive layout, keyboard and unified apply."""
    block = OpenAIRealtimeSttBlock()
    node = block.build_node_payload(node_id="properties-browser", config_overrides={"api_key_ref": REF})
    # Use an isolated UI host: real framework styles, asset loader and action bridge, no user graph.
    page.goto(server.base_url + "/api/health")
    page.set_content('<div id="blockUiModalBackdrop" class="modal-backdrop hidden"><div id="blockUiModalMount"></div></div><div id="inspectorTestMount" style="width:340px;height:700px;overflow:auto;padding:12px"></div>')
    page.add_style_tag(path=str(ROOT / "frontend/app.css"))
    page.add_script_tag(path=str(ROOT / "frontend/block_ui.js"))
    page.evaluate("""async node => {
      window.sttTestNode = node;
      window.sttTestPatches = [];
      window.sttTestOptions = {
        kind: 'openai_realtime_stt', node,
        getNode: () => window.sttTestNode,
        onApplyPatch: (_id, patch) => { window.sttTestPatches.push(patch); Object.assign(window.sttTestNode, patch); },
      };
      await CWBlockUi.openBlockModal(window.sttTestOptions);
    }""", node)
    modal = page.locator("[data-openai-realtime-stt-modal-root]")
    apply = modal.locator("[data-stt-apply]")
    expect(apply.count() == 1 and apply.is_disabled(), "Exactly one initially disabled Apply in the real modal.")
    expect(modal.locator('[data-stt-setting="api_key_ref"]').is_visible(), "Reference visible immediately.")
    top = modal.locator('[data-stt-setting="api_key_ref"]').bounding_box()
    panel = modal.bounding_box()
    expect(top["width"] > panel["width"] * 0.8 and top["y"] - panel["y"] < 300, "No empty left column before properties.")
    widths = modal.locator(".stt-section").evaluate_all("elements => elements.map(el => el.getBoundingClientRect().width)")
    expect(abs(widths[0] - widths[1]) < 1, "All sections use the same available width.")
    expect(not modal.locator('[data-stt-setting="drain_timeout_sec"]').is_visible(), "Advanced options collapsed initially.")
    modal.locator("[data-stt-title]").fill("Meeting transcription")
    modal.locator('[data-stt-setting="segment_seconds"]').fill("20")
    apply.click()
    page.wait_for_function("window.sttTestPatches.length === 1")
    patch = page.evaluate("window.sttTestPatches[0]")
    expect(patch["title"] == "Meeting transcription" and patch["config"]["segment_seconds"] == 20, f"One API action applies title and config: {patch}")
    page.wait_for_function("document.querySelector('[data-stt-apply]').disabled")
    modal.locator(".stt-advanced > summary").focus()
    page.keyboard.press("Enter")
    expect(modal.locator('[data-stt-setting="drain_timeout_sec"]').is_visible(), "Keyboard opens advanced settings.")
    modal.locator('[data-stt-setting="drain_timeout_sec"]').fill("0")
    modal.locator(".stt-advanced > summary").click()
    apply.click()
    expect(modal.locator(".stt-advanced").evaluate("el => el.open"), "Invalid hidden setting is revealed.")
    expect(page.evaluate("window.sttTestPatches.length") == 1, "Invalid form does not submit.")
    modal.locator('[data-stt-setting="drain_timeout_sec"]').fill("5")
    modal.locator(".stt-advanced > summary").click()
    screenshots = Path(tempfile.mkdtemp(prefix="stt-properties-")) if "--screenshots" in sys.argv else None
    for width, height in ((1440, 900), (1100, 886), (390, 780), (320, 640)):
        page.set_viewport_size({"width": width, "height": height})
        modal.locator(".stt-body").evaluate("el => { el.scrollTop = el.scrollHeight; }")
        bounds = apply.bounding_box()
        expect(bounds and 0 <= bounds["y"] and bounds["y"] + bounds["height"] <= height, "Apply stays on screen after scrolling.")
        expect(modal.evaluate("el => el.scrollWidth <= el.clientWidth + 1"), "Modal has no horizontal overflow.")
        expect(modal.locator(".stt-body").evaluate("el => el.scrollWidth <= el.clientWidth + 1"), "Fields fit the narrow body.")
        modal.locator(".stt-body").evaluate("el => { el.scrollTop = 0; }")
        if screenshots:
            page.screenshot(path=str(screenshots / f"modal-{width}.png"))
    page.evaluate("CWBlockUi.closeBlockModal()")
    page.set_viewport_size({"width": 1100, "height": 886})
    page.evaluate("""async () => CWBlockUi.renderInspectorPanel({ ...window.sttTestOptions,
      node: window.sttTestNode, mount: document.querySelector('#inspectorTestMount') })""")
    inspector = page.locator("[data-openai-realtime-stt-inspector-root]")
    expect(inspector.locator('[data-stt-setting="api_key_ref"]').is_visible(), "Inspector shows the same grouped settings.")
    inspector.locator("[data-stt-title]").fill("Inspector title")
    inspector.locator("[data-stt-apply]").click()
    page.wait_for_function("window.sttTestPatches.length === 2")
    expect(page.evaluate("window.sttTestNode.title") == "Inspector title", "Inspector also saves the title through the single action.")
    if screenshots:
        page.screenshot(path=str(screenshots / "inspector.png"))
        print(f"[ui screenshots] {screenshots}")
    overflow = inspector.evaluate("el => [...el.querySelectorAll('*')].filter(item => item.getBoundingClientRect().right > el.getBoundingClientRect().right + 1).map(item => [item.tagName, item.className, item.getBoundingClientRect().width])")
    expect(inspector.evaluate("el => el.scrollWidth <= el.clientWidth + 1"), f"Narrow inspector has no horizontal overflow: {overflow}")


def main():
    """Execute the complete block-local verification slice without real OpenAI calls."""
    test_contract_secrets_and_simulation()
    test_audio_bounds_and_connection_contract()
    test_final_before_stop()
    test_output_bounds_and_reconciliation()
    test_streaming_containers_and_reuse()
    test_final_waits_for_last_completion()
    test_errors_empty_and_shutdown()
    test_graph_active()
    test_ui_and_simulation_graph()
    test_settings_javascript()
    test_properties_structure()
    print("[ok] F5.46_openai_realtime_stt_block")


if __name__ == "__main__":
    if "--ui" in sys.argv:
        from ui_smoke_common import run_playwright_smoke
        run_playwright_smoke("F5.46 STT properties", test_properties_browser)
    else:
        main()
