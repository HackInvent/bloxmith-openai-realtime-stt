"""Stream graph-wired audio sources to OpenAI; all session policy is block-owned."""

from __future__ import annotations

import asyncio
import base64
from collections import deque
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from html import escape
import importlib.util
import json
import math
import os
import re
import shutil
import subprocess
import time
from typing import Any

from bloxsmith_app.block_api import (
    BlockDefinition, BlockRuntimeContext, BlockRuntimeListenerContext,
    BlockRuntimeOutput, BlockRuntimePreparation, BlockRuntimePreparationContext,
    BlockRuntimeResult, RuntimeAudioFrame, TEXT_PLAIN,
    render_inspector_template, render_node_card_template,
)

from .external_turns import ExternalPcmTurns, ExternalTurnError


MODEL = "gpt-live-transcribe"
REALTIME_URL = "wss://api.openai.com/v1/realtime?intent=transcription"
MAX_BUFFER = 8 * 1024 * 1024
MAX_TEXT = 32_000
PCM_BYTES_PER_SECOND = 48_000  # Mono, signed little-endian PCM16 at 24 kHz.
SECRET_REF = re.compile(r"secret://(?:workspace/[A-Za-z0-9_.-]{1,80}|project/[A-Za-z0-9_.-]{1,80}/[A-Za-z0-9_.-]{1,80})")
DEFAULTS = {
    "api_key_ref": "", "model": MODEL, "languages": "fr", "prompt": "", "delay": "low",
    "segmentation": "duration", "segment_seconds": 15, "drain_timeout_sec": 5, "final_timeout_sec": 20,
    "connect_timeout_sec": 10, "max_duration_sec": 3600,
}
BOUNDS = {
    "segment_seconds": (1, 60), "drain_timeout_sec": (0.25, 30),
    "final_timeout_sec": (1, 60), "connect_timeout_sec": (1, 30),
    "max_duration_sec": (1, 3600),
}


class RealtimeSttError(ValueError):
    """A safe, block-authored diagnostic with no key, raw API event or audio bytes."""


def _config(raw: Mapping | None) -> dict:
    """Validate startup/UI settings without resolving secrets or performing IO."""
    source = raw or {}
    if not isinstance(source, Mapping):
        raise RealtimeSttError("The STT configuration must be an object.")
    if any(key in source for key in ("api_key", "token", "authorization")):
        raise RealtimeSttError("Use a wallet reference, never a clear-text key in the block.")
    config = {key: source.get(key, default) for key, default in DEFAULTS.items()}
    for key in ("api_key_ref", "model", "languages", "prompt", "delay", "segmentation"):
        if not isinstance(config[key], str):
            raise RealtimeSttError(f"Invalid text setting: {key}.")
        config[key] = config[key].strip()
    if config["api_key_ref"] and not SECRET_REF.fullmatch(config["api_key_ref"]):
        raise RealtimeSttError("Invalid reference: secret://workspace/name or secret://project/id/name expected.")
    if config["model"] != MODEL:
        raise RealtimeSttError(f"This block uses the {MODEL} model.")
    languages = [item.strip().lower() for item in config["languages"].split(",") if item.strip()]
    if len(languages) > 8 or any(not re.fullmatch(r"[a-z]{2,3}(?:-[a-z]{2})?", item) for item in languages):
        raise RealtimeSttError("Invalid languages: at most 8 comma-separated codes.")
    config["languages"] = ",".join(dict.fromkeys(languages))
    if len(config["prompt"]) > 1024:
        raise RealtimeSttError("The context is limited to 1,024 characters.")
    if config["delay"] not in {"minimal", "low", "medium", "high", "xhigh"}:
        raise RealtimeSttError("Invalid model delay.")
    if config["segmentation"] not in {"duration", "external"}:
        raise RealtimeSttError("Invalid segmentation: duration or external expected.")
    for key, (minimum, maximum) in BOUNDS.items():
        try:
            value = float(config[key])
        except (ValueError, TypeError, OverflowError):
            raise RealtimeSttError(f"Invalid number: {key}.") from None
        integer = key in {"segment_seconds", "max_duration_sec"}
        if isinstance(config[key], bool) or not math.isfinite(value) or not minimum <= value <= maximum or (integer and not value.is_integer()):
            raise RealtimeSttError(f"{key} must be {'an integer ' if integer else ''}between {minimum} and {maximum}.")
        config[key] = int(value) if integer else value
    return config


def _command(raw: Any) -> dict:
    """Parse source start/stop and external begin/commit boundaries on the decoded audio clock."""
    if isinstance(raw, str):
        if len(raw) > 4096:
            raise RealtimeSttError("Audio command too large.")
        try:
            raw = json.loads(raw)
        except (ValueError, RecursionError):
            raise RealtimeSttError("command_in expects a start/stop/begin/commit JSON object.") from None
    if not isinstance(raw, Mapping) or raw.get("action") not in ("start", "stop", "begin", "commit"):
        raise RealtimeSttError("command_in expects a start, stop, begin or commit action.")
    stream_id = raw.get("stream_id")
    if not isinstance(stream_id, str) or not stream_id.strip() or len(stream_id) > 128:
        raise RealtimeSttError("The command must provide a non-empty stream_id (128 characters maximum).")
    result = {"action": raw["action"], "stream_id": stream_id}
    if result["action"] in {"begin", "commit"}:
        key = "audio_start_ms" if result["action"] == "begin" else "audio_end_ms"
        value = raw.get(key)
        if set(raw) != {"action", "stream_id", key} or type(value) is not int or not 0 <= value <= 3_600_000:
            raise RealtimeSttError(f"Invalid audio boundary: {key}, an integer from 0 to 3,600,000 ms, with no other field.")
        result[key] = value
    if result["action"] == "stop":
        for key in ("frame_count", "byte_count"):
            value = raw.get(key)
            if type(value) is not int or not 0 <= value <= 9_007_199_254_740_991:
                raise RealtimeSttError(f"Invalid final counter: {key}.")
            result[key] = value
        if type(raw.get("aborted", False)) is not bool:
            raise RealtimeSttError("aborted must be a boolean.")
        result["aborted"] = raw.get("aborted", False)
    return result


def _secret(context: Any, config: dict) -> str:
    """Resolve only the configured wallet reference; never retain it in runtime state."""
    if not config["api_key_ref"]:
        raise RealtimeSttError("Missing OpenAI secret reference. Configure the block in Settings → Secrets.")
    resolver = context.services.get("resolve_secret")
    if not callable(resolver):
        raise RealtimeSttError("Secret resolver unavailable in this runtime.")
    try:
        value = resolver(config["api_key_ref"])
    except Exception:
        raise RealtimeSttError("OpenAI key unreachable: unlock the wallet and check the reference.") from None
    if not isinstance(value, str) or not value.strip() or any(char in value for char in "\r\n"):
        raise RealtimeSttError("The OpenAI secret value is empty or invalid.")
    return value.strip()


def _commands(raw: Any) -> list[dict]:
    """Validate one JSON command or an ordered batch of at most 64 before any forwarding."""
    if isinstance(raw, str):
        if len(raw.encode("utf-8")) > 65536:
            raise RealtimeSttError("Audio command batch too large (64 KiB maximum).")
        try:
            raw = json.loads(raw)
        except (ValueError, RecursionError):
            raise RealtimeSttError("command_in expects one JSON command or an array of commands, without concatenation.") from None
    commands = raw if isinstance(raw, list) else [raw]
    if not 1 <= len(commands) <= 64:
        raise RealtimeSttError("A batch must contain 1 to 64 audio commands.")
    return [_command(command) for command in commands]


def _failure(error: Exception) -> BlockRuntimeResult:
    """Expose only known-safe diagnostics; network/decoder exceptions may contain secrets."""
    message = str(error) if isinstance(error, (RealtimeSttError, ExternalTurnError)) else "Transcription interrupted: connection, transport or audio decoding error."
    return BlockRuntimeResult(status="failed", error=message, last_message=message,
                              metadata={"openai_realtime_stt": {"state": "error"}})


def _boundary_warning(context: Any, stream_id: str, message: str) -> None:
    """Report a rejected/expired speech command without closing otherwise healthy capture sessions."""
    context.emit_result(BlockRuntimeResult(last_message=message, logs=[f"[stt-boundary-warning] {message}"],
        metadata={"openai_realtime_stt": {"state": "warning", "stream_id": stream_id}}))


@dataclass
class _Capture:
    """One bounded encoded stream, correlated exclusively through explicit stream_id."""

    stream_id: str
    started: float = field(default_factory=time.monotonic)
    frames: deque = field(default_factory=deque)
    queued_bytes: int = 0
    frame_count: int = 0
    byte_count: int = 0
    last_sequence: int | None = None
    profile: tuple | None = None
    stop: dict | None = None
    stopped_at: float | None = None
    boundaries: list[tuple[int, str, float]] = field(default_factory=list)
    latest_boundary: dict[str, int] = field(default_factory=dict)

    def request_boundary(self, command: dict, config: dict, *, arrived: float | None = None) -> None:
        """Queue up to 64 monotonic speech boundaries; duplicate/older offsets are harmless."""
        if config["segmentation"] != "external":
            raise RealtimeSttError("begin/commit command ignored: choose external segmentation in the STT properties.")
        action = command["action"]
        offset = command["audio_start_ms" if action == "begin" else "audio_end_ms"]
        if offset > config["max_duration_sec"] * 1000:
            raise RealtimeSttError("Speech boundary beyond the maximum capture duration; command ignored.")
        if offset <= self.latest_boundary.get(action, -1):
            return
        if len(self.boundaries) >= 64:
            raise RealtimeSttError("At most 64 pending speech boundaries; command ignored.")
        self.boundaries.append((offset, action, time.monotonic() if arrived is None else arrived))
        self.latest_boundary[action] = offset

    def feed(self, frame: RuntimeAudioFrame) -> None:
        """Validate continuity before admitting a frame; reject overflow instead of truncating."""
        profile = (frame.codec, frame.sample_rate_hz, frame.channels, frame.source_id)
        if frame.stream_id != self.stream_id or frame.codec not in {"opus", "aac"} or frame.channels not in {1, 2}:
            raise RealtimeSttError("Invalid audio stream format or identity.")
        if self.profile is not None and self.profile != profile:
            raise RealtimeSttError("The format or the source changed during the capture.")
        if self.last_sequence is not None and frame.sequence != self.last_sequence + 1:
            raise RealtimeSttError("Incomplete stream: audio frames lost or reordered.")
        if len(self.frames) >= 128 or self.queued_bytes + len(frame.payload) > MAX_BUFFER:
            raise RealtimeSttError("Transcription too slow: audio buffer saturated.")
        self.profile, self.last_sequence = profile, frame.sequence
        self.frame_count += 1
        self.byte_count += len(frame.payload)
        self.queued_bytes += len(frame.payload)
        self.frames.append(frame.payload)
        self.complete()

    def finish(self, command: dict) -> None:
        """Record immutable stop totals while allowing late audio to drain independently."""
        if command["aborted"]:
            raise RealtimeSttError("The source interrupted the audio stream; transcription cancelled.")
        if self.stop is not None and self.stop != command:
            raise RealtimeSttError("Conflicting stop commands for the same capture.")
        if self.stop is None:
            self.stop, self.stopped_at = command, time.monotonic()
        self.complete()

    def complete(self) -> bool:
        """Return true only at exact stop totals, rejecting any data beyond those totals."""
        if self.stop is None:
            return False
        if self.frame_count > self.stop["frame_count"] or self.byte_count > self.stop["byte_count"]:
            raise RealtimeSttError("The received frames exceed the counters announced by stop.")
        return self.frame_count == self.stop["frame_count"] and self.byte_count == self.stop["byte_count"]


class _Decoder:
    """Nonblocking persistent FFmpeg pipe; no per-frame files, decoder threads or network protocols."""

    def __init__(self, header: bytes):
        """Choose a container from its first bytes, then decode only stdin to PCM stdout."""
        if header.startswith(b"\x1aE\xdf\xa3"):
            container = "matroska"
        elif header.startswith(b"OggS"):
            container = "ogg"
        elif header[4:8] == b"ftyp":
            container = "mov"
        else:
            raise RealtimeSttError("Missing audio header: WebM, Ogg or fragmented MP4 expected at the start of the capture.")
        executable = shutil.which("ffmpeg")
        if not executable:
            raise RealtimeSttError("FFmpeg is required on the server to decode the audio stream.")
        self.process = subprocess.Popen([
            executable, "-hide_banner", "-loglevel", "error", "-nostdin",
            "-probesize", "32", "-analyzeduration", "0", "-protocol_whitelist", "pipe",
            "-f", container, "-i", "pipe:0", "-map", "0:a:0", "-vn", "-sn", "-dn",
            "-ac", "1", "-ar", "24000", "-acodec", "pcm_s16le", "-f", "s16le",
            "-flush_packets", "1", "pipe:1",
        ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        for handle in (self.process.stdin, self.process.stdout, self.process.stderr):
            os.set_blocking(handle.fileno(), False)

    def write(self, data: bytes | bytearray) -> int:
        """Write available pipe capacity, returning zero under temporary backpressure."""
        try:
            return os.write(self.process.stdin.fileno(), data)
        except BlockingIOError:
            return 0

    def read(self) -> bytes | None:
        """Read a bounded PCM chunk; None means pending, empty bytes means EOF."""
        # Drain stderr too: a noisy decoder must never deadlock or expose raw input.
        with suppress(BlockingIOError):
            os.read(self.process.stderr.fileno(), 4096)
        try:
            return os.read(self.process.stdout.fileno(), 9600)
        except BlockingIOError:
            return None

    def close(self) -> None:
        """Reap the owned decoder promptly, including cancellation and malformed streams."""
        if self.process.poll() is None:
            self.process.kill()
        with suppress(subprocess.TimeoutExpired):
            self.process.wait(timeout=0.15)
        for handle in (self.process.stdin, self.process.stdout, self.process.stderr):
            handle.close()


class _Transcripts:
    """Stream provisional text and publish completed segments in audio order, independently of stop."""

    def __init__(self, context: BlockRuntimeListenerContext, capture: _Capture):
        """Keep all transcript buffers local to one capture, never on the block definition."""
        self.context, self.capture = context, capture
        self.items: dict[str, dict] = {}
        self.order: deque[str] = deque()
        self.retired: deque[str] = deque(maxlen=128)
        self.commits = self.acknowledged = self.finalized = 0
        self.ready = asyncio.Event()
        self.changed = asyncio.Event()
        self.last_partial_at = 0.0

    def flush_partial(self) -> None:
        """Publish current-segment replacement snapshots at most five times per second."""
        if time.monotonic() - self.last_partial_at < 0.2:
            return
        for item_id, item in self.items.items():
            if item["dirty"] and item["final"] is None:
                self.context.emit_result(BlockRuntimeResult(
                    outputs=[BlockRuntimeOutput(port_id=1, port_name="partial_out",
                                                value=item["text"], content_type=TEXT_PLAIN)],
                    last_message="Transcription en cours…",
                    metadata={"openai_realtime_stt": {"state": "transcribing", "stream_id": self.capture.stream_id,
                        "item_id": item_id, "is_final": False, "frames_received": self.capture.frame_count}},
                ))
                item["dirty"] = False
                self.last_partial_at = time.monotonic()
                break

    def emit_final(self, text: str, item_id: str) -> None:
        """Publish one confirmed segment on final_out, preserving the API text without a stop gate."""
        self.context.emit_result(BlockRuntimeResult(
            outputs=[BlockRuntimeOutput(port_id=2, port_name="final_out", value=text, content_type=TEXT_PLAIN)],
            last_message="Segment transcrit.",
            metadata={"openai_realtime_stt": {"state": "transcribing", "stream_id": self.capture.stream_id,
                "item_id": item_id, "is_final": True, "frames_received": self.capture.frame_count}},
        ))

    def event(self, event: dict) -> None:
        """Route deltas to previews and confirmed finals to final_out in commit order, without stop."""
        kind = event.get("type")
        if kind == "session.updated":
            self.ready.set()
            return
        if kind == "error" or kind == "conversation.item.input_audio_transcription.failed":
            code = (event.get("error") or {}).get("code")
            if code in {"invalid_api_key", "authentication_error"}:
                raise RealtimeSttError("OpenAI refuses the API key; check the secret and its permissions.")
            if code in {"rate_limit_exceeded", "insufficient_quota"}:
                raise RealtimeSttError("OpenAI quota or limit reached; check your API account.")
            raise RealtimeSttError("OpenAI refused the transcription. Check the model, the languages and the API account permissions.")
        if kind not in {"input_audio_buffer.committed", "conversation.item.input_audio_transcription.delta",
                        "conversation.item.input_audio_transcription.completed"}:
            return
        item_id = event.get("item_id")
        if not isinstance(item_id, str) or not 1 <= len(item_id) <= 128:
            raise RealtimeSttError("OpenAI returned an invalid segment identifier.")
        if item_id in self.retired:
            return
        if item_id not in self.items:
            if len(self.items) >= 64:
                raise RealtimeSttError("Too many segments are waiting for a final transcription.")
            self.items[item_id] = {"text": "", "final": None, "dirty": False}
        item = self.items[item_id]
        if kind == "input_audio_buffer.committed":
            if item_id not in self.order:
                self.order.append(item_id)
                self.acknowledged += 1
        else:
            text = event.get("transcript" if kind.endswith(".completed") else "delta")
            if not isinstance(text, str):
                raise RealtimeSttError("OpenAI returned an invalid transcription text.")
            if len(text) > MAX_TEXT or (kind.endswith(".delta") and len(item["text"]) + len(text) > MAX_TEXT):
                raise RealtimeSttError("Transcription too long: 32,000 characters maximum per segment.")
            if kind.endswith(".completed"):
                item["final"] = text
            elif item["final"] is None:
                item["text"] += text
                item["dirty"] = True
        # Ack order is audio order because the block is the sole commit producer.
        while self.order and self.items[self.order[0]]["final"] is not None:
            head = self.order.popleft()
            completed = self.items.pop(head)
            self.emit_final(completed["final"], head)
            self.retired.append(head)
            self.finalized += 1
        self.changed.set()
        self.flush_partial()


async def _guard(work, reader: asyncio.Task):
    """Await a session operation while failing promptly if the WebSocket reader dies."""
    task = asyncio.ensure_future(work)
    try:
        await asyncio.wait((task, reader), return_when=asyncio.FIRST_COMPLETED)
        if reader.done():
            await reader
            raise RealtimeSttError("OpenAI connection closed before the end of the transcription.")
        return await task
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


# FB1 - Fixed explicit audio/command inputs, listener from Run and no-op simulation.
# FB2 - Bounded source start/stop and external speech begin/commit correlation, late-frame drain and reuse.
# FB3 - Continuous container decoding to PCM, cancellable server-side OpenAI WebSocket.
# FB4 - Live segment previews on partial_out; confirmed segment text on final_out without waiting for stop.
# FB5 - Wallet-only authentication, bounded settings and non-sensitive failures.
# FB6 - Autonomous configuration surfaces, discovery, documentation and two-mode integration.
class OpenAIRealtimeSttBlock(BlockDefinition):
    """An autonomous live transcription consumer; no behavior is delegated to the framework."""

    kind = "openai_realtime_stt"

    def prepare_runtime(self, context: BlockRuntimePreparationContext) -> BlockRuntimePreparation:
        """Validate static settings/ports and declare listening without IO or secrets."""
        _config(context.config)
        self._ports(context)
        return BlockRuntimePreparation(listen_on_run=context.runtime_mode == "zeromq_active")

    def initialize_runtime(self, context: BlockRuntimeContext) -> BlockRuntimeResult:
        """Fail early on missing active credentials/tools, without connecting to OpenAI."""
        try:
            config = _config(context.config)
            if context.runtime_mode == "zeromq_active":
                _secret(context, config)
                if not shutil.which("ffmpeg"):
                    raise RealtimeSttError("FFmpeg is required on the server to decode the microphone stream.")
                if importlib.util.find_spec("websockets") is None:
                    raise RealtimeSttError("Missing dependency: install the requirements.txt of the OpenAI Realtime STT block.")
            return BlockRuntimeResult(last_message="STT listening ready at Run.")
        except Exception as exc:
            return _failure(exc)

    def execute_runtime(self, context: BlockRuntimeContext) -> BlockRuntimeResult:
        """Forward distinct delivered commands, never concatenating JSON from a grouped input wave."""
        try:
            _config(context.config)
            self._ports(context)
            if context.runtime_mode != "zeromq_active":
                return BlockRuntimeResult(status="skipped", last_message="Realtime STT is only available in Active Runtime.",
                    metadata={"openai_realtime_stt": {"state": "simulation"}})
            delivered = [event.value for event in context.input_events if event.input_port_id == 2]
            values = delivered if context.input_events else [context.input_value("command_in")]
            values = [value for value in values if value is not None and value != ""]
            if not values:
                return BlockRuntimeResult(status="skipped", last_message="Listening on command_in.")
            commands = [command for value in values for command in _commands(value)]
            if len(commands) > 64:
                raise RealtimeSttError("Maximum 64 commandes par activation STT.")
            sender = context.services.get("runtime_listener")
            if sender is None:
                raise RealtimeSttError("Listener STT indisponible : Stop puis Run.")
            sender.send(commands[0] if len(commands) == 1 else commands)
            return BlockRuntimeResult(last_message=f"Commande {commands[-1]['action']} transmise au STT.")
        except Exception as exc:
            return _failure(exc)

    def listen_runtime(self, context: BlockRuntimeListenerContext) -> None:
        """Host cancellable IO on the framework-owned listener thread, with local state only."""
        asyncio.run(self._listen(context, _config(context.config)))

    async def _listen(self, context: BlockRuntimeListenerContext, config: dict) -> None:
        """Poll ready audio/command queues independently while supervising session tasks."""
        sessions: dict[str, tuple[_Capture, asyncio.Task]] = {}
        retired: deque[str] = deque(maxlen=128)
        pending: deque[tuple[float, RuntimeAudioFrame]] = deque()
        pending_boundaries: deque[tuple[float, dict]] = deque()
        command_batch: deque[dict] = deque()
        pending_bytes = 0
        try:
            audio = context.services.get("runtime_audio_streams")
            if audio is None or not audio.available:
                raise RealtimeSttError("Wire audio_in to an Opus source (Microphone Stream or OpenAI TTS Stream), and command_in to its command_out output.")
            while not context.stop_requested():
                now = time.monotonic()
                while pending and now - pending[0][0] > 5:
                    pending_bytes -= len(pending.popleft()[1].payload)
                while pending_boundaries and now - pending_boundaries[0][0] > 5:
                    _, expired = pending_boundaries.popleft()
                    _boundary_warning(context, expired["stream_id"], "Speech command expired before start (5 s); no stream created.")
                # Completed captures are retired before admitting a new command.
                for stream_id, (capture, task) in list(sessions.items()):
                    if task.done():
                        task.result()
                        del sessions[stream_id]
                        retired.append(stream_id)
                    elif now - capture.started > config["max_duration_sec"]:
                        raise RealtimeSttError("Maximum capture duration reached; stop, then restart the audio source.")
                    elif capture.stop and not capture.complete() and now - capture.stopped_at > config["drain_timeout_sec"]:
                        raise RealtimeSttError("Incomplete stream after stop: the last audio frames were not received.")
                if not command_batch:
                    incoming = context.receive_command(timeout_sec=0)
                    if incoming is not None:
                        command_batch.extend(_commands(incoming.payload))
                if command_batch:
                    command = command_batch.popleft()
                    stream_id = command["stream_id"]
                    if stream_id not in retired:
                        if command["action"] == "start" and stream_id not in sessions:
                            if len(sessions) >= 4:
                                raise RealtimeSttError("At most four simultaneous or finalizing captures.")
                            capture = _Capture(stream_id)
                            sessions[stream_id] = (capture, asyncio.create_task(self._session(context, config, capture)))
                            remaining = deque()
                            for arrived, frame in pending:
                                if frame.stream_id == stream_id:
                                    capture.feed(frame)
                                    pending_bytes -= len(frame.payload)
                                else:
                                    remaining.append((arrived, frame))
                            pending = remaining
                            retained = deque()
                            for arrived, boundary in pending_boundaries:
                                if boundary["stream_id"] == stream_id:
                                    try:
                                        capture.request_boundary(boundary, config, arrived=now)
                                    except RealtimeSttError as exc:
                                        _boundary_warning(context, stream_id, str(exc))
                                else:
                                    retained.append((arrived, boundary))
                            pending_boundaries = retained
                        elif command["action"] == "stop":
                            if stream_id not in sessions:
                                raise RealtimeSttError("Stop received without a start for this capture.")
                            sessions[stream_id][0].finish(command)
                        elif command["action"] in {"begin", "commit"}:
                            try:
                                if config["segmentation"] != "external":
                                    raise RealtimeSttError("begin/commit command ignored: enable the external segmentation mode.")
                                if stream_id in sessions:
                                    sessions[stream_id][0].request_boundary(command, config)
                                elif any(queued == command for _, queued in pending_boundaries):
                                    pass
                                elif len(pending_boundaries) >= 64:
                                    raise RealtimeSttError("At most 64 speech commands before start; command ignored.")
                                else:
                                    pending_boundaries.append((now, command))
                            except RealtimeSttError as exc:
                                _boundary_warning(context, stream_id, str(exc))
                # A finite batch prevents a hot audio source from starving commands/Stop.
                for _ in range(16):
                    frame = audio.receive_port("audio_in", timeout_sec=0)
                    if frame is None:
                        break
                    if frame.stream_id in sessions:
                        sessions[frame.stream_id][0].feed(frame)
                    elif frame.stream_id not in retired:
                        pending.append((now, frame))
                        pending_bytes += len(frame.payload)
                        while len(pending) > 128 or pending_bytes > MAX_BUFFER:
                            pending_bytes -= len(pending.popleft()[1].payload)
                await asyncio.sleep(0.01)
        except Exception as exc:
            if not context.stop_requested():
                context.emit_result(_failure(exc))
        finally:
            for _, task in sessions.values():
                task.cancel()
            await asyncio.gather(*(task for _, task in sessions.values()), return_exceptions=True)
        # A business failure is terminal for this worker, but only the host owns shutdown.
        while not context.stop_requested():
            await asyncio.sleep(0.02)

    async def _connect(self, api_key: str, config: dict):
        """Connect only to OpenAI over verified TLS; the key lives in a server header."""
        from websockets.asyncio.client import connect
        from websockets.exceptions import InvalidStatus

        try:
            return await connect(REALTIME_URL, additional_headers={"Authorization": f"Bearer {api_key}"},
                open_timeout=config["connect_timeout_sec"], close_timeout=0.15,
                max_size=1_048_576, max_queue=16, write_limit=32768, compression=None, proxy=None)
        except InvalidStatus as exc:
            status = exc.response.status_code
            if status in {401, 403}:
                raise RealtimeSttError("OpenAI refuses the API key or the model access; check the wallet and the account permissions.") from None
            if status == 429:
                raise RealtimeSttError("Quota or OpenAI connection limit reached.") from None
            raise RealtimeSttError("OpenAI refuses the transcription connection.") from None
        except Exception:
            raise RealtimeSttError("OpenAI connection failed: check the network and the connection timeout.") from None

    async def _session(self, context: BlockRuntimeListenerContext, config: dict, capture: _Capture) -> None:
        """Own one WebSocket until all announced bytes and final transcripts are drained."""
        transcripts = _Transcripts(context, capture)
        ws = None
        reader = None
        try:
            context.emit_result(BlockRuntimeResult(last_message="Connecting to OpenAI…",
                metadata={"openai_realtime_stt": {"state": "connecting", "stream_id": capture.stream_id}}))
            ws = await self._connect(_secret(context, config), config)
            reader = asyncio.create_task(self._read_events(ws, transcripts))
            transcription = {"model": MODEL, "delay": config["delay"]}
            if config["languages"]:
                transcription["languages"] = config["languages"].split(",")
            if config["prompt"]:
                transcription["prompt"] = config["prompt"]
            await self._send(ws, {"type": "session.update", "session": {"type": "transcription", "audio": {"input": {
                "format": {"type": "audio/pcm", "rate": 24000}, "transcription": transcription, "turn_detection": None,
            }}}})
            await _guard(asyncio.wait_for(transcripts.ready.wait(), config["connect_timeout_sec"]), reader)
            await _guard(self._stream_audio(ws, capture, transcripts, config), reader)

            async def await_finals():
                """Wait for every explicit commit, not just the most recent completion event."""
                while (transcripts.acknowledged != transcripts.commits or transcripts.finalized != transcripts.commits
                       or transcripts.items or transcripts.order):
                    transcripts.changed.clear()
                    await transcripts.changed.wait()

            await _guard(asyncio.wait_for(await_finals(), config["final_timeout_sec"]), reader)
            # Stop drains the tail and closes the session; each confirmed segment has already been published.
            context.emit_result(BlockRuntimeResult(last_message="Transcription finished." if capture.frame_count else "Empty capture: no text.",
                metadata={"openai_realtime_stt": {"state": "completed", "stream_id": capture.stream_id,
                    "segments": transcripts.finalized, "frames_received": capture.frame_count, "bytes_received": capture.byte_count}}))
        except TimeoutError:
            raise RealtimeSttError("Timeout while waiting for the OpenAI configuration, the decoding or the final results.") from None
        finally:
            if reader is not None:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
            if ws is not None:
                # Abort avoids waiting on a remote close handshake during framework Stop.
                ws.transport.abort()
                with suppress(Exception):
                    await asyncio.wait_for(ws.wait_closed(), 0.2)

    @staticmethod
    async def _send(ws: Any, event: dict) -> None:
        """Bound outbound waits so network backpressure never pins a listener indefinitely."""
        await asyncio.wait_for(ws.send(json.dumps(event, ensure_ascii=False)), 2)

    @staticmethod
    async def _read_events(ws: Any, transcripts: _Transcripts) -> None:
        """Read bounded events and flush coalesced previews even when OpenAI sends no new delta."""
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), 0.1)
            except TimeoutError:
                transcripts.flush_partial()
                continue
            event = json.loads(raw)
            if not isinstance(event, dict):
                raise RealtimeSttError("Invalid OpenAI event.")
            transcripts.event(event)

    async def _stream_audio(self, ws: Any, capture: _Capture, transcripts: _Transcripts, config: dict) -> None:
        """Pump one persistent decoder, using duration cuts or explicit offset-based speech turns."""
        decoder = None
        pending = bytearray()
        pcm = bytearray()
        segment_bytes = 0
        eof_at = None
        segment_limit = config["segment_seconds"] * PCM_BYTES_PER_SECOND

        async def commit(*, reason="duration", audio_start_ms=None, committed_audio_start_ms=None,
                         audio_end_ms=None, committed_audio_end_ms=None):
            """Close one nonempty remote turn; requested/actual cuts expose prefix and overlap handling."""
            nonlocal segment_bytes
            if not segment_bytes:
                return
            if transcripts.commits - transcripts.finalized >= 64:
                raise RealtimeSttError("Too many segments are waiting for a final transcription.")
            if segment_bytes < 4800:
                await self._send(ws, {"type": "input_audio_buffer.append",
                    "audio": base64.b64encode(bytes(4800 - segment_bytes)).decode("ascii")})
            transcripts.commits += 1
            await self._send(ws, {"type": "input_audio_buffer.commit"})
            segment_bytes = 0
            if config["segmentation"] == "external":
                transcripts.context.emit_result(BlockRuntimeResult(last_message="Speech sent for final transcription.",
                    metadata={"openai_realtime_stt": {"state": "segment_committed", "stream_id": capture.stream_id,
                        "reason": reason, "audio_start_ms": audio_start_ms,
                        "committed_audio_start_ms": committed_audio_start_ms,
                        "audio_end_ms": audio_end_ms, "committed_audio_end_ms": committed_audio_end_ms}}))

        async def append_pcm(raw: bytes):
            """Append real PCM while tracking only this remote turn's actual audio samples."""
            nonlocal segment_bytes
            await self._send(ws, {"type": "input_audio_buffer.append", "audio": base64.b64encode(raw).decode("ascii")})
            segment_bytes += len(raw)

        async def send_pcm():
            """Send aligned PCM frames, splitting exactly at explicit segment boundaries."""
            nonlocal segment_bytes
            while len(pcm) >= 2:
                count = min(len(pcm) // 2 * 2, segment_limit - segment_bytes)
                raw = bytes(pcm[:count])
                del pcm[:count]
                await append_pcm(raw)
                if segment_bytes == segment_limit:
                    await commit()

        external = ExternalPcmTurns(capture.boundaries, append_pcm, commit,
            lambda message: _boundary_warning(transcripts.context, capture.stream_id, message),
            timeout_sec=config["drain_timeout_sec"]) if config["segmentation"] == "external" else None
        try:
            while True:
                if external is not None:
                    await external.tick()
                if not pending and capture.frames:
                    chunk = capture.frames.popleft()
                    capture.queued_bytes -= len(chunk)
                    if decoder is None:
                        decoder = _Decoder(chunk)
                    pending.extend(chunk)
                if decoder is None and capture.complete():
                    return
                if decoder is not None:
                    if pending:
                        written = decoder.write(pending)
                        del pending[:written]
                    if not pending and not capture.frames and capture.complete() and eof_at is None:
                        decoder.process.stdin.close()
                        eof_at = time.monotonic()
                    chunk = decoder.read()
                    if chunk:
                        if external is not None:
                            await external.feed(chunk)
                        else:
                            pcm.extend(chunk)
                            await send_pcm()
                    elif chunk == b"":
                        if eof_at is None:
                            raise RealtimeSttError("The audio decoder stopped before the end of the stream.")
                        if decoder.process.poll() is None:
                            await asyncio.sleep(0.01)
                            continue
                        if decoder.process.returncode != 0 or pcm:
                            raise RealtimeSttError("Invalid or truncated audio: FFmpeg did not finish decoding.")
                        break
                    if eof_at is not None and time.monotonic() - eof_at > config["final_timeout_sec"]:
                        raise RealtimeSttError("The audio decoder did not finish within the expected delay.")
                await asyncio.sleep(0.002)
            if external is not None:
                await external.finish()
                return
            if segment_bytes:
                await commit()
        finally:
            if decoder is not None:
                decoder.close()

    @staticmethod
    def _ports(context: Any) -> None:
        """Protect the fixed port identity and independent audio/message readiness contract."""
        inputs, outputs = tuple(context.input_ports), tuple(context.output_ports)
        if len(inputs) != 2 or len(outputs) != 2:
            raise RealtimeSttError("The STT requires two fixed inputs and two fixed outputs; recreate the modified node.")
        for ports, expected in ((inputs, ((1, "audio_in", "audio_stream"), (2, "command_in", "message"))),
                                (outputs, ((1, "partial_out", "message"), (2, "final_out", "message")))):
            actual = {(p.id, p.name, getattr(p, "transport", "message")) for p in ports}
            if actual != set(expected):
                raise RealtimeSttError("The names and transports of the STT ports must stay unchanged.")
        by_name = {p.name: p for p in inputs}
        for name, required in (("audio_in", False), ("command_in", True)):
            port = by_name[name]
            requirement = "required_for_execution" if required else "not_required_for_execution"
            if port.required != required or port.execution_requirement != requirement or port.multiplicity != "one":
                raise RealtimeSttError("audio_in must stay optional and command_in required, each with multiplicity one.")

    def render_node_card(self, *, node: dict, payload: dict | None = None) -> dict:
        """Render a bounded status, leaving ports and execution decoration to the shell."""
        runtime = (payload or {}).get("runtime") or node.get("runtimeUi") or {}
        result = runtime.get("result") or runtime
        return render_node_card_template(block=self, node=node, node_classes=["realtime-stt-node"], replacements={
            "title": node.get("title") or self.default_title(), "model": MODEL,
            "status": str(result.get("last_message") or "Run → listening · start → transcribe")[:240],
        })

    def _settings_html(self, node: dict, *, surface: str = "modal") -> str:
        """Render essential/advanced groups with escaped values and surface-local help IDs.

        Args:
            node: Serialized node; invalid imported settings remain editable.
            surface: Distinguishes labels/help when modal and inspector coexist.
        """
        config = {**DEFAULTS, **(node.get("config") or {})}
        labels = {
            "api_key_ref": "OpenAI secret reference", "languages": "Expected languages (fr,en…)",
            "prompt": "Context / vocabulary", "segment_seconds": "Periodic split · duration mode (s)",
            "segmentation": "End of speech turns",
            "drain_timeout_sec": "Wait for the last frames after stop (s)",
            "final_timeout_sec": "Wait for the final results (s)",
            "connect_timeout_sec": "Connection timeout (s)", "max_duration_sec": "Maximum capture duration (s)",
        }
        hints = {
            "api_key_ref": "Copy the complete reference from Settings → Secrets, not the API key. Unlock the wallet before Run.",
            "languages": "Comma-separated codes, for example fr,en. Optional.",
            "prompt": "Proper nouns, domain terms or useful context. Optional · 1,024 characters maximum.",
            "segment_seconds": "Used only in Duration mode. Every confirmed segment is published immediately; this setting does not detect sentences.",
            "segmentation": "Duration: periodic split. External: wire the detector's begin/commit to command_in. Resumes up to 1.5 s before the detected start, without duplicates; 8 s audio memory, with no added delay. Nothing is sent while idle; safety cut after 60 s of speech.",
            "drain_timeout_sec": "Time granted to the last frames after the microphone stops.",
            "final_timeout_sec": "Time granted to the final decoding and to the OpenAI results, per step.",
            "connect_timeout_sec": "Time granted to the connection and to the session configuration.",
            "max_duration_sec": "Safety limit per capture: 3,600 s = 1 hour.",
        }
        prefix = escape(f"{node.get('id', 'stt')}-{surface}-stt", quote=True)
        fields = {}
        for key, label in labels.items():
            value = escape(str(config[key]), quote=True)
            attrs = f'id="{prefix}-{key}" aria-describedby="{prefix}-{key}-help" data-stt-setting="{key}"'
            if key == "segmentation":
                choices = {"duration": "Duration (default)", "external": "External voice detector"}
                if config[key] not in choices:
                    choices = {str(config[key]): "Unrecognized value", **choices}
                options = "".join(f'<option value="{escape(option, quote=True)}"{" selected" if config[key] == option else ""}>{label}</option>'
                                  for option, label in choices.items())
                control = f'<select {attrs}>{options}</select>'
            elif key == "prompt":
                control = f'<textarea {attrs} rows="3" maxlength="1024" placeholder="E.g. BloxSmith, product names, technical vocabulary…">{value}</textarea>'
            elif key in BOUNDS:
                minimum, maximum = BOUNDS[key]
                step = "1" if key in {"segment_seconds", "max_duration_sec"} else "0.25"
                control = f'<input {attrs} type="number" required min="{minimum}" max="{maximum}" step="{step}" value="{value}" />'
            else:
                placeholder = ' placeholder="secret://workspace/openai_stt_api"' if key == "api_key_ref" else ''
                control = f'<input {attrs} type="text" autocomplete="off" spellcheck="false"{placeholder} value="{value}" />'
            fields[key] = (f'<div class="field-group"><label for="{prefix}-{key}">{escape(label)}</label>{control}'
                           f'<small class="stt-help" id="{prefix}-{key}-help">{escape(hints[key])}</small></div>')
        delay_labels = {"minimal": "Minimal", "low": "Low (default)", "medium": "Medium", "high": "High", "xhigh": "Very high"}
        options = "".join(f'<option value="{option}"{" selected" if config["delay"] == option else ""}>{label}</option>'
                          for option, label in delay_labels.items())
        # Preserve an invalid imported value visibly until the user chooses a valid setting.
        if config["delay"] not in delay_labels:
            invalid = escape(str(config["delay"]), quote=True)
            options = f'<option value="{invalid}" selected>Unrecognized value: {invalid}</option>' + options
        delay = (f'<div class="field-group"><label for="{prefix}-delay">Model delay</label>'
                 f'<select id="{prefix}-delay" data-stt-setting="delay">{options}</select></div>')
        return (
            '<section class="stt-section"><div class="stt-section-heading"><h3>Connection</h3>'
            f'<span class="stt-model">Model: <code>{MODEL}</code></span></div>{fields["api_key_ref"]}'
            '<p class="stt-notice">The audio is sent to OpenAI and billed to your API account. The key stays in the wallet, on the server.</p></section>'
            '<section class="stt-section"><h3>Transcription</h3><div class="stt-fields-grid">'
            f'{fields["languages"]}{fields["segmentation"]}{fields["segment_seconds"]}</div>{fields["prompt"]}</section>'
            '<details class="stt-disclosure stt-advanced"><summary>Advanced settings <span>Delays and limits</span></summary>'
            '<div class="stt-disclosure-body"><p class="stt-help">The default values are fine to start with.</p>'
            f'<div class="stt-fields-grid">{delay}{fields["max_duration_sec"]}{fields["connect_timeout_sec"]}'
            f'{fields["drain_timeout_sec"]}{fields["final_timeout_sec"]}</div></div></details>'
            '<p class="stt-help stt-run-note">Run prepares the listening; the microphone drives the start and the end on the data link. After changing the settings: Stop, then Run.</p>'
        )

    def render_modal(self, *, node: dict, payload: dict | None = None) -> dict:
        """Render the node's grouped properties, unified apply bar and optional runtime snapshot."""
        template = (self.directory / "block_modal.html").read_text(encoding="utf-8")
        template = template.replace("{{ settings_html }}", self._settings_html(node))
        has_error = bool(self._runtime_error_text(payload or {}))
        diagnostics = (f'<details class="stt-disclosure stt-diagnostics"{" open" if has_error else ""}>'
                       f'<summary>Diagnostic · {"Error" if has_error else "No error"}</summary>'
                       f'<div class="stt-disclosure-body">{self._render_generic_modal_error_tab(payload or {})}</div></details>')
        template = template.replace("{{ diagnostics_html }}", diagnostics)
        return {"html": self._render_generic_modal_template(template=template, node=node, payload=payload or {}),
                "context": {"node_id": str(node.get("id") or ""), "node_kind": self.kind}}

    def render_inspector_panel(self, *, node: dict, payload: dict | None = None) -> dict:
        """Render grouped properties for the node and preserve the framework's inspector tabs."""
        template = (self.directory / "inspector_panel.html").read_text(encoding="utf-8")
        template = template.replace("{{ settings_html }}", self._settings_html(node, surface="inspector"))
        return {"html": render_inspector_template(template=template, node={**node, "type": self.kind, "kind": self.kind},
                payload=payload, show_duplicate=True), "context": {"node_id": str(node.get("id") or ""), "full_panel": True}}

    def handle_ui_action(self, *, node: dict, action: str, values: dict, payload: dict | None = None) -> dict:
        """Validate node properties atomically before returning a graph patch.

        ``save_properties`` accepts title/config together from either UI surface;
        ``save_settings`` retains its config-only payload. Only wallet references,
        never raw keys, are accepted. Invalid values return an error without a patch.
        """
        try:
            if action in {"save_settings", "save_properties"}:
                # Owned property saves validate the whole edit before returning an atomic node patch.
                # Generic field actions are handled by the framework before block-specific validation.
                if action == "save_properties":
                    if set(values) - {"title", "config"} or not isinstance(values.get("title"), str):
                        raise RealtimeSttError("Invalid STT properties.")
                    settings = values.get("config")
                else:
                    settings = values
                if not isinstance(settings, dict) or set(settings) - DEFAULTS.keys():
                    raise RealtimeSttError("Unknown STT setting; no clear-text key can be saved.")
                config = _config({**(node.get("config") or {}), **settings})
                patch = {"config": config}
                if action == "save_properties":
                    patch["title"] = values["title"]
                return {"node_patch": patch, "rerender_inspector": False}
            result = super().handle_ui_action(node=node, action=action, values=values, payload=payload)
            patch = result.get("node_patch") or {}
            if "config" in patch:
                if set(patch["config"]) - DEFAULTS.keys():
                    raise RealtimeSttError("Unknown STT setting.")
                patch["config"] = _config({**(node.get("config") or {}), **patch["config"]})
            return result
        except RealtimeSttError as exc:
            return {"error": str(exc)}
