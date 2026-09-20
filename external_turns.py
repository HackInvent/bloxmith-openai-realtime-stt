"""Bounded PCM turn gating owned by Realtime STT, independent of graph/runtime internals."""

from __future__ import annotations

import time
from typing import Awaitable, Callable


PCM_BYTES_PER_MS = 48
HISTORY_BYTES = 8_000 * PCM_BYTES_PER_MS
SPEECH_PREFIX_BYTES = 1_500 * PCM_BYTES_PER_MS
SAFETY_TURN_BYTES = 60_000 * PCM_BYTES_PER_MS


class ExternalTurnError(ValueError):
    """A safe diagnostic for loss of PCM required by an explicitly announced speech turn."""


class ExternalPcmTurns:
    """Retain eight seconds of PCM and recover a prefix only after an explicit speech begin.

    ``boundaries`` is the capture-owned list of (offset_ms, action, arrival_time).
    The listener may append to it between awaits; all access stays on its event loop.
    ``append`` accepts real PCM bytes. ``commit`` accepts a reason and requested/actual
    source offsets; synthetic minimum-duration padding belongs to the caller, not this clock.
    The 1.5-second prefix changes the cut, never the upstream VAD decision. Logical
    closed boundaries and the sent high-water mark prevent replay between utterances.
    """

    def __init__(self, boundaries: list, append: Callable[[bytes], Awaitable[None]],
                 commit: Callable[..., Awaitable[None]], warn: Callable[[str], None],
                 *, timeout_sec: float, clock: Callable[[], float] = time.monotonic):
        """Bind the current capture callbacks without allocating IO or a background task."""
        self.boundaries, self.append, self.commit, self.warn = boundaries, append, commit, warn
        self.timeout_sec, self.clock = timeout_sec, clock
        self.history = bytearray()
        self.tail = b""
        self.decoded_bytes = self.sent_until = self.segment_bytes = 0
        self.closed_until = -1
        self.opened_at = None
        self.segment_start = 0
        self.active = False
        self.rejected_begin = False

    @property
    def history_start(self) -> int:
        """Return the absolute source-byte offset still retained in the rolling prebuffer."""
        return self.decoded_bytes - len(self.history)

    async def feed(self, raw: bytes) -> None:
        """Admit aligned decoded samples and process commands; retaining history adds no playback delay."""
        raw = self.tail + raw
        length = len(raw) // 2 * 2
        self.tail = raw[length:]
        self.history.extend(raw[:length])
        self.decoded_bytes += length
        # Keep the just-read chunk while an active turn drains it; idle history is trimmed below.
        await self.tick()
        if len(self.history) > HISTORY_BYTES:
            del self.history[:-HISTORY_BYTES]

    async def _commit_segment(self, reason: str, audio_end_ms: int | None) -> None:
        """Commit a nonempty segment with its requested VAD start and actual unique PCM range."""
        if self.segment_bytes:
            await self.commit(reason=reason, audio_start_ms=self.opened_at // PCM_BYTES_PER_MS,
                              committed_audio_start_ms=self.segment_start // PCM_BYTES_PER_MS,
                              audio_end_ms=audio_end_ms,
                              committed_audio_end_ms=self.sent_until // PCM_BYTES_PER_MS)
        self.segment_bytes = 0
        self.segment_start = self.sent_until

    async def _send_until(self, end: int) -> None:
        """Append retained source audio in bounded chunks, with a 60-second speech safety cut."""
        while self.sent_until < end:
            offset = self.sent_until - self.history_start
            if offset < 0:
                raise ExternalTurnError("Required audio outside the STT prebuffer: the speech start arrived too late.")
            count = min(end - self.sent_until, 9600, SAFETY_TURN_BYTES - self.segment_bytes)
            await self.append(bytes(self.history[offset:offset + count]))
            self.sent_until += count
            self.segment_bytes += count
            if self.segment_bytes == SAFETY_TURN_BYTES:
                await self._commit_segment("safety", None)

    async def tick(self) -> None:
        """Apply ready begin/commit commands even without new PCM; expire impossible waits visibly."""
        while self.boundaries:
            # Callbacks can admit commands during append/commit awaits; reconsider their source order.
            self.boundaries.sort(key=lambda item: (item[0], item[1] != "begin"))
            offset_ms, action, arrived = self.boundaries[0]
            offset = offset_ms * PCM_BYTES_PER_MS
            if action == "commit" and offset <= self.closed_until:
                self.boundaries.pop(0)
                continue
            if offset > self.decoded_bytes:
                break
            if action == "begin" and self.active and offset > self.opened_at:
                # A reordered next begin must wait for the current commit, not erase that next turn.
                break
            if action == "commit" and not self.active:
                # A rejected turn's end cannot block the next speech for five seconds.
                # A later begin also proves this orphan cannot close that newer turn;
                # otherwise keep waiting for a reordered earlier begin within the deadline.
                next_begin = any(candidate[1] == "begin" and candidate[0] >= offset_ms
                                 for candidate in self.boundaries[1:])
                if self.rejected_begin or next_begin:
                    self.boundaries.pop(0)
                    self.closed_until = max(self.closed_until, offset)
                    if not self.rejected_begin:
                        self.warn(f"Speech end without an accepted start ignored at {offset_ms} ms; the next speech turn stays admissible.")
                    self.rejected_begin = False
                    continue
                break
            self.boundaries.pop(0)
            if action == "begin":
                if self.active:
                    self.warn("Speech start ignored: a speech turn is already open; commit expected.")
                elif offset < max(self.history_start, self.decoded_bytes - HISTORY_BYTES) or offset < self.closed_until:
                    self.rejected_begin = True
                    self.warn("Speech start too old: outside the 8 s STT prebuffer, or the turn is already closed; no truncated audio is sent. "
                              f"start={offset_ms}ms decoded={self.decoded_bytes // PCM_BYTES_PER_MS}ms "
                              f"prebuffer_since={max(self.history_start, self.decoded_bytes - HISTORY_BYTES) // PCM_BYTES_PER_MS}ms "
                              f"already_sent_until={self.sent_until // PCM_BYTES_PER_MS}ms "
                              f"command_wait={max(0, int((self.clock() - arrived) * 1000))}ms")
                else:
                    # Prefix only confirmed turns. Never replay samples already appended, even when a
                    # late previous commit sent past this new begin; retain the new turn's unsent tail.
                    self.active = True
                    self.rejected_begin = False
                    self.opened_at = offset
                    self.sent_until = max(0, offset - SPEECH_PREFIX_BYTES, self.sent_until,
                                          self.closed_until, self.history_start,
                                          self.decoded_bytes - HISTORY_BYTES)
                    self.segment_start = self.sent_until
                    self.segment_bytes = 0
            else:
                # Data/audio can race. A late commit cannot retract audio already appended remotely.
                await self._send_until(max(offset, self.sent_until))
                await self._commit_segment("external", offset_ms)
                # A requested end and the actual sent end are different clocks for deduplication:
                # the next valid speech may begin inside the late commit's already-sent overlap.
                self.closed_until = offset
                self.opened_at = None
                self.active = False
        now = self.clock()
        for boundary in list(self.boundaries):
            if now - boundary[2] > self.timeout_sec:
                self.boundaries.remove(boundary)
                self.warn("Speech command expired: audio offset not received or begin missing; capture kept.")
        if self.active:
            await self._send_until(self.decoded_bytes)

    async def finish(self) -> None:
        """Commit only an open speech tail on source stop, never idle silence or nonexistent samples."""
        if self.tail:
            raise ExternalTurnError("Truncated PCM audio: incomplete final sample.")
        await self.tick()
        if self.active:
            await self._commit_segment("stop", None)
        self.active = False
        self.opened_at = None
        self.segment_bytes = 0
        if self.boundaries:
            self.warn("Capture ended before the expected speech offsets; remaining commands ignored.")
            self.boundaries.clear()
