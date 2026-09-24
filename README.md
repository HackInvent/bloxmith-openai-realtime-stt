# OpenAI Realtime STT

<!-- block-metadata:start -->
[![Block version: 0.1.0](https://img.shields.io/badge/block-0.1.0-blue)](model.json)
[![BloxSmith compatibility: 1.0.9](https://img.shields.io/badge/BloxSmith-1.0.9-brightgreen)](compatibility.json)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Verified BloxSmith versions: **1.0.9** (bundled-block tests; see [test evidence](compatibility.json)).
<!-- block-metadata:end -->


Transcribe **Microphone Stream** or **OpenAI TTS Stream** live, without an intermediate audio file. The block sends audio to OpenAI Realtime in a `transcription` session using `gpt-live-transcribe`. An API key with access to that model is required; OpenAI usage is billed to its associated account.

## Declared version

Block version **0.1.0** follows the shared initial-version policy. The declared and tested framework version is **BloxSmith 1.0.9**, matching the tested framework's `VERSION`, in `centralized` and `zeromq_active`. No other framework version is inferred to be compatible.

## Connections

- `Microphone Stream.audio_out → OpenAI Realtime STT.audio_in`: audio link.
- `Microphone Stream.command_out → OpenAI Realtime STT.command_in`: separate data link.
- `partial_out` (**provisional text**, “Texte provisoire” in the UI) → Display: evolving previews.
- `final_out` (**final text**, “Texte final” in the UI) → Display or text processing: each OpenAI-confirmed segment, without waiting for the microphone to stop.

Two fixed inputs and two fixed outputs; no implicit data channel inside audio. `audio_in` accepts mono/stereo Opus (WebM/Ogg) and AAC (fragmented MP4), and does not trigger ordinary execution. `command_in` is a required JSON input for data activation, with multiplicity one.

For **OpenAI TTS Stream**, connect both its `audio_out` and `command_out` to the matching inputs. Each text automatically opens a transcription session; its stop command closes the last segment. No microphone action is needed.

The shared graph format is **Opus in WebM or Ogg**. TTS emits Ogg/Opus; Microphone Stream selects WebM/Opus or Ogg/Opus. AAC support is retained for existing compatible external sources. Mono 24 kHz PCM is an **internal STT adaptation** for OpenAI, not a required graph-link format.

## API key and prerequisites

1. Create an OpenAI key entry in **Settings → Secrets**.
2. Enter only its reference in the OpenAI secret-reference field, for example `secret://workspace/openai` or `secret://project/<workspace_project_id>/openai`.
3. Unlock the wallet before Run. Missing/invalid references, a locked wallet or a rejected key produce explicit errors.

The key is not stored in blueprint configuration or sent through ports. Only the injected runtime secret resolver is used: no plaintext key field, environment-variable fallback, key in a URL or OpenAI connection from block JavaScript. Raw responses and connection headers are never logged.

The server requires **FFmpeg** on PATH and the Python dependency in `requirements.txt`, installed with the interpreter running BloxSmith. From the block repository:

```sh
python3 -m pip install -r requirements.txt
```

Use a virtual environment when the operating system manages Python. The block downloads or installs nothing during a Run. Catalog discovery and simulation remain possible without these dependencies.

## Capture lifecycle

1. Select **Active Runtime**, then **Run**. Listening is ready without Play.
2. Start the source. Its `start` opens an OpenAI session and arms transcription.
3. Compressed chunks are continuously decoded into signed 16-bit mono PCM at 24 kHz. `partial_out` publishes the current segment's previews; `final_out` publishes each confirmed segment in audio order.
4. Stop the **source**. Its `stop` announces final frame/byte counts. The block waits for missing chunks, drains the decoder, closes the last segment, waits for remaining final results, then closes the connection. Previously published segments are not repeated, and no cumulative capture transcript is emitted.
5. Wait for **Transcription complete** before stopping the Run. A new capture can then start without reloading the listener.

Runtime **Stop/Cancel cancels active sessions**. It does not replace source `stop` and cannot guarantee the last segment. Already published previews and final segments remain run results. This block does not capture extra audio. A source `stop` with `aborted: true` explicitly cancels its transcription. Diagnostics refer to an audio source rather than assuming a microphone.

The TTS `F5.49_opus_interoperability.py` suite checks TTS → STT through the real runtime with simulated HTTP/WebSocket APIs: final segments before stop, mono/stereo decoding and automatic closure, without paid requests.

### Continuous conversations and connection renewal

The STT no longer ends a capture after one hour. Source `start`/`stop` controls the
capture lifetime; enable **Continuous capture** in Microphone Stream for an
untimed microphone session. No blueprint wiring change is needed.

The existing `max_duration_sec` setting now controls **OpenAI connection lifetime**,
not total capture duration. Its name is retained for existing configurations.
With the default 3,600 seconds, the block preconnects around 55 minutes, then
switches between speech turns or at the next committed segment. An open turn can
be safety-split before the deadline (`reason: renewal`), without closing that VAD
turn. This is block-owned renewal policy, not a guarantee of a provider's session
limit or availability.

The same source `stream_id`, frame/byte counters, persistent FFmpeg decoder,
absolute decoded-audio clock, eight-second history and 1.5-second prefix survive
every renewal. The MediaRecorder stream is not restarted and no audio is replayed.
Idle external-mode silence remains unsent and produces no text.

At most two remote connections coexist per capture: either current + preparing,
or current + draining. Old final results are drained while the new connection
receives audio. Confirmed final segments retain audio order across generations;
late previews from an older generation are ignored after switching. Output
metadata adds `connection_generation` to distinguish reused remote item IDs.
Renewals report `renewing`/`renewed`, not `failed` or capture `completed`.

Pending finals retain their finite timeout and bounded queues. Authentication,
quota, unexpected disconnects, missing final results or saturation still report
errors. There is no speculative transcript, unlimited retry or hidden audio replay.
Source Stop cancels an unused replacement and drains the current capture; Run
Stop cancels all connections immediately as before. OpenAI context is configured
again from the same settings, but no provider-internal conversation state is
copied between transcription connections.

## Text outputs

Both outputs emit `text/plain` and also declare static `message/*` compatibility, like file-based OpenAI STT, so they connect to Display.

- **`partial_out`** publishes evolving previews, coalescing OpenAI `delta` events at most five times per second. Each value replaces that segment's preview: it is neither a fragment to concatenate nor a full-capture transcript. It may be corrected; do not use it to trigger irreversible actions.
- **`final_out`** publishes **each complete segment once**, on `conversation.item.input_audio_transcription.completed`, without waiting for stop. Acknowledgements establish audio order: an early result waits for earlier segments, not for capture completion. OpenAI's final text is preserved exactly, without stitching segments or rebuilding it from deltas. A delta, acknowledgement alone, stop, error, cancellation or timeout never turns provisional text into a final result.

Default segmentation uses fixed durations (15 seconds), committing the remainder on stop. Confirmed segments can appear while the microphone remains open. A segment is **not necessarily a sentence**: duration mode does not automatically detect silence or sentence boundaries.

Output metadata identifies `stream_id`, `item_id`, state, `is_final` and counters. Session state `completed` means closure after stop, not the first available final segment. Text port values themselves do not contain these IDs. An empty capture emits no text; an empty final transcript for a segment remains an empty final value.

## Conversation with external voice detection

Set **Turn boundaries → External voice detector** (`segmentation: external`). Ports do not change. Merge the microphone's start/stop commands and detector commands converted by a Python block into `command_in`, for example through **Event OR**. Keep every message as a separate JSON object:

```json
{"action":"begin","stream_id":"capture-123","audio_start_ms":1200}
{"action":"commit","stream_id":"capture-123","audio_end_ms":3200}
```

Offsets are integer **decoded-audio milliseconds since stream start**, not arrival times or system timestamps. Keep the microphone's `stream_id`.

Decoding is continuous. While idle, the block retains **eight seconds** of rolling PCM history (375 KiB at mono 24 kHz) and sends no audio to OpenAI. After a confirmed begin, it recovers **up to 1.5 seconds before `audio_start_ms`**, preserving initial syllables that preceded detection. This memory adds no waiting delay and cannot start transcription on its own. VAD thresholds, model and events are unchanged: rejected sounds do not become STT turns because of prebuffering.

Recovery is bounded by stream start, retained history, the last accepted turn end and the last sample already sent. Closely spaced phrases therefore do not resend overlapping audio. If a previous commit already sent beyond a new begin boundary, **the unsent remainder of the new phrase is still accepted**. A fully covered phrase produces neither duplicates nor an empty commit. Previously sent audio cannot be reassigned retroactively.

A begin older than the eight-second history or belonging to a closed turn is rejected with a warning, never silently trimmed. Diagnostics include requested, decoded, retained and already-sent offsets and local command wait time. The matching end of a rejected begin is discarded without blocking later speech.

An end without a begin waits briefly for a reordered begin. If a newer begin arrives, that orphaned end is ignored with a diagnostic rather than causing cascading expirations. A new begin received before the active turn ends waits for that end within the usual bounded timeout instead of being immediately discarded.

A commit waits until decoded audio reaches `audio_end_ms`, then commits the turn to OpenAI. Its confirmed text appears on `final_out` without closing the connection, decoder or microphone. Subsequent idle silence is not sent; a new begin opens another turn.

Audio already sent beyond a late commit boundary cannot be withdrawn. `segment_committed` metadata exposes requested `audio_start_ms`/`audio_end_ms` and actual `committed_audio_start_ms`/`committed_audio_end_ms`, rounded down to milliseconds, making recovered lead-in and late overlaps visible.

### External-boundary limits

- Offsets must advance for each action. Equal/older offsets are ignored. Offsets are nonnegative safe JSON integers (at most 9,007,199,254,740,991 ms), independent of connection lifetime; offsets beyond one hour remain valid.
- At most 64 pending boundaries per capture, plus 64 pre-start messages retained for at most five seconds. Early boundaries alone never open a session.
- A boundary lacking its audio or begin expires after `drain_timeout_sec` with a nonterminal warning; capture remains available.
- Continuous speech is safety-split at **60 seconds**, then continues without another begin. Metadata reports `reason: safety`. Idle silence is not periodically committed instead of external commands.
- A turn shorter than 100 ms is padded with silence for the OpenAI commit without advancing the source clock. Stop closes an open turn, never idle silence.
- Ordered JSON batches of 1–64 commands are accepted, up to 64 KiB. The whole batch is validated before one listener-envelope delivery, then drained in order. Distinct `input_events` in an activation are handled similarly without concatenating JSON objects. Invalid batches are never partially delivered.

Default `duration` mode and `segment_seconds` are unchanged. Begin/commit in duration mode produce warnings without interrupting capture.

## Settings

| Setting | Default | Constraint |
| --- | --- | --- |
| Secret reference | Empty | `secret://workspace/name` or `secret://project/id/name`; required in Active Runtime |
| Model | `gpt-live-transcribe` | Live previews and final segments |
| Expected languages | `fr` | Up to eight comma-separated language codes; empty means no hint |
| Context | Empty | Vocabulary/context hint, at most 1,024 characters |
| Model delay | `low` | `minimal`, `low`, `medium`, `high`, `xhigh` |
| Segmentation | `duration` | Periodic duration or external begin/commit |
| Segment duration | 15 s | Integer 1–60 s, duration mode only |
| Post-stop frame wait | 5 s | 0.25–30 s |
| Connection/configuration wait | 10 s | 1–30 s |
| Final decoder/results wait | 20 s | 1–60 s per stage |
| OpenAI connection lifetime (`max_duration_sec`) | 3,600 s | Integer 1–3,600 s per remote connection; automatic renewal, no total capture limit |

Apply changes, then **Stop → Run** to load them. The card shows runtime state; the modal shows an opening snapshot. Connected Displays receive live outputs.

## Properties UI

The modal and inspector group fields into **Connection** (full secret reference) and **Transcription** (languages, turn mode, periodic duration and context). External-mode help explains commands, prebuffering and the speech limit. **Advanced settings** are collapsed by default and contain model delay, timeouts and connection lifetime. Help text distinguishes connection renewal from capture duration; layout adapts to the viewport or inspector width.

One Apply button saves the block name and settings together through block-owned `save_properties` on the generic UI bridge. All fields are validated before returning one patch; `save_settings` remains for legacy clients. Apply stays accessible while scrolling, at the modal bottom or inspector top.

Status distinguishes drafts, saving and errors. Restoring applied values disables Apply; edits made during a save remain pending. An invalid numeric field opens its advanced section before being reported.

Ports, opening state and diagnostics are collapsible. Runtime errors automatically expand diagnostics on opening while preserving the generic error panel. This layout does not change ports, persisted configuration or the audio/STT protocol.

## Reliability and limits

- Framework-supervised listening; per-node/per-run state and cancellable I/O.
- At most four simultaneous/finalizing captures. Use one capture at a time when text ports must unambiguously correspond to one source.
- Pre-start buffer: five seconds, 128 frames, 8 MiB. Capture queues are also bounded to 128 frames/8 MiB; saturation fails explicitly.
- Stop counters and sequence continuity detect loss. Audio and commands remain best-effort, with no automatic replay that could duplicate transcripts or billing.
- The first chunk must contain container headers. One persistent decoder per capture retains that context; chunks are not decoded independently.
- Text is bounded to 32,000 characters per segment and 64 pending items. Published segments are released; a full-capture transcript is not accumulated.
- Each of the at most two connections has bounded pending items. At most 64 confirmed new-generation segments can wait for old finals; the older connection's timeout bounds that wait. Other graph consumers and framework result retention have their own limits.
- Limit violations, network failures, invalid formats and aborted captures are errors, not silent successes.
- A later error cannot retract previews or final segments already consumed downstream.
- Latency depends on the browser, FFmpeg, network and OpenAI. Validate MediaRecorder formats in target browsers; installation alone guarantees no fixed latency. No diarization or word-level timestamps.
- **One Shot Simulation (`centralized`)** explicitly returns `skipped`, without wallet access, OpenAI requests, FFmpeg processes or fake outputs. Live streaming requires Active Runtime; `openai_stt` handles files.

## References and tests

Protocol references: [Realtime transcription](https://developers.openai.com/api/docs/guides/realtime-transcription) and [server-side WebSocket connections](https://developers.openai.com/api/docs/guides/realtime-websocket).

Local tests simulate OpenAI and use no real key or billed request. Framework-independent `external_turns.py` owns external-turn PCM management. `tests/F5.52_external_turns.py` checks prebuffering, offsets, long-speech safety, command queues and confirmed turns before stop.

Initial-word recovery is checked with identifiable PCM and normal/quiet synthetic phrases using the unchanged real Silero model and a 3.2-second command delay. Ogg/Opus also passes through the real decoder and local WebSocket; sent samples must exactly match the intended decoded-file range.

Tests cover shared prefixes, fully sent turns, reordered commands, the eight-second bound and no publication for silence, noise, clicks, DC or tones. These integration cases use Voice Activity Detection dependencies and FFmpeg's `flite` filter only during tests. Original VAD tests remain unchanged and must pass; fixtures do not guarantee rejection of all background speech or acoustic echo.

Other coverage includes real WebM/Ogg/AAC decoding; previews and confirmed segments before stop; actual execution of the final Display; no final output from a delta or acknowledgement alone; last-segment waiting without blocking previous results; duplicates and out-of-order completions; text bounds; no fabricated final text on errors/cancellation; loss, timeouts, cooperative stop, HTTP surfaces and mini-graphs in both modes.

`F5.76_continuous_sessions.py` covers quiet and mid-speech renewals, exact PCM
equality across sockets, reused item IDs, late old finals, continuous previews,
Stop during reconnection, visible authentication failures and offsets beyond two
hours. `F5.77_continuous_endurance.py` streams **2 h 15 min of generated Ogg/Opus**
through the real decoder/runtime and a local provider simulator, advancing the
connection clock with decoded audio. It checks repeated turns, counters, one
decoder, bounded queues/history and process memory high-water growth. Its JSON
report includes actual wall-clock duration and memory samples. This accelerated
test is not a wall-clock soak test or a live OpenAI availability certification.

UI tests check drafts, errors, edits during requests, atomic renaming/settings, restored values, advanced validation and read-only states. FFmpeg container probing is bounded so PCM starts before recording ends.

From the private integration workspace:

```sh
python3 -B tests/run_tests.py openai_realtime_stt --timeout-sec 360
```

Integration fixtures create temporary blueprints through the framework before preparing an instance; no real instance IDs are reused.

Additional visual checks from the prepared application copy require Playwright/Chromium:

```sh
python3 blocs/openai_realtime_stt/tests/F5.46_openai_realtime_stt_block.py --ui
python3 blocs/openai_realtime_stt/tests/F5.52_external_turns.py --ui
```

They cover 320–1,440 px modals, a narrow inspector, atomic Apply through the real UI/HTTP bridge, scrolling and keyboard access to advanced options.

## Compatibility policy

[compatibility.json](compatibility.json) records HackInvent's verified BloxSmith versions and test evidence. Only the versions listed above have been verified, using the block-owned suites in a **bundled-block test installation**. This is not a certification of managed-package installation, every browser/OS, or live provider availability. Other framework versions are unverified, not necessarily incompatible.

The block-version badge follows `model.json`, not a published Git tag. `unversioned` means that no block release version is declared; no number is inferred from the framework version. The framework still uses `model.json` for its runtime/install contract; the tester-owned JSON does not replace it. Official integration tests run in the private `bloxmith-blocs` workspace. Test helpers and the proprietary framework are not bundled in this public block repository.

## Properties ergonomics

Modal and inspector styles are owned by this package and scoped to its exact
release. Forms adapt to narrow panels, checkboxes stay beside their labels, and
long values do not widen the inspector. Existing labels are associated with
controls; keyboard navigation complements the block’s own tab handlers.
These presentation helpers do not change port bindings, authored settings,
runtime behavior or the block’s original surface cleanup.
