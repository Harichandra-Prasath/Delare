# Delare

Delare (binary name `delared`) is a lightweight Docker log aggregation daemon written in Go with zero external dependencies. It streams logs from running containers, encodes them in a compact binary format, and persists them to segmented, indexed files on disk.

## The Problem

Container logs are ephemeral. When a container stops or is removed, its logs are lost with it. Collecting logs reliably requires an agent that:

- Runs continuously and survives container restarts.
- Resumes from where it left off after its own restarts.
- Persists logs in a durable, space-efficient format.
- Adds minimal overhead and footprint.

Existing solutions tend to be heavy, pulling in large dependency trees and framework layers. Delare is a single binary with no external dependencies, designed to do one thing well: collect Docker container logs and store them durably.

## How It Works

1. Connect to the Docker daemon via the Unix socket at `/var/run/docker.sock`.
2. Start streaming logs from each configured container, resuming from its last checkpoint.
3. Subscribe to Docker events to continuously track container `start` and `die` events, automatically starting and stopping streams.
4. Encode each log line into a custom binary frame.
5. Push frames into a lock-free ring buffer.
6. A single dispatch goroutine batches buffered frames and flushes them to segmented, indexed files on disk.
7. Per-container checkpoints are persisted periodically so ingestion resumes at the correct position after a restart.

## Architecture

The daemon follows a producer-consumer pipeline: `stream -> encode -> ring buffer -> dispatch -> disk`.

| Package | Responsibility |
|---|---|
| `internal/ingestion` | Docker socket client, event-based control panel, per-container log streaming |
| `internal/protocol` | Custom binary frame encoding |
| `internal/storage` | Ring buffer, dispatch loop, disk writer, container mapper, checkpoint manager |
| `internal/arena` | Pooled byte buffers to reduce garbage collection pressure |
| `internal/logging` | Structured JSON logging via `slog` |

All packages use only the Go standard library.

## Custom Binary Protocol

Each log frame is a 20-byte header followed by the encoded payload:

| Offset | Size | Field |
|---|---|---|
| 0-1 | 2 bytes | Magic bytes `0xDE 0xAD` |
| 2-5 | 4 bytes | Total frame length |
| 6-13 | 8 bytes | Timestamp (microseconds since Unix epoch) |
| 14-15 | 2 bytes | Container ID (`uint16`) |
| 16-19 | 4 bytes | CRC32 checksum of the payload |

The CRC32 checksum provides integrity verification for each frame.

## Storage Format

Data is stored under `~/.delared/`.

| File | Purpose |
|---|---|
| `<ts>.log` | Segmented log file holding contiguous encoded frames. New segments are created per timestamp and rotate at 10MB. |
| `<ts>.index` | Paired index file with 16-byte entries (timestamp + file offset) written every 4KB of segment data, allowing time-based lookup. |
| `mapper.json` | Maps container names to compact `uint16` IDs. |
| `checkpoints.json` | Per-container last-ingested timestamp, persisted every 5 seconds. |

On startup, Delare recovers state from the latest segment, the index, the mapper, and the checkpoints.

## Build

Compile the binary with `make` or `make build`:

```sh
make
```

The binary is written to `bin/delared`.

## Usage

```
delared --containers=<name1,name2,...> [--log-level=<level>]
```

| Flag | Description |
|---|---|
| `--containers` | Comma-separated list of container names to collect logs from. Required. |
| `--log-level` | Log level for the daemon. Defaults to `info`. |

## Design Decisions

- **Zero external dependencies**: The Docker client, ring buffer, protocol, and disk writer are all implemented from scratch using only the standard library.
- **Lock-free ring buffer**: The multi-producer single-consumer ring buffer uses atomic operations with cache-line padding to avoid false sharing and lock contention.
- **Pooled buffers**: Byte buffers are recycled through a `sync.Pool` to reduce garbage collection pressure under sustained log volume.
- **Compact binary protocol**: Logs are stored as tightly packed binary frames with CRC32 integrity checks rather than plain text or JSON.
- **Checkpoint-based resumption**: Ingestion positions are persisted so the daemon resumes exactly where it left off, avoiding duplicate or missing logs across restarts.

## Work in Progress

- **Resource cleanup on shutdown**: Graceful handling of SIGTERM/SIGINT is not yet implemented. This includes cancelling active log streams, draining the ring buffer, flushing in-flight batches, closing open file handles, and persisting a final checkpoint before exit.
- **Replay agent/client**: An interface to read back and decode the persisted binary logs is planned.

## License

MIT License
