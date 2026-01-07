# Go-WAL: A Lightweight, Durable Write-Ahead Log

A high-performance, crash-consistent Write-Ahead Log (WAL) implementation in Go. Designed for systems requiring strict durability guarantees, such as custom databases or Raft-based distributed state machines.

## Key Features

* **Prefix Consistency**: Bypasses user-space buffering to ensure reads (`GetEntry`) always see the latest writes (`Append`) via the OS page cache.
* **Crash Recovery**: Automatic log scanning on startup with CRC32 checksum validation to detect and truncate partial writes or corrupted data.
* **OOM Protection**: Implements a "Double-Read" pattern (Header -> Validation -> Body) to prevent memory exhaustion from corrupted length fields.
* **Raft-Ready**: Includes `TruncateFromIndex` for log conflict resolution and `Sync()` for explicit durability control.
* **Zero-Allocation Checksumming**: Optimized hashing logic to reduce GC pressure during high-throughput ingestion.

## Entry Format

Each entry is serialized into a binary frame:
| Byte Offset | Field | Type | Description |
| :--- | :--- | :--- | :--- |
| 0 | Type | `uint8` | Entry type (Data/Internal) |
| 1-4 | Length | `uint32` | Size of the data payload |
| 5-8 | Checksum | `uint32` | CRC32 of Type + Length + Data |
| 9-N | Data | `[]byte` | The raw payload |

## Usage

### Initialization

```go
import "github.com/youruser/go-wal"

// Initialize with default 10MB entry limit
w, err := wal.New("data/server.wal")
if err != nil {
    panic(err)
}
defer w.Close()

```

### Writing & Syncing

```go
// Append to OS page cache (fast, not yet durable)
err := w.Append([]byte("transaction_data"))

// Force bits to disk (slow, durable)
err = w.Sync()

// Or do both atomically
err = w.AppendAndSync([]byte("critical_op"))

```

### Recovery & Conflict Resolution

```go
// Rebuild state on restart
entries, err := w.ReadAll()

// Handle Raft conflicts: Delete everything from index 10 onwards
err = w.TruncateFromIndex(10)

```

## Implementation Details

### Consistency Model

The library maintains an in-memory `EntryIndex` (a slice of offsets). While the log is open, `GetEntry` uses `ReadAt` on the file descriptor. By not using `bufio`, we ensure that the kernel's page cache acts as the single source of truth between the writer and the reader.

### Durability

Durability is achieved by calling `fsync` on the file and the parent directory. Syncing the directory is essential on Linux filesystems to ensure that the file creation itself survives a power loss.

### Safety

The recovery process treats the disk as untrusted. If a checksum fails or a length field exceeds the `MaxEntrySize` configuration, the WAL assumes a crash occurred during a write and truncates the file at the last valid boundary to maintain a clean state.

## Performance

* **Append**: O(1)
* **Read**: O(1) (via in-memory index)
* **Recovery**: O(N) (where N is the number of entries)
