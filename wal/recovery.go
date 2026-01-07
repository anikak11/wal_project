package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
)

func (w *WAL) initialize() error {
	stat, _ := w.file.Stat()
	if stat.Size() == 0 {
		buf := make([]byte, WALFileHeaderSize)
		binary.BigEndian.PutUint32(buf[0:4], WALMagicNumber)
		binary.BigEndian.PutUint32(buf[4:8], WALVersion)
		w.file.Write(buf)
		w.file.Sync()
		w.offset = int64(WALFileHeaderSize)
		return nil
	}
	return w.recover()
}

func (w *WAL) recover() error {
	header := make([]byte, WALFileHeaderSize)
	if _, err := w.file.ReadAt(header, 0); err != nil { return err }
	if binary.BigEndian.Uint32(header[0:4]) != WALMagicNumber { return ErrCorruptedWAL }

	offset := int64(WALFileHeaderSize)
	nextIdx := uint64(1)

	for {
		_, size, err := w.readEntryAt(offset)
		if err != nil {
			if err != io.EOF { w.truncate(offset) }
			break
		}
		w.index = append(w.index, EntryIndex{Index: nextIdx, Offset: offset})
		offset += size
		nextIdx++
	}
	w.offset = offset
	w.nextIndex = nextIdx
	w.file.Seek(w.offset, 0)
	return nil
}

func (w *WAL) readEntryAt(offset int64) (*WALEntry, int64, error) {
	headBuf := make([]byte, EntryHeaderSize)
	if _, err := w.file.ReadAt(headBuf, offset); err != nil { return nil, 0, err }

	dLen := binary.BigEndian.Uint32(headBuf[1:5])
	if dLen > w.config.MaxEntrySize { return nil, 0, ErrEntryTooLarge }

	data := make([]byte, dLen)
	if _, err := w.file.ReadAt(data, offset+EntryHeaderSize); err != nil { return nil, 0, err }

	entry := &WALEntry{Type: headBuf[0], Data: data, Checksum: binary.BigEndian.Uint32(headBuf[5:9])}
	if computeChecksum(entry.Type, data) != entry.Checksum {
		atomic.AddInt64(&w.metrics.Corruptions, 1)
		return nil, 0, ErrCorruptedWAL
	}
	return entry, int64(EntryHeaderSize + dLen), nil
}

func (w *WAL) truncate(offset int64) error {
	if err := w.file.Truncate(offset); err != nil { return err }
	return w.file.Sync()
}

// TruncateFromIndex removes all entries from the given index onwards.
// index is 1-based. If index is 5, entries 5, 6, 7... are deleted.
// This is essential for Raft when a follower must resolve log conflicts.
func (w *WAL) TruncateFromIndex(index uint64) error {
	if atomic.LoadInt32(&w.closed) == 1 {
		return ErrWALClosed
	}

	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	w.indexMu.Lock()
	defer w.indexMu.Unlock()

	// 1. Validation: Ensure index is within the current log range
	if index == 0 || index > uint64(len(w.index)) {
		return fmt.Errorf("invalid truncate index: %d (current log size: %d)", index, len(w.index))
	}

	// 2. Find the file offset of the entry to be removed
	// Since index is 1-based, index-1 is the slice position.
	truncateOffset := w.index[index-1].Offset

	// 3. Physical Truncation
	// This removes the data from the underlying storage.
	if err := w.file.Truncate(truncateOffset); err != nil {
		return fmt.Errorf("failed to physically truncate file: %w", err)
	}

	// 4. Force Sync
	// Critical: Ensure the file system metadata (new size) is durable.
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync after truncation: %w", err)
	}

	// 5. Update In-Memory State
	w.index = w.index[:index-1] // Remove indices from memory
	w.nextIndex = index         // Set next index to the one we just cleared
	w.offset = truncateOffset   // Move write pointer back

	// 6. Reset File Pointer
	// Required because Append uses w.file.Write()
	if _, err := w.file.Seek(w.offset, 0); err != nil {
		return fmt.Errorf("failed to seek to new end: %w", err)
	}

	return nil
}