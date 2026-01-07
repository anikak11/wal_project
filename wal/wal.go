package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

func New(filePath string) (*WAL, error) {
	return NewWithConfig(filePath, &Config{
		MaxEntrySize:   DefaultMaxEntrySize,
		MaxSegmentSize: DefaultMaxSegmentSize,
	})
}

func NewWithConfig(filePath string, config *Config) (*WAL, error) {
	dirPath := filepath.Dir(filePath)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	// Sync directory for durability
	dir, _ := os.Open(dirPath)
	dir.Sync()
	dir.Close()

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		file:      file,
		filePath:  filePath,
		dirPath:   dirPath,
		config:    config,
		index:     make([]EntryIndex, 0),
		nextIndex: 1,
	}

	if err := w.initialize(); err != nil {
		file.Close()
		return nil, err
	}
	return w, nil
}

func (w *WAL) Append(data []byte) error {
	if atomic.LoadInt32(&w.closed) == 1 { return ErrWALClosed }
	if data == nil { return fmt.Errorf("data is nil") }
	if uint32(len(data)) > w.config.MaxEntrySize { return ErrEntryTooLarge }

	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	entry := &WALEntry{Type: EntryTypeData, Data: data}
	entry.Checksum = computeChecksum(entry.Type, data)
	encoded := entry.encode()

	n, err := w.file.Write(encoded)
	if err != nil { return err }

	entryOffset := w.offset
	w.offset += int64(n)

	w.indexMu.Lock()
	w.index = append(w.index, EntryIndex{Index: w.nextIndex, Offset: entryOffset})
	w.indexMu.Unlock()

	w.nextIndex++
	atomic.AddInt64(&w.metrics.WriteCount, 1)
	atomic.AddInt64(&w.metrics.BytesWritten, int64(n))
	return nil
}

func (w *WAL) Sync() error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	err := w.file.Sync()
	atomic.AddInt64(&w.metrics.SyncCount, 1)
	atomic.StoreInt64(&w.metrics.LastSyncTime, time.Now().UnixNano())
	return err
}

func (w *WAL) GetEntry(index uint64) ([]byte, error) {
	w.indexMu.RLock()
	if index == 0 || index > uint64(len(w.index)) {
		w.indexMu.RUnlock()
		return nil, fmt.Errorf("index out of bounds")
	}
	info := w.index[index-1]
	w.indexMu.RUnlock()

	w.readMu.RLock()
	defer w.readMu.RUnlock()
	entry, _, err := w.readEntryAt(info.Offset)
	return entry.Data, err
}

func (w *WAL) AppendAndSync(data []byte) error {
	if err := w.Append(data); err != nil {
		return err
	}
	return w.Sync()
}

func (w *WAL) LastIndex() uint64 {
	w.indexMu.RLock()
	defer w.indexMu.RUnlock()
	if len(w.index) == 0 {
		return 0
	}
	return w.index[len(w.index)-1].Index
}

func (w *WAL) ReadAll() ([][]byte, error) {
	w.indexMu.RLock()
	indices := make([]EntryIndex, len(w.index))
	copy(indices, w.index)
	w.indexMu.RUnlock()

	results := make([][]byte, 0, len(indices))
	w.readMu.RLock()
	defer w.readMu.RUnlock()

	for _, idx := range indices {
		entry, _, err := w.readEntryAt(idx.Offset)
		if err != nil {
			return nil, fmt.Errorf("failed to read entry at index %d: %w", idx.Index, err)
		}
		results = append(results, entry.Data)
	}

	return results, nil
}

func (w *WAL) Close() error {
	if !atomic.CompareAndSwapInt32(&w.closed, 0, 1) { return nil }
	w.Sync()
	return w.file.Close()
}