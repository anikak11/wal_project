package wal

import (
	"errors"
	"os"
	"sync"
)

const (
	WALMagicNumber = uint32(0x57414C21) // "WAL!"
	WALVersion     = uint32(1)

	EntryTypeData = uint8(1)

	WALFileHeaderSize = 8
	EntryHeaderSize   = 9

	DefaultMaxEntrySize   = 10 * 1024 * 1024  // 10MB
	DefaultMaxSegmentSize = 100 * 1024 * 1024 // 100MB
)

var (
	ErrCorruptedWAL  = errors.New("WAL file is corrupted")
	ErrInvalidEntry  = errors.New("invalid entry format")
	ErrEntryTooLarge = errors.New("entry exceeds maximum size")
	ErrWALClosed     = errors.New("WAL is closed")
)

type WALEntry struct {
	Type     uint8
	Data     []byte
	Checksum uint32
}

type EntryIndex struct {
	Index  uint64
	Offset int64
}

type WALMetrics struct {
	WriteCount   int64
	SyncCount    int64
	BytesWritten int64
	Corruptions  int64
	LastSyncTime int64
}

type Config struct {
	MaxEntrySize   uint32
	MaxSegmentSize int64
}

type WAL struct {
	file     *os.File
	filePath string
	dirPath  string

	writeMu sync.Mutex
	readMu  sync.RWMutex
	indexMu sync.RWMutex

	index     []EntryIndex
	nextIndex uint64

	config  *Config
	offset  int64
	closed  int32
	metrics WALMetrics
}