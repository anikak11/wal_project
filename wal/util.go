package wal

import (
	"encoding/binary"
	"hash/crc32"
)

func (e *WALEntry) encode() []byte {
	dLen := uint32(len(e.Data))
	buf := make([]byte, EntryHeaderSize+dLen)
	buf[0] = e.Type
	binary.BigEndian.PutUint32(buf[1:5], dLen)
	binary.BigEndian.PutUint32(buf[5:9], e.Checksum)
	copy(buf[9:], e.Data)
	return buf
}

func computeChecksum(t uint8, data []byte) uint32 {
	crc := crc32.NewIEEE()
	var header [5]byte
	header[0] = t
	binary.BigEndian.PutUint32(header[1:5], uint32(len(data)))
	crc.Write(header[:])
	crc.Write(data)
	return crc.Sum32()
}