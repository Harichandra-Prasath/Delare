package protocol

import (
	"encoding/binary"
	"hash/crc32"
)

const HEADER_SIZE = 20

func EncodeLog(buf []byte, timestamp uint64, containerID uint16) {
	buf[0] = 0xDE
	buf[1] = 0xAD

	binary.BigEndian.PutUint32(buf[2:6], uint32(len(buf)))

	binary.BigEndian.PutUint64(buf[6:14], timestamp)

	binary.BigEndian.PutUint16(buf[14:16], containerID)

	checksum := crc32.ChecksumIEEE(buf[20:])
	binary.BigEndian.PutUint32(buf[16:20], checksum)
}
