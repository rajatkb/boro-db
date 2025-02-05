package file

import (
	"encoding/binary"
	"hash/crc32"
)

func calculateCRC(checkSumLocation []byte, buffer []byte) {
	chksum1 := crc32.ChecksumIEEE(buffer)
	binary.BigEndian.PutUint32(checkSumLocation, chksum1)
}
