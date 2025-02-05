package heap

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

func calculateCRC(checkSumLocation []byte, buffer []byte) {
	chksum1 := crc32.ChecksumIEEE(buffer)
	binary.BigEndian.PutUint32(checkSumLocation, chksum1)
}

func compareCRC(buffer1 []byte, buffer2 []byte) bool {
	if buffer1[0] != buffer2[0] || buffer1[1] != buffer2[1] || buffer1[2] != buffer2[2] || buffer1[3] != buffer2[3] {
		return false
	}
	return true
}

func heapFileName(number int) string {
	return fmt.Sprintf("%s%s%d", heapFileNamePrefix, heapfileNameSepparate, number)
}
