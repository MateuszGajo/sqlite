package main

func parseVarint(data []byte) (uint64, []byte) {
	var val = uint64(0)
	for range 9 {
		b := data[0]
		data = data[1:]
		val |= (val << 7) | uint64(b&127)

		if b&0x80 == 0 {
			break
		}
	}
	return val, data
}

func parseVarint2(buffer []byte, offset uint16) (uint64, uint16) {
	currentOffset := offset
	var varint uint64

	for range 9 {
		b := buffer[currentOffset]

		varint <<= 7
		varint |= uint64(b & 0b01111111)

		currentOffset++

		if b&0b10000000 == 0 {
			break
		}
	}

	return varint, currentOffset
}
