package main

func parseVarint(buffer []byte) (uint64, []byte) {
	currentOffset := 0
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

	return varint, buffer[currentOffset:]
}

func bigEndianConversion(val any, data []byte) {
	switch v := val.(type) {
	case *uint16:
		for _, b := range data {
			*v = (*v << 8) | uint16(b)
		}

	case *uint32:
		for _, b := range data {
			*v = (*v << 8) | uint32(b)
		}
	case *uint64:
		for _, b := range data {
			*v = (*v << 8) | uint64(b)
		}
	default:
		panic("unsporrted type")
	}
}

func reverse[T any](s []T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
