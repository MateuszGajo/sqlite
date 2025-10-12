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
