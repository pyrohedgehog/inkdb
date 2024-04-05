package inkdb

import "encoding/binary"

type SplotchKey [4]byte //a 32 bit index string

// lets say the keys are represented in big endian order.
func (a SplotchKey) LessThan(b SplotchKey) bool {
	for i := 0; i < len(a); i++ {
		if a[i] == b[i] {
			continue
		}
		return a[i] < b[i]
	}
	return false
}
func (a SplotchKey) LessOrEqual(b SplotchKey) bool {
	for i := 0; i < len(a); i++ {
		if a[i] == b[i] {
			continue
		}
		return a[i] < b[i]
	}
	return true

}
func (a SplotchKey) GreaterThan(b SplotchKey) bool {
	for i := 0; i < len(a); i++ {
		if a[i] == b[i] {
			continue
		}
		return a[i] > b[i]
	}
	return false
}
func (a SplotchKey) GreaterOrEqual(b SplotchKey) bool {
	for i := 0; i < len(a); i++ {
		if a[i] == b[i] {
			continue
		}
		return a[i] > b[i]
	}
	return true
}
func (a SplotchKey) Equal(b SplotchKey) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// generates the next incremental key
func (k SplotchKey) NextKey() SplotchKey {
	return k.Plus(1)
}

// TODO: if i change Splotchkey back to variable length, i need to change this
func (k SplotchKey) Plus(a uint32) SplotchKey {
	//tbh, I'm not entirely sure why I wrote the keys as byte arrays instead of uint32s... Originally i was going to make them more flexible, in length, but decided to change that...
	return SplotchKey(binary.BigEndian.AppendUint32([]byte{}, binary.BigEndian.Uint32(k[:])+a))
}