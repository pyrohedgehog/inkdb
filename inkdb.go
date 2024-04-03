package inkdb

import (
	"encoding/binary"
)

//lets crack out a main db layer

// this is the top layer called.
type InkDB struct {
	inkSacks map[string]inkSack //map[tableName]->sacks
}

func NewInkDB(storing string) (*InkDB, error) {
	idb := &InkDB{}
	//TODO: listed
	//find any files associated to itself.
	//be able to add tables
	//be able to get from tables
	//get will be the keyword to get a selection. It will be get <from> <to>, and only return in order.
	//put will be the keyword to append a new piece of data to a specific table
	//place will be to place into a specific table with a specific key (if ever, for some reason is needed)
	//kick will act the same as get, but in reverse. removing all that data from the DB.
	return idb, nil
}

// this is the bottom most layer. The item that is actually written to disc.
type storedItem struct {
	Key   SplotchKey
	Value []byte
}

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
