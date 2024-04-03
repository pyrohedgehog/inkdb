package inkdb

//lets crack out a main db layer

// this is the top layer called.
type InkDB struct {
	inkSacks map[string]inkSack //map[tableName]->sacks
}

// this is per clustering of splotches. EG, one per stored table.
type inkSack struct {
	folderLocation string //what folder are the splotches in
	inkSplotches   []inkSplotch
}

// all of the item at the top of the file.
type fileHeadings struct {
	LargestKey  SplotchKey
	LinesStored int
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
func (k SplotchKey) NextKey() SplotchKey {
	carriedOne := true
	out := SplotchKey{}
	for i := len(k) - 1; i != 0; i-- {
		by := k[i]
		if carriedOne {
			//we need to add one to this value.
			if uint8(by) == 255 {
				out[i] = 0
			} else {
				carriedOne = false
				out[i] = by + 1
			}
		} else {
			out[i] = by
		}
	}
	return out
}
