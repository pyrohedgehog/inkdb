package inkdb

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplotchKeys(t *testing.T) {
	// allKeys := []SplotchKey{}
	MaxKeySizedInt := 1 << 23
	//the largest size my computer will handle quickly.
	//if you have a really well multi threaded system, you could slap a go routine on each of the testKeysInOrder, and possible get a slight improvement in time.
	lastKey := SplotchKey{}
	for i := 0; i < MaxKeySizedInt; i++ {
		//step one, generate a key
		// key := SplotchKey{}
		//step two, by hand, generate the next key.
		newKey := SplotchKey{}
		binary.BigEndian.PutUint64(newKey[:], binary.BigEndian.Uint64(lastKey[:])+uint64(1))

		//step three, test the two against each other.
		testKeysInOrder(t, lastKey, newKey)

		//check key.NextKey works
		nextKey := lastKey.NextKey()
		testKeysInOrder(t, lastKey, nextKey)
		assert.Equal(t, newKey, nextKey)

		//check that key,Plus(Val) works
		assert.Equal(t, lastKey, SplotchKey{}.Plus(i))

		lastKey = nextKey
	}
}
func testKeysInOrder(t *testing.T, oldKey, newKey SplotchKey) bool {
	return assert.True(t, oldKey.LessThan(newKey)) &&
		assert.True(t, newKey.GreaterThan(oldKey)) &&
		assert.False(t, oldKey.Equal(newKey)) &&
		assert.Equal(t, uint64(1), binary.BigEndian.Uint64(newKey[:])-binary.BigEndian.Uint64(oldKey[:]))
}
