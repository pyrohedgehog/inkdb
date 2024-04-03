package inkdb

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

// tries to make sure that the test env is setup, and empty
func getSplotchTestFile() string {
	dir, _ := os.Getwd()
	testSplotchFile := path.Join(dir, "/testLocation/test1.txt")
	os.Remove(testSplotchFile)
	return testSplotchFile
}
func TestInkSplotchSaveToFile(t *testing.T) {
	fileLocation := getSplotchTestFile()
	splotch, err := NewInkSplotch(fileLocation)
	if err != nil {
		t.Fatal(err)
	}
	if err := splotch.saveToFile(); err != nil {
		t.Fatal(err)
	}
	if err := splotch.fullyLoad(); err != nil {
		t.Fatal(err)
	}
	if err := splotch.Append(storedItem{
		Key:   SplotchKey{0, 2, 0, 1},
		Value: []byte("first value"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := splotch.AutoAppend([]byte("Hello World!")); err != nil {
		t.Fatal(err)
	}
	if err := splotch.saveToFile(); err != nil {
		t.Fatal(err)
	}
	splotch2, err := NewInkSplotch(fileLocation)
	if err != nil {
		t.Fatal(err)
	}
	//check that it passed the stored values.
	if val, err := splotch2.Get(SplotchKey{0, 2, 0, 1}); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, []byte("first value"), val)
	}
	if val, err := splotch2.Get(SplotchKey{0, 2, 0, 2}); err != nil {
		t.Fatal(err)
	} else {
		assert.Equal(t, []byte("Hello World!"), val)
	}
}
func BenchmarkSplotchGetByHalfOfKeys(b *testing.B) {
	splotch, err := NewInkSplotch(getSplotchTestFile())
	if err != nil {
		b.Fatal(err)
	}
	validKeys := []SplotchKey{}
	MaxRowsPerSplotch = b.N*2 + 1
	for i := 0; i < b.N*2; i++ {
		if err := splotch.AutoAppend([]byte(fmt.Sprintf("Value Of i:%v", i))); err != nil {
			b.Fatal(err)
		}
		validKeys = append(validKeys, splotch.headings.LargestKey)
	}

	//put the keys into a random order.
	for i := range validKeys {
		j := rand.Intn(i + 1)
		validKeys[i], validKeys[j] = validKeys[j], validKeys[i]
	}
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		_, err := splotch.Get(validKeys[i])
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSplotchStoring(b *testing.B) {
	splotch, err := NewInkSplotch(getSplotchTestFile())
	if err != nil {
		b.Fatal(err)
	}
	MaxRowsPerSplotch = b.N + 1
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		data := []byte(fmt.Sprintf("Value Of i:%v", i))
		b.StartTimer()
		if err := splotch.AutoAppend(data); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func BenchmarkSplotchStoringToDisc(b *testing.B) {
	splotch, err := NewInkSplotch(getSplotchTestFile())
	if err != nil {
		b.Fatal(err)
	}
	MaxRowsPerSplotch = b.N + 1
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		data := []byte(fmt.Sprintf("Value Of i:%v", i))
		if err := splotch.AutoAppend(data); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := splotch.saveToFile(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}
func BenchmarkSplotchStoringBulkToDisc(b *testing.B) {
	splotch, err := NewInkSplotch(getSplotchTestFile())
	if err != nil {
		b.Fatal(err)
	}
	MaxRowsPerSplotch = b.N + 1
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		data := []byte(fmt.Sprintf("Value Of i:%v", i))
		if err := splotch.AutoAppend(data); err != nil {
			b.Fatal(err)
		}
		if i%100 == 0 {
			b.StartTimer()
			if err := splotch.saveToFile(); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	}
}
