package inkdb

import (
	"fmt"
	"os"
	"path"
	"testing"
)

// tries to make sure that the test env is setup, and empty
func getSackTestFolder() string {
	dir, _ := os.Getwd()
	testFolder := path.Join(dir, "/testLocation/inkSack/")

	if err := os.RemoveAll(testFolder); err != nil {
		panic(err)
	}
	return testFolder
}
func TestInkSack(t *testing.T) {
	folder := getSackTestFolder()
	MaxRowsPerSplotch = 100
	is, err := NewInkSack(folder)
	if err != nil {
		t.Fatal(err)
	}
	if err := is.AutoAppend([]byte("hi there!")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000000; i++ {
		if err := is.AutoAppend([]byte(fmt.Sprintf("%010v", i))); err != nil {
			t.Fatal(err)
		}
	}
	ans, err := is.SearchForSplotch(func(a storedItem) bool {
		//lets find the splotch that has the key 1234
		return SplotchKey{0, 0, 0, 0, 0, 1, 0, 0}.LessThan(a.Key)
	}, func(si storedItem) bool {
		return SplotchKey{0, 0, 0, 0, 0, 1, 0, 0}.Equal(si.Key)
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ans.Get(SplotchKey{0, 0, 0, 0, 0, 1, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
}
