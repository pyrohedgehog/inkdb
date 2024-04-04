package inkdb

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

// tries to make sure that the test env is setup, and empty
func getInkTestFile() string {
	dir, _ := os.Getwd()
	testFolder := path.Join(dir, "/testLocation/")

	if err := os.RemoveAll(testFolder); err != nil {
		panic(err)
	}
	return testFolder
}
func TestInkDB(t *testing.T) {
	folder := getInkTestFile()
	ink, err := NewInkDB(folder)
	if err != nil {
		t.Fatal(err)
	}
	type testType struct {
		Name string
	}

	if err := ink.NewTable("legs", &testType{}); err != nil {
		t.Fatal(err)
	}
	if err := ink.Append("legs", &testType{"bob"}); err != nil {
		t.Fatal(err)
	}
	vals, keys, err := ink.Get("legs", SplotchKey{0, 0, 0, 0}, SplotchKey{1})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, &testType{"bob"}, vals[0].(*testType))
	assert.Equal(t, SplotchKey{}.NextKey(), keys[0])

}
