package inkdb

import (
	"fmt"
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
func TestInkDBAlreadyFilled(t *testing.T) {
	folder := getInkTestFile()
	ink, err := NewInkDB(folder)
	if err != nil {
		t.Fatal(err)
	}
	tableName := "table"
	MaxRowsPerSplotch = 10 //just lower this for ease of use.
	itemsAdded := 2 * MaxRowsPerSplotch
	//just make 10 splotches

	//no table made yet, so this should throw an error
	assert.Error(t, ink.Append(tableName, generateTestableObject(0)))

	if err := ink.NewTable(tableName, &testableObject{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < itemsAdded; i++ {
		ink.Append(tableName, generateTestableObject(i))
	}
	ink.Commit()

	//check that it can get this from just memory.
	for i := 1; i < itemsAdded; i++ {
		//we dont need to check all of them, just check a good few.
		// itemIndex := rand.Intn(itemsAdded)
		itemIndex := i

		//get by the key
		goalKey := SplotchKey{}.Plus(itemIndex)
		items, keys, err := ink.Get(tableName, goalKey, goalKey)
		if err != nil {
			t.Fatal(err)
		}
		if !assert.Equal(t, 1, len(items), "wrong number of things returned!") {
			//lets get some more info
			t.Logf("Index:%v", itemIndex)
			t.Logf("key:%v", goalKey)
		}
		assert.Equal(t, goalKey, keys[0])
		assert.Equal(t, generateTestableObject(itemIndex-1), items[0].(*testableObject))
	}
}

func BenchmarkInkDBCommit(b *testing.B) {
	folder := getInkTestFile()
	ink, err := NewInkDB(folder)
	if err != nil {
		b.Fatal(err)
	}
	tableName := "table"

	//no table made yet, so this should throw an error

	if err := ink.NewTable(tableName, &testableObject{}); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		ink.Append(tableName, generateTestableObject(i))
	}
	b.ResetTimer()
	b.StartTimer()
	err = ink.Commit()
	b.StopTimer()
	if err != nil {
		b.Fatal(err)
	}
}
func BenchmarkInkDBFullUse(b *testing.B) {
	folder := getInkTestFile()
	ink, err := NewInkDB(folder)
	if err != nil {
		b.Fatal(err)
	}

	//no table made yet, so this should throw an error
	for i := 0; i < b.N; i++ {
		tableName := fmt.Sprintf("table:%#04x", i)
		if err := ink.NewTable(tableName, &testableObject{}); err != nil {
			b.Fatal(err)
		}
		for i := 0; i < b.N; i++ {
			ink.Append(tableName, generateTestableObject(i))
		}
	}
}

// a type used within inkdb for testing.
type testableObject struct {
	StringVal string
	IntVal    int
}

func generateTestableObject(seed int) *testableObject {
	to := &testableObject{
		StringVal: fmt.Sprintf("storedString: %v", seed),
		IntVal:    seed,
	}
	return to
}
