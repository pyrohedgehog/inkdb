package inkdb

//this is a completely unrelated item to any other piece of code here. It solves a problem sure, but is too general to fit in with any other part.
//it also isnt quite as general as the builtin version (using .compare),

// Search arr for the index of the value. isLessThan(test) should be target<test
func BinarySearch[T any](arr []T, isLessThan, eq func(T) bool) (index int) {
	size := len(arr) / 2
	index = size
	//just throwing this in a normal for loop. I know it should be faster than n/2, but for now this will work.
	for i := 0; i < len(arr)/2; i++ {
		item := arr[index]
		if eq(item) {
			return index
		}
		size /= 2
		if size == 0 {
			size = 1
		}
		if isLessThan(item) {
			//it is less than this item
			index -= size
		} else {
			index += size
		}
	}
	return -1
}
