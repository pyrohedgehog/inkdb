package inkdb

import (
	"encoding/gob"
	"errors"
	"io"
	"os"
)

// this is kept as a variable instead of a constant for the sake of testing. Benchmarks scale each splotch larger than I might otherwise want.
var MaxRowsPerSplotch int = 65535 //2^16-1

// all of the item at the top of the file.
type fileHeadings struct {
	LargestKey  SplotchKey
	LinesStored int
}

// this is per folder. Holds all of the stored items, as well as their values. Can be generated from a file.
type inkSplotch struct {
	fileLocation   string //what file is this stored in
	storedItems    []*storedItem
	smallestKey    SplotchKey //the smallest key added to this
	headings       fileHeadings
	unsavedItems   []*storedItem
	hasFullyLoaded bool
}

func NewInkSplotch(fileLocation string) (*inkSplotch, error) {
	splotch := &inkSplotch{
		fileLocation: fileLocation,
	}
	//check that the file already exists.
	if _, err := os.Stat(fileLocation); errors.Is(err, os.ErrNotExist) {
		// it doesn't exist.
		return splotch, splotch.SaveToFile()
	} else if err == nil {
		//file already exists. So we will try to load from it
		return splotch, splotch.PartialLoad()
	} else {
		//some unknown error occurred
		return nil, err
	}
}

func (splotch *inkSplotch) IsFull() bool {
	return splotch.headings.LinesStored >= MaxRowsPerSplotch
}

// get the smallest and largest end currently stored
func (splotch *inkSplotch) GetEnds() (smallest, largest SplotchKey) {
	return splotch.smallestKey, splotch.headings.LargestKey
}
func (splotch *inkSplotch) AutoAppend(value []byte) error {
	if splotch.headings.LinesStored == MaxRowsPerSplotch {
		return ErrSplotchFull
	}
	newKey := splotch.headings.LargestKey.NextKey()
	splotch.headings.LargestKey = newKey
	fullData := storedItem{
		Key:   newKey,
		Value: value,
	}
	splotch.unsavedItems = append(splotch.unsavedItems, &fullData)
	splotch.storedItems = append(splotch.storedItems, &fullData)
	splotch.headings.LinesStored++
	if splotch.headings.LinesStored == 1 {
		splotch.smallestKey = newKey
	}
	return nil
}
func (splotch *inkSplotch) Append(fullData storedItem) error {
	if splotch.IsFull() {
		return ErrSplotchFull
	}
	if splotch.headings.LargestKey.GreaterOrEqual(fullData.Key) {
		return ErrSplotchRangeExceeded
	}
	splotch.headings.LargestKey = fullData.Key
	splotch.unsavedItems = append(splotch.unsavedItems, &fullData)
	splotch.storedItems = append(splotch.storedItems, &fullData)
	splotch.headings.LinesStored++
	if splotch.headings.LinesStored == 1 {
		splotch.smallestKey = fullData.Key
	}
	return nil
}
func (splotch *inkSplotch) Get(by SplotchKey) ([]byte, error) {
	found, err := splotch.GetStoredItem(by)
	return found.Value, err
}
func (splotch *inkSplotch) GetStoredItem(by SplotchKey) (storedItem, error) {
	if by.GreaterThan(splotch.headings.LargestKey) || by.LessThan(splotch.smallestKey) {
		return storedItem{}, ErrSplotchRangeExceeded
	}
	if !splotch.hasFullyLoaded {
		if err := splotch.FullyLoad(); err != nil {
			return storedItem{}, err
		}
	}
	//it should be in here. So lets write a bit of a binary search function
	found, err := splotch.SearchFor(func(a storedItem) bool {
		return by.LessThan(a.Key)
	}, func(a storedItem) bool {
		return by.Equal(a.Key)
	})

	return found, err
}
func (splotch *inkSplotch) SearchFor(lt func(a storedItem) bool, eq func(a storedItem) bool) (storedItem, error) {
	if splotch.headings.LinesStored == 0 {
		//checks if it is empty. If it is, it cannot have anything.
		return storedItem{}, ErrSplotchRangeExceeded
	}
	zeroVal := *splotch.storedItems[0]
	lastVal := *splotch.storedItems[len(splotch.storedItems)-1]
	//the edges can cause errors, so we check them right away
	if eq(zeroVal) {
		return zeroVal, nil
	}
	if eq(lastVal) {
		return lastVal, nil
	}
	if lt(zeroVal) ||
		!lt(lastVal) {
		//its outside of our range. because its not the first or last one, and it's larger than the last(largest) one, and smaller than the first(smallest) one.
		return storedItem{}, ErrSplotchRangeExceeded
	}
	//it should be within here.
	size := len(splotch.storedItems) / 2
	start := size
	for {
		if eq(*splotch.storedItems[start]) {
			//then we've found it.
			return *splotch.storedItems[start], nil
		}
		size = size / 2
		if size == 0 {
			size = 1
		}
		if lt(*splotch.storedItems[start]) {
			//its smaller than this half
			start -= size
		} else {
			//its larger than this half point
			start += size
		}
	}
}
func (splotch *inkSplotch) PartialLoad() error {
	if _, err := os.Stat(splotch.fileLocation); err != nil {
		//the file does not exist
		return err
	}
	//if the file already exists, if it does, load this data. if not, create a new blank file for it.
	f, err := os.OpenFile(splotch.fileLocation, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}

	dec := gob.NewDecoder(f)
	if err := dec.Decode(&splotch.headings); err != nil {
		return err
	}

	var smallest storedItem
	err = dec.Decode(&smallest)
	if err == io.EOF {
		//then there is no smallest yet, and we can return
		return nil
	}
	if err != nil {
		//a catch case for any other errors
		return err
	}
	splotch.smallestKey = smallest.Key
	splotch.storedItems = []*storedItem{&smallest}

	//the file should consist of the largest key. Then line by line each item.
	//then that data should be put into splotch
	return nil
}
func (splotch *inkSplotch) FullyLoad() error {
	if _, err := os.Stat(splotch.fileLocation); err != nil {
		//the file does not exist
		return err
	}
	//if the file already exists, if it does, load this data. if not, create a new blank file for it.
	f, err := os.OpenFile(splotch.fileLocation, os.O_RDONLY, 0666) //TODO: fix the permissions to be closer to what we need...
	if err != nil {
		return err
	}

	dec := gob.NewDecoder(f)
	if err := dec.Decode(&splotch.headings); err != nil {
		return err
	}
	splotch.storedItems = []*storedItem{}
	for {
		var nextValue storedItem
		err := dec.Decode(&nextValue)
		if err == io.EOF {
			//we've read every item from the file.
			break
		}
		if err != nil {
			//we've hit an unexpected error
			return err
		}
		splotch.storedItems = append(splotch.storedItems, &nextValue)
	}
	//we now need to re-append all of the added values we already had (if any), and update the largest value in the header again.
	splotch.storedItems = append(splotch.storedItems, splotch.unsavedItems...)
	if len(splotch.unsavedItems) != 0 {
		splotch.headings.LargestKey = splotch.storedItems[len(splotch.storedItems)-1].Key
	}
	splotch.hasFullyLoaded = true
	return nil
}
func (splotch *inkSplotch) SaveToFile() error {
	//first, we write the largest value we've found so far.
	f, err := os.OpenFile(splotch.fileLocation, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	if err := enc.Encode(&splotch.headings); err != nil {
		return err
	}
	_, err = f.Seek(0, 2) //go to the end of the file
	if err != nil {
		return err
	}
	for _, item := range splotch.unsavedItems {

		if err := enc.Encode(&item); err != nil {
			return err
		}
	}
	splotch.unsavedItems = []*storedItem{}

	return f.Close()
}
func (splotch *inkSplotch) GetAll(from, to SplotchKey) ([]storedItem, error) {
	if from.GreaterThan(splotch.headings.LargestKey) || to.LessThan(splotch.smallestKey) {
		//outside our range, no need to care.
		return nil, ErrSplotchRangeExceeded
	}
	if !splotch.hasFullyLoaded {
		if err := splotch.FullyLoad(); err != nil {
			return nil, err
		}
	}
	//TODO: this can be sped up by checking first if the range would fully contain this, start within but go on, start outside but finish within, or if it is fully contained, and handle it from there.
	//if this is fully contained, then just return all items.
	//if it just starts/stops here, find that point, and take the rest.
	//if it's contained within this, find the start and end, and return that portion.
	startIndex := 0
	if from.GreaterThan(splotch.smallestKey) {
		//it starts somewhere within us.
		size := len(splotch.storedItems) / 2
		startIndex = size
		for {
			//half our size each time it's in the lower half
			focalKey := splotch.storedItems[startIndex].Key
			if focalKey.Equal(from) {
				break
			}
			size = size / 2
			if size == 0 {
				size = 1
			}
			if focalKey.GreaterThan(from) {
				//then we need to check the lower part
				startIndex -= size
			} else {
				startIndex += size
			}
		}
	}
	endIndex := len(splotch.storedItems) - 1
	if to.LessThan(splotch.headings.LargestKey) {
		//it ends within us.
		size := (len(splotch.storedItems)) / 2
		start := size

		for {
			//half our size each time it's in the lower half
			focalKey := splotch.storedItems[start].Key
			if focalKey.Equal(to) {
				endIndex = start
				break
			}
			size = size / 2
			if size == 0 {
				size = 1
			}
			if focalKey.GreaterThan(to) {
				//then we need to check the lower part
				start -= size
			} else {
				start += size
			}
		}
	}
	foundItems := make([]storedItem, 0, endIndex-startIndex)
	for i := startIndex; i <= endIndex; i++ {
		//just do the iteration so we can convert the type
		foundItems = append(foundItems, *splotch.storedItems[i])
	}
	return foundItems, nil
}
