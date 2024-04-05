package inkdb

import (
	"fmt"
	"os"
	"path"
	"sort"
)

// this is per clustering of splotches. EG, one per stored table.
type inkSack struct {
	//this items data will always be under /inkSackData
	//the splotch data will be under /splotches/n.txt
	localFilesLocation string //where is this storing it's data.
	inkSplotches       []*inkSplotch
	largestKey         SplotchKey
}

func NewInkSack(localFiles string) (*inkSack, error) {
	is := &inkSack{
		localFilesLocation: localFiles,
	}
	//first, we should setup the file directory system it needs, if it isn't already.
	//I think we'll need to save some states within this, but i'm not actually certain anymore
	//once we have the file structure setup, we should load any data stored already for this inkSack, then load the children splotches
	if err := is.setupFolderStructure(); err != nil {
		return nil, err
	}
	return is, is.LoadChildrenFromDisc()
}
func (is *inkSack) setupFolderStructure() error {
	if _, err := os.Stat(is.localFilesLocation); err != nil {
		//no folder found there
		if err = os.Mkdir(is.localFilesLocation, 0777); err != nil {
			return err
		}
		if err = os.Mkdir(path.Join(is.localFilesLocation, "splotches"), 0777); err != nil {
			return err
		}
	}
	return nil
}
func (is *inkSack) LoadChildrenFromDisc() error {
	if _, err := os.Stat(is.localFilesLocation); err != nil {
		//the file does not exist
		return err
	}

	//figure out what files are inkSplotch files, then load those to a partial state.
	files, _ := os.ReadDir(path.Join(is.localFilesLocation, "/splotches/"))
	is.inkSplotches = make([]*inkSplotch, len(files))
	for i, filePath := range files {
		splotch, err := NewInkSplotch(filePath.Name())
		if err != nil {
			return err
		}
		is.inkSplotches[i] = splotch
	}
	//they should be in order, but just in case.
	sort.Slice(is.inkSplotches, func(i, j int) bool {
		return is.inkSplotches[i].smallestKey.LessThan(is.inkSplotches[j].smallestKey)
	})
	return nil
}
func (is *inkSack) AutoAppend(data []byte) error {
	if len(is.inkSplotches) == 0 {
		if err := is.addSplotch(); err != nil {
			return err
		}
	}
	if is.inkSplotches[len(is.inkSplotches)-1].IsFull() {
		//then we need to generate the next splotch
		if err := is.addSplotch(); err != nil {
			return err
		}
	}
	if err := is.inkSplotches[len(is.inkSplotches)-1].AutoAppend(data); err != nil {
		return err
	}
	is.largestKey = is.inkSplotches[len(is.inkSplotches)-1].headings.LargestKey
	return nil
}

func (is *inkSack) Append(data storedItem) error {
	//check that the key is in an acceptable point.
	//first, find the splotch it belongs to.
	size := len(is.inkSplotches) - 1
	start := 0
	for {
		splotch := is.inkSplotches[start+size]
		min, max := splotch.GetEnds()
		if data.Key.GreaterOrEqual(min) && (data.Key.LessOrEqual(max) || max.Equal(SplotchKey{})) {
			//we found out spot
			return splotch.Append(data)
		}
		if min.LessThan(data.Key) {
			//this min is too high
			size = size / 2
		} else {
			//this min is too low
			start += size
		}
	}
}

func (is *inkSack) SearchForSplotch(lt, eq func(storedItem) bool) (*inkSplotch, error) {
	if len(is.inkSplotches) <= 1 {
		return is.inkSplotches[0], nil
	}
	//look through all of the splotches, if the max value is less than, and the min value is not less than, thats our splotch.
	index := BinarySearch(is.inkSplotches,
		func(splotch *inkSplotch) bool {
			min, _ := splotch.GetEnds()
			minItem, _ := splotch.GetStoredItem(min)
			return lt(minItem)
		},
		func(splotch *inkSplotch) bool {
			min, max := splotch.GetEnds()
			minItem, _ := splotch.GetStoredItem(min)
			maxItem, _ := splotch.GetStoredItem(max)
			return ((lt(maxItem) || SplotchKey{}.Equal(max)) && !lt(minItem)) || //checking that it is within the range
				eq(maxItem) //however, if it is the last item were looking for, then we would never be able to find it by that check

		},
	)
	if index == -1 {
		return nil, ErrSplotchRangeExceeded
	}
	return is.inkSplotches[index], nil
}

// i've written this so many times, i'm amazed i didn't think of this.
// TODO: this should have a better home... Or be replaced with a library import.

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

// add another splotch to follow the last one
func (is *inkSack) addSplotch() error {
	nextFileName := fmt.Sprintf("%v/splotches/s%#08x.txt", is.localFilesLocation, len(is.inkSplotches))
	splotch, err := NewInkSplotch(nextFileName)
	if err != nil {
		return err
	}
	//set the new splotch's smallest key, to one more than the previous ones largest.
	if len(is.inkSplotches) != 0 {
		splotch.headings.LargestKey = is.inkSplotches[len(is.inkSplotches)-1].headings.LargestKey
	}
	is.inkSplotches = append(is.inkSplotches, splotch)
	return nil
}
func (is *inkSack) Commit() error {
	for _, splotch := range is.inkSplotches {
		if err := splotch.SaveToFile(); err != nil {
			return err
		}
	}
	return nil
}
func (is *inkSack) GetAll(from, to SplotchKey) ([]storedItem, error) {
	//TODO: this can be sped up a lot with binary searches... but i'm tired of writing those for today...
	ans := []storedItem{}
	for _, splotch := range is.inkSplotches {
		returned, err := splotch.GetAll(from, to)
		if err != ErrSplotchRangeExceeded && err != nil {
			return nil, err
		} else if err == ErrSplotchRangeExceeded && len(ans) != 0 {
			//then we've hit this error after finding items. therefor we should be at the end of the range
			break
		}
		if err == nil {
			ans = append(ans, returned...)
		}
	}
	return ans, nil
}
