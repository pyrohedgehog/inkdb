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

func (is *inkSack) SearchForSplotch(lt func(a storedItem) bool) (*inkSplotch, error) {
	if len(is.inkSplotches) <= 1 {
		return is.inkSplotches[0], nil
	}
	//look through all of the splotches, if the max value is less than, and the min value is not less than, thats our splotch.
	size := len(is.inkSplotches) - 1
	start := 0
	for {
		splotch := is.inkSplotches[start+size]
		min, max := splotch.GetEnds()
		minData, err := splotch.Get(min)
		if err != nil {
			return nil, err
		}
		maxData, err := splotch.Get(max)
		minItem := storedItem{Key: min, Value: minData}
		maxItem := storedItem{Key: max, Value: maxData}
		if err != nil {
			return nil, err
		}
		if (lt(maxItem) || max.Equal(SplotchKey{})) && !lt(minItem) {
			//we've found it!
			return splotch, nil
		}
		if lt(minItem) {
			//its smaller than this halfway point
			size = size / 2
		} else {
			//its larger than this halfway point
			start += size
		}
	}
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
		splotch.headings.LargestKey = is.inkSplotches[len(is.inkSplotches)-1].headings.LargestKey.NextKey()
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
