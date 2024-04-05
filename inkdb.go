package inkdb

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path"

	"go.uber.org/zap/buffer"
)

//lets crack out a main db layer

// this is the top layer called.
type InkDB struct {
	fileStartPoint string
	inkSacks       map[string]*inkSack //map[tableName]->sacks
	inkColors      map[string]any
}

func NewInkDB(storing string) (*InkDB, error) {
	idb := &InkDB{
		fileStartPoint: storing,
		inkSacks:       map[string]*inkSack{},
		inkColors:      map[string]any{},
	}
	if err := idb.loadTables(); err != nil {
		return nil, err
	}
	//what to work on.
	//find any files associated to itself.
	//be able to add tables
	//be able to get from tables
	//get will be the keyword to get a selection. It will be get <from> <to>, and only return in order.
	//put will be the keyword to append a new piece of data to a specific table
	//place will be to place into a specific table with a specific key (if ever, for some reason is needed)
	//kick will act the same as get, but in reverse. removing all that data from the DB.

	//the idb will need it's own file folder structure (to start)
	//it will need to be able to take in structs, and possible an encoder input.
	//depending how energetic i feel, i might even work on adding an any support point for table type input.
	return idb, nil
}

// loads the minimal amount based on the files found around itself
func (ink *InkDB) loadTables() error {
	if _, err := os.Stat(path.Join(ink.fileStartPoint, "inksacks")); err != nil {
		//no folder found there
		if err = os.Mkdir(ink.fileStartPoint, 0777); err != nil {
			return err
		}
		if err = os.Mkdir(path.Join(ink.fileStartPoint, "inksacks"), 0777); err != nil {
			return err
		}
	} else {
		//there already was a file system.
		files, _ := os.ReadDir(path.Join(ink.fileStartPoint, "/inksacks/"))
		ink.inkSacks = map[string]*inkSack{}
		for _, filePath := range files {
			sack, err := NewInkSack(filePath.Name())
			if err != nil {
				return err
			}
			ink.inkSacks[filePath.Name()] = sack
		}
	}
	return nil
}
func (ink *InkDB) NewTable(name string, of any) error {
	if ink.inkSacks[name] != nil {
		return fmt.Errorf("inksack already exists")
	}
	newSack, err := NewInkSack(path.Join(ink.fileStartPoint, "/inksacks/", name))

	if err != nil {
		return err
	}
	ink.inkSacks[name] = newSack
	ink.inkColors[name] = of
	return nil
}

// automatically generate a key, and append the item to the given inksack
func (ink *InkDB) Append(inksack string, item any) error {
	if ink.inkSacks[inksack] == nil {
		return fmt.Errorf("no inksack(table) found under %v", inksack)
	}
	buffer := buffer.NewPool().Get()
	enc := gob.NewEncoder(buffer)
	if err := enc.Encode(item); err != nil {
		return err
	}
	return ink.inkSacks[inksack].AutoAppend(buffer.Bytes())
}

// get from <inksack> with values <from>, <to>
func (ink *InkDB) Get(inksack string, from, to SplotchKey) ([]any, []SplotchKey, error) {
	ans, err := ink.GetStored(inksack, from, to)
	if err != nil {
		return nil, nil, err
	}
	outVals := make([]any, len(ans))
	keys := make([]SplotchKey, len(ans))
	for i, val := range ans {
		keys[i] = val.Key
		//handle the decoding.
		buffer := bytes.NewBuffer(val.Value)
		dec := gob.NewDecoder(buffer)
		var value = ink.inkColors[inksack]

		if err := dec.Decode(value); err != nil {
			return nil, nil, err
		}
		//I'm pretty sure this will make sure to copy the value at a low level.
		tmpArr := make([]any, 1)
		copy(tmpArr, []any{value})
		outVals[i] = tmpArr[0]
	}

	return outVals, keys, nil
}

// get the stored items from an inksack, from <from>, to <to>. Returns any error encountered.
func (ink *InkDB) GetStored(inksack string, from, to SplotchKey) ([]storedItem, error) {
	if ink.inkSacks[inksack] == nil {
		return nil, fmt.Errorf("no inksack(table) found under %v", inksack)
	}
	ans, err := ink.inkSacks[inksack].GetAll(from, to)
	if err != nil {
		return nil, err
	}
	return ans, nil
}

// Commit is what actually saves the changes to the disc!
func (ink *InkDB) Commit() error {
	for _, inksack := range ink.inkSacks {
		err := inksack.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

// this is the bottom most layer. The item that is actually written to disc.
type storedItem struct {
	Key   SplotchKey
	Value []byte
}
