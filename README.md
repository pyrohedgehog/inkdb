# InkDB
A database system based around the idea of append-only, unchanging data.
### GET \<from> \<to>
Returns all items, in chronological order, from one ink sack (table)
### PUT \<ink-sack> \<data>
Appends a piece of data to a specific ink-sack
### PLACE \<ink-sack> \<key> \<data>
Like append, but will (attempt) to place the data at a specific point. No promises of support though.
### KICK \<from> \<to>
removes all data between those points. Ideal for removing expired data.

# How to use it!
Start by creating a new InkDB (or loading) within your code.
```Go
//something that needs storage

ink, err := inkdb.NewInkDB(path.Join(os.Getwd(), "<where it should live>"))
if err!=nil{
  //or figure out how to handle the error. I write the library, not the rules here.
  panic(err)
}
type placeholderStorage struct{
  ExportableType string
}
ink.NewTable("whatever you want to name it", &placeholderStorage{})
ink.Append("whatever you named it", &palceholderStorage{"hello world"})

```
## Places for improvement

### threading.
I need to add some thread safety, but each query should (ideally) be able to be run in its own thread.

### storage limitations
>[!WARNING]
> Each inksack(table), makes its own folder for storing. There is no check for storage availability. 
currently, each table needs all its contents in the same folder location. If I change that, to allow multiple file locations, we can have redundancy files, as well as tables that take up multiple storage devices.


### support
>[!WARNING]
>I wouldn't even count this as supporting MacOS yet. Use at your own risk.
