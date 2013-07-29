/*
    Define a wrapper around an operating system file, including its methods.
    This forms part of the "IO read infrastructure" which consists of the GenericRecord container, the Process and ReadRecord functions here, and the various processor functions elsewhere.  When there are lots of processor functions, this approach simplifies the logic and reduces code duplication, but there is double handling of record data in each processor which is not optimal.
    A File wraps an os.File with some other properties, including locks.  This locking requires that we maintain a map of file paths to Files, and that an existing File be used if available when opening a file.
*/
package logbase

import (
	"os"
    "io"
    "bytes"
    //"bufio"
    "encoding/binary"
    "sync"
    "fmt"
)

const (
    DEFAULT_FILEMODE        os.FileMode = 0766 // octal with leading zero
    LOCKFILE_FORMAT         string = "lock.%s.%s" // type, filename
)

const (
    LOCK_WHILE_WRITING = iota
    LOCK_WHILE_READING = iota
)

var fileCounter int = 0 // for debugging only

// Wrap an os file with a current pointer.
type File struct {
    id      int
	gofile  *os.File
    path    string // path name used to open file
    rwmu    *sync.RWMutex
    debug   *DebugLogger
    isOpen  bool // Its ok to have multiple opens of same gofile
}

func NewFile() *File {
    return &File{
        rwmu: new(sync.RWMutex),
    }
}

// Map of all files managed by the Logbase.  This allows us to keep a single RWMutex
// associated with each file.
type FileRegister struct {
    files   map[string]*File
}

// Init new file register.
func NewFileRegister() *FileRegister {
	return &FileRegister{files: make(map[string]*File)}
}

// Open a new or existing File for read/write access.
func (lbase *Logbase) GetFile(fpath string) (file *File, err error) {
    // Use existing File if present
    file, present := lbase.files[fpath]
    if present {return}

    file = NewFile()
    file.id = fileCounter
    fileCounter++
    file.path = fpath
    file.debug = lbase.debug
    lbase.files[fpath] = file

    err = file.Touch()
	return
}

func OpenFile(path string) (gfile *os.File, err error) {
    return os.OpenFile(
        path,
        os.O_CREATE |
        os.O_APPEND |
        os.O_RDWR,
        DEFAULT_FILEMODE)
}

// A tailored file opener for full create/append/rw.
func (file *File) Open() (err error) {
    var gfile *os.File
    gfile, err = OpenFile(file.path)
    if err == nil {
        file.gofile = gfile
        file.isOpen = true
    }
    return
}

func (file *File) Close() (err error) {
    err = file.gofile.Close()
    if err == nil {file.isOpen = false}
    return
}

// If file does not exist, create it.
func (file *File) Touch() error {
    _, err := os.Stat(file.path)
    if os.IsNotExist(err) {
        err2 := file.Open()
        if err2 == nil {
            file.Close()
        } else {
            return err2
        }
    } else if err != nil {return err}
    return nil
}

// Returns the current file position.
func (file *File) Here() (LBUINT, error) {
    seek, err := file.gofile.Seek(0, os.SEEK_CUR)
    pos := AsLBUINT(int(seek))
    if err != nil {return pos, err}
    return pos, nil
}

// Go to location in file relative to start.
func (file *File) Goto(i LBUINT) (LBUINT, error) {
    seek, err := file.gofile.Seek(int64(i), os.SEEK_SET)
    pos := AsLBUINT(int(seek))
    if err != nil {return pos, err}
    return pos, nil
}

// Jump to location in file relative to current position.
func (file *File) JumpFromHere(j LBUINT) (LBUINT, error) {
    seek, err := file.gofile.Seek(int64(j), os.SEEK_CUR)
    pos := AsLBUINT(int(seek))
    if err != nil {return pos, err}
    return pos, nil
}

// Jump to location in file relative to the end.
func (file *File) JumpFromEnd(j LBUINT) (LBUINT, error) {
    seek, err := file.gofile.Seek(int64(j), os.SEEK_END)
    pos := AsLBUINT(int(seek))
    if err != nil {return pos, err}
    return pos, nil
}

// Allow looping through a file to be separated from processing of its
// records.
type Processor func(rec *GenericRecord) error

// Process the log file using the given function.
func (file *File) Process(process Processor, rectype int, needDataVal bool) (err error) {
    file.Open()
    defer file.Close()
    var rec *GenericRecord
    var pos LBUINT = 0
	for {
        rec, pos, err = file.ReadRecord(pos, rectype, needDataVal)
	    if err != nil {break}
        err = process(rec)
        if err != nil {break}
	}
    if err == io.EOF {err = nil}
    return
}

// Read a record from the gofile, including the value depending on readDataVal.
func (file *File) ReadRecord(pos LBUINT, rectype int, readDataVal bool) (rec *GenericRecord, newpos LBUINT, err error) {
    rec = NewGenericRecord()
    // Key size
    size := LBUINT(ParamSize(rec.ksz))
	err = file.ReadIntoParam(pos, size, &rec.ksz, "keysize") // implicitely moves position
    if err != nil {return}

    pos += size
    file.Goto(pos)

    // Does this record type have a generic value size?
    var readvsz = DoReadDataValueSize[rectype]

    if readvsz {
        size := LBUINT(ParamSize(rec.vsz))
        // Generic value size
	    err = file.ReadIntoParam(pos, size, &rec.vsz, "generic valsize")
        if err != nil {return}
        pos += size
        file.Goto(pos)
    }

    // Key
    var key []byte
	key, err = file.LockedReadAt(pos, rec.ksz, "key")
    rec.key = key
	if err != nil {return}

    pos += rec.ksz
    file.Goto(pos)

    var valsize LBUINT = 0
    if readvsz {
        if readDataVal {valsize = rec.vsz} // otherwise, valsize = 0
    } else {
        valsize = GenericValueSize[rectype]
    }

    if valsize > 0 {
        rec.vpos = pos
        var val []byte
	    val, err = file.LockedReadAt(pos, valsize, "value")
	    if err != nil {return}

        rec.val = val
        pos += valsize
        file.Goto(pos)
    }

    newpos = pos
	return
}

// Read a block of bytes into a parameter from the gofile.
func (file *File) ReadIntoParam(pos, size LBUINT, data interface{}, desc string) (err error) {
	b, err := file.LockedReadAt(pos, size, desc)
    if err != nil {return}
	bfr := bytes.NewBuffer(b)
	err = binary.Read(bfr, binary.BigEndian, data)
    return
}

// Read a block of bytes from the file.  The read starts from the current file
// position, which is implicitely updated.  Because the current file position is
// mutable by other processes, the caller must be responsible for its own file
// position changes.  Also for this reason, we cannot have concurrent reads, and
// must wait for other read/writes.  The caller must ensure the file is
// opened and closed.
func (file *File) LockedReadAt(pos, size LBUINT, desc string) (bytes []byte, err error) {
	bytes = make([]byte, size)
	var nread int

    file.rwmu.RLock() // other reads ok
    // Locked action
	nread, err = file.gofile.ReadAt(bytes, int64(pos))
    file.rwmu.RUnlock()
	if err != nil {return}

	if LBUINT(nread) != size {
        err = FmtErrDataSize(desc, file.path, size, nread)
	}
    return
}

// Wait for any locks, set lock, write bytes to file and unlock.
// The caller is responsible for opening and closing the file.
func (file *File) LockedWriteAt(bytes []byte, pos LBUINT) (nwrite int, err error) {

    file.rwmu.Lock()
    // Locked action
	nwrite, err = file.gofile.WriteAt(bytes, int64(pos))
    file.rwmu.Unlock()
    if err != nil {return}

    return
}

func (freg *FileRegister) StringArray() []string {
    var result []string
    for k, _ := range freg.files {
        result = append(result, freg.files[k].String())
    }
    return result
}

func (file *File) String() string {
    return fmt.Sprintf(
        "%v %d %s %v",
        &file,
        file.id,
        file.path,
        file.rwmu)
}
