/*
    Define a wrapper around an operating system file, including its methods.
    This forms part of the "IO read infrastructure" which consists of the GenericRecord container, the Process and ReadRecord functions here, and the various processor functions elsewhere.  When there are lots of processor functions, this approach simplifies the logic and reduces code duplication, but there is double handling of record data in each processor which is not optimal.
    A Gofile wraps an os.File with some other properties, including locks.  This locking requires that we maintain a map of file paths to Gofiles, and that an existing Gofile be used if available when opening a file.
*/
package logbase

import (
	"os"
    "io"
    "bytes"
    "bufio"
    "encoding/binary"
    "sync"
)

const (
    DEFAULT_FILEMODE        os.FileMode = 0766 // octal with leading zero
    LOCKFILE_FORMAT         string = "lock.%s.%s" // type, filename
)

const (
    LOCK_WHILE_WRITING = iota
    LOCK_WHILE_READING = iota
)

// Wrap an os file with a current pointer.
type Gofile struct {
	file    *os.File
    path    string // path name used to open file
    rwmu    *sync.RWMutex
}

func NewGofile() *Gofile {
    return &Gofile{rwmu: new(sync.RWMutex)}
}

/*
type Locker struct {
    read    Lock
    write   Lock
}

type Lock struct {
    isLocked    bool
    hasChanged  chan bool
}
*/

//  Map of all files managed by the Logbase.
type FileRegister struct {
    files   map[string]*Gofile
}

// Init new file register.
func NewFileRegister() *FileRegister {
	return &FileRegister{files: make(map[string]*Gofile)}
}

// A tailored file opener for full create/append/rw.
func OpenFile(fpath string) (*os.File, error) {
    return os.OpenFile(
        fpath,
        os.O_CREATE |
        os.O_APPEND |
        os.O_RDWR,
        DEFAULT_FILEMODE)
}

// Open a new or existing Gofile for read/write access.
func (lbase *Logbase) OpenGofile(fpath string) (gfile *Gofile, err error) {
    var file *os.File
    file, err = OpenFile(fpath)
    if err != nil {return}

    // Use existing Gofile if present
    gfile, present := lbase.files[fpath]
    if present {return gfile, nil}

    gfile = NewGofile()
    gfile.file = file
    gfile.path = fpath
    lbase.files[fpath] = gfile
	return
}

// Open the os.File in the gofile for IO.
func (gfile *Gofile) Open() (err error) {
    file, err := os.OpenFile(
        gfile.path,
        os.O_CREATE |
        os.O_APPEND |
        os.O_RDWR,
        DEFAULT_FILEMODE)
    if err != nil {return}
    gfile.file = file
    return
}

// Close just the os.File for IO.
func (gfile *Gofile) Close() error {
    return gfile.file.Close()
}

// Returns the current file position.
func (gfile *Gofile) Here() (LBUINT, error) {
    seek, err := gfile.file.Seek(0, os.SEEK_CUR)
    pos := AsLBUINT(int(seek))
    if err != nil {return pos, err}
    return pos, nil
}

// Go to location in file relative to start.
func (gfile *Gofile) Goto(i LBUINT) (LBUINT, error) {
    seek, err := gfile.file.Seek(int64(i), os.SEEK_SET)
    pos := AsLBUINT(int(seek))
    if err != nil {return pos, err}
    return pos, nil
}

// Jump to location in file relative to current position.
func (gfile *Gofile) JumpFromHere(j LBUINT) (LBUINT, error) {
    seek, err := gfile.file.Seek(int64(j), os.SEEK_CUR)
    pos := AsLBUINT(int(seek))
    if err != nil {return pos, err}
    return pos, nil
}

// Jump to location in file relative to the end.
func (gfile *Gofile) JumpFromEnd(j LBUINT) (LBUINT, error) {
    seek, err := gfile.file.Seek(int64(j), os.SEEK_END)
    pos := AsLBUINT(int(seek))
    if err != nil {return pos, err}
    return pos, nil
}

// Allow looping through a file to be separated from processing of its
// records.
type Processor func(rec *GenericRecord) error

// Process the log file using the given function.
func (gfile *Gofile) Process(process Processor, rectype int, needDataVal bool) (err error) {
    var rec *GenericRecord
    var pos LBUINT = 0
	for {
        rec, pos, err = gfile.ReadRecord(pos, rectype, needDataVal)
	    if err != nil && err != io.EOF {break}
        err = process(rec)
        if err != nil {break}
	}
    return
}

// Read a record from the gofile, including the value depending on readDataVal.
func (gfile *Gofile) ReadRecord(pos LBUINT, rectype int, readDataVal bool) (rec *GenericRecord, newpos LBUINT, err error) {
    rec = NewGenericRecord()
    // Key size
	err = gfile.ReadIntoParam(pos, rec.ksz, "keysize") // implicitely moves position
    if err != nil {return}

    pos += ParamSize(rec.ksz)

    // Does this record type have a generic value size?
    var readvsz = DoReadDataValueSize[rectype]

    if readvsz {
        // Generic value size
	    err = gfile.ReadIntoParam(pos, rec.vsz, "generic valsize")
        if err != nil {return}
        pos += ParamSize(rec.vsz)
    }

    // Key
    var key []byte
	key, err = gfile.LockedReadAt(pos, rec.ksz, "key")
    rec.key = key
	if err != nil {return}

    pos += rec.ksz

    var valsize LBUINT = 0
    if readvsz {
        if readDataVal {valsize = rec.vsz} // otherwise, valsize = 0
    } else {
        valsize = GenericValueSize[rectype]
    }

    if valsize > 0 {
        rec.vpos = pos
        var val []byte
	    val, err = gfile.LockedReadAt(pos, valsize, "value")
	    if err != nil {return}

        rec.val = val
        pos += valsize
    }

    newpos = pos
	return
}

// Read a block of bytes into a parameter from the gofile.
func (gfile *Gofile) ReadIntoParam(pos LBUINT, data interface{}, desc string) (err error) {
	bfr1, err := gfile.LockedReadAt(pos, LBUINT(ParamSize(data)), desc)
    if err != nil {return}
	bfr2 := bufio.NewReader(bytes.NewBuffer(bfr1))
	binary.Read(bfr2, binary.BigEndian, &data)
    return
}

// Read a block of bytes from the gofile.  The read starts from the current file
// position, which is implicitely updated.  Because the current file position is
// mutable by other processes, the caller must be responsible for its own file
// position changes.  Also for this reason, we cannot have concurrent reads, and
// must wait for other read/writes.
func (gfile *Gofile) LockedReadAt(pos, size LBUINT, desc string) (bfr []byte, err error) {
	bfr = make([]byte, size)
	var nread int

    gfile.rwmu.RLock() // other reads ok

    // Locked action
	nread, err = gfile.file.ReadAt(bfr, int64(pos))
	if err != nil {return}
    gfile.rwmu.RUnlock()

	if LBUINT(nread) != size {
        err = FmtErrDataSize(desc, gfile.path, size, nread)
	}
    return
}

// Wait for any locks, set lock, write bytes to file and unlock.
func (gfile *Gofile) LockedWriteAt(bytes []byte, pos LBUINT) (nwrite int, err error) {

    gfile.rwmu.Lock()

    // Locked action
	nwrite, err = gfile.file.WriteAt(bytes, int64(pos))
    if err != nil {return}
    gfile.rwmu.Unlock()

    return
}
