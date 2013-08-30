/*
	Define a wrapper around an operating system file, including its methods, for use in this and other applications.
	This forms part of an "IO read infrastructure" which consists of the GenericRecord container, the Process and ReadRecord functions here, and the various processor functions elsewhere.  When there are lots of processor functions, this approach simplifies the logic and reduces code duplication, but there is double handling of record data in each processor which is not optimal.
	A File wraps an os.File with some other properties, including locks.  This locking requires that we maintain a map of file paths to Files, and that an existing File be used if available when opening a file.
*/
package logbase

import (
	"os"
	"io"
	"bytes"
	"path"
	"path/filepath"
	//"bufio"
	"encoding/binary"
	"sync"
	"fmt"
)

const (
	DEFAULT_FILEMODE	os.FileMode = 0666 // octal with leading zero
	TMPFILE_PREFIX      string = ".tmp."
)

type LBUINT uint32 // Unsigned Logbase integer type used on file

const (
	LBUINT_SIZE		LBUINT = 4 // bytes 
	LBUINT_SIZE_x2  LBUINT = 2 * LBUINT_SIZE
	LBUINT_SIZE_x3  LBUINT = 3 * LBUINT_SIZE
	LBUINT_SIZE_x4  LBUINT = 4 * LBUINT_SIZE
)

// 
const (
	APPEND				int = os.O_APPEND
	READ_ONLY			int = os.O_RDONLY
	WRITE_ONLY			int = os.O_WRONLY
	READ_WRITE			int = os.O_RDWR
	CREATE				int = os.O_CREATE
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
	abspath string // path name used to open file
	sync.RWMutex
	debug   *DebugLogger
	isOpen  bool // its ok to have multiple opens of same gofile
	size    int // size in bytes
	tmp		*File // temporary "twin" file
}

func NewFile() *File {
	return &File{}
}

// Test whether the given file or directory exists.
func Exists(abspath string) bool {
	_, err := os.Stat(abspath)
	if !os.IsNotExist(err) {return true}
	return false
}

// Open a new or existing File for read/write access.
// Use this as the gateway for file creation/retrieval
// where possible to take advantage of the file register
// and ensure proper initialisation.
func (lbase *Logbase) GetFile(relpath string) (*File, error) {
	fpath := path.Join(lbase.abspath, relpath)
	// Use existing File if present
	obj, present := lbase.FileCache().objects[fpath]
	if present {return obj.(*File), nil}

	// Create file and its tmp twin
	file := lbase.MakeFile(fpath)
	// The tmp twin
	file.tmp = lbase.MakeFile(file.TmpTwinPath())

	err := file.Touch()
	return file, err
}

// Construct a new file.
func (lbase *Logbase) MakeFile(path string) (file *File) {
	file = NewFile()
	file.id = fileCounter
	fileCounter++
	file.abspath = path
	file.debug = lbase.debug
	lbase.FileCache().objects[file.abspath] = file
	return file
}

func OpenFile(abspath string, flags int) (*os.File, error) {
	return os.OpenFile(abspath, flags, DEFAULT_FILEMODE)
}

// A tailored file opener for full create/append/rw.
func (file *File) Open(flags int) (err error) {
	var gfile *os.File
	gfile, err = OpenFile(file.abspath, flags)
	if err == nil {
		file.gofile = gfile
		file.isOpen = true
		info, err2 := os.Stat(file.abspath)
		if err2 != nil {
			err = err2
			return
		}
		file.size = int(info.Size())
	}
	return
}

// Close file for IO.
func (file *File) Close() (err error) {
	err = file.gofile.Close()
	if err == nil {file.isOpen = false}
	return
}

// Delete file.
func (file *File) Remove() (err error) {
	return os.Remove(file.abspath)
}

// Return file path relative to the logbase path.
func (file *File) RelPath(lbase *Logbase) string {
	result, err := filepath.Rel(lbase.abspath, file.abspath)
	if err != nil {
		WrapError(fmt.Sprint(
			"Could not extract a relative path using basepath %q " +
			"and targetpath %q",
			lbase.abspath,
			file.abspath), err).Fatal()
	}
	return result
}

// If file does not exist, create it.  Updates file size.
func (file *File) Touch() error {
	info, err := os.Stat(file.abspath)
	if os.IsNotExist(err) {
		err2 := file.Open(CREATE)
		if err2 == nil {
			file.Close()
		} else {
			return err2
		}
		file.size = 0
	} else if err != nil {
		return err
	} else {
		file.size = int(info.Size())
	}
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

// Dump file bytes in hex format to the debugger.  If finish == 0,
// go to end of file.
func (file *File) ToHex(start, finish int) []string {
	file.Open(READ_ONLY)
	defer file.Close()
	if finish == 0 || finish > file.size {finish = file.size}
	if start < 0 {start = 0}
	if start > finish {
		FmtErrBadArgs(
			"start %v must be less than finish %v",
			start, finish).Fatal()
	}
	const bytesPerRow int = 16
	// Make buffers            
	var lines []string
	for i := start; i < finish; i = i + bytesPerRow {
		byts, err := file.LockedReadAt(LBUINT(i), LBUINT(bytesPerRow), "hexdump")
		if err != nil && err != io.EOF {
			WrapError(fmt.Sprintf(
				"Problem with %s trying to read %d bytes at position %d",
				file.abspath, bytesPerRow, i),
				err).Fatal()
		}
		lines = append(lines, FmtHexString(byts))
	}
	return lines
}

// Returns the path of the temporary twin.
func (file *File) TmpTwinPath() string {
	return path.Join(
			filepath.Dir(file.abspath),
			TMPFILE_PREFIX + filepath.Base(file.abspath))
}

// Replace the file with its temporary twin.
func (file *File) ReplaceWithTmpTwin() (err error) {
	file.Lock()
	if err = file.Remove(); file.debug.Error(err) != nil {return}
	err = os.Rename(file.tmp.abspath, file.abspath)
	file.debug.Error(err)
	file.Unlock()
	return
}

// Allow looping through a file to be separated from processing of its
// records.
type Processor func(rec *GenericRecord) error

// Process the file using the given function.
func (file *File) Process(process Processor, rectype int, needDataVal bool) (err error) {
	file.Open(READ_ONLY)
	defer file.Close()
	var rec *GenericRecord
	var pos LBUINT = 0
	var err2 error
	for {
		rec, pos, err = file.ReadRecord(pos, rectype, needDataVal)
		file.debug.Fine("Process generic rec = %v pos = %v err = %v", rec, pos, err)
		err2 = process(rec)
		if err != nil || err2 != nil {break}
	}
	if err == io.EOF {err = nil}
	if err == nil && err2 != nil {err = err2}
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
	var readvsz = FileDecodeConfigs[rectype].readDataValueSize

	if readvsz {
		size := LBUINT(ParamSize(rec.vsz))
		// Generic value size
	    err = file.ReadIntoParam(pos, size, &rec.vsz, "generic valsize")
		if err != nil {return}
		pos += size
		file.Goto(pos)
	}

	// Key
	kbyts, err := file.LockedReadAt(pos, rec.ksz, "key")
	key, ktype := SnipKeyType(kbyts, file.debug)
	rec.kbyts = key
	rec.ktype = ktype
	if err != nil {return}

	pos += rec.ksz
	file.Goto(pos)

	// Generic Value
	var valsize LBUINT = 0
	var snipval = FileDecodeConfigs[rectype].snipValueType
	if readvsz {
		if readDataVal {valsize = rec.vsz} // otherwise, valsize = 0
	} else {
		valsize = FileDecodeConfigs[rectype].genericValueSize
	}

	if valsize > 0 {
		rec.vpos = pos
		var vbyts []byte
	    vbyts, err = file.LockedReadAt(pos, valsize, "value")
		if snipval {
			val, vtype := SnipValueType(vbyts, file.debug)
			rec.vbyts = val
			rec.vtype = vtype
		} else {
			rec.vbyts = vbyts
		}
	    if err != nil {return}

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
	err = file.debug.Error(binary.Read(bfr, binary.BigEndian, data))
	return
}

// Read a block of bytes from the file.  The read starts from the current file
// position, which is implicitely updated.  Because the current file position is
// mutable by other processes, the caller must be responsible for its own file
// position changes.  Also for this reason, we cannot have concurrent reads, and
// must wait for other read/writes.  The caller must ensure the file is
// opened and closed.
func (file *File) LockedReadAt(pos, size LBUINT, desc string) (byts []byte, err error) {
	byts = make([]byte, size)
	var nr int
	file.RLock() // other reads ok
	// Locked action
	nr, err = file.gofile.ReadAt(byts, int64(pos))
	file.RUnlock()
	byts = byts[0:nr]
	if err != nil {return}

	if LBUINT(nr) != size {
		err = FmtErrReadSize(desc, file.abspath, size, nr)
	}
	return
}

// Wait for any locks, set lock, write bytes to file and unlock.
// The caller is responsible for opening and closing the file.
func (file *File) LockedWriteAt(byts []byte, pos LBUINT) (nw int, err error) {
	file.Lock()
	// Locked action
	nw, err = file.gofile.WriteAt(byts, int64(pos))
	file.Unlock()
	return
}

func (file *File) String() string {
	return fmt.Sprintf(
		"%v %d %s",
		&file,
		file.id,
		file.abspath)
}
