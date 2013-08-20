/*
	File IO and management specific to this application.

	Legend
	------

	K       Key
	V       Value
	GV      Generic Value (used by ReadRecord)
	KS      Key size
	VS      Value size
	GVS     Generic Value size (used by ReadRecord)
	VP      Value position
	F       File number
	P		Permission
	C       Checksum
	RS      (Entire) Record size
	RP      (Entire) Record position
	LBT		Logbase Type (LBTYPE)
	MCK		Master Catalog Key (MCID_TYPE)

	GENERIC RECORD (used in ReadRecord)
	+------+------+------+------+------+------+------+------+
	|      |      |             |                           |
	|  KS  |  GVS |      K      |             GV            |
	|      |      |             |                           |
	+------+------+------+------+------+------+------+------+

	LOGFILE RECORD (LOG_RECORD)
	+------+------+------+------+------+------+------+------+
	|      |      |             |                    :      |
	|  KS  |  GVS |      K      |          V         :  C   |
	|      |      |             |                    :      |
	+------+------+------+------+------+------+------+------+
			                    |<----------- GV ---------->|

	LOGFILE INDEX FILE RECORD (INDEX_RECORD)
	+------+------+------+------+------+
	|      |             |      |      |
	|  KS  |      K      |  VS  |  VP  |     No GVS
	|      |             |      |      |
	+------+------+------+------+------+
			             |<---- GV --->|

	MASTER CATALOG FILE RECORD (MASTER_RECORD)
	...with a ValueLocation
	+------+------+------+------+------+------+------+
	|      |             |      |      |      |      |
	|  KS  |      K      | LBT  |  F   |  VS  |  VP  |  No GVS
	|      |             |      |      |      |      |
	+------+------+------+------+------+------+------+
			             |<----------- GV ---------->|
	...with a MasterCatalogKey
	+------+------+------+------+------+
	|      |             |      |      |
	|  KS  |      K      | LBT  |  MCK |   No GVS
	|      |             |      |      |
	+------+------+------+------+------+
			             |<--- GV ---->|

	ZAPMAP FILE RECORD (ZAP_RECORD)
	+------+------+------+------+------+------+------+------+------+------+
	|      |      |             |      :      :      |      :      :      |
	|  KS  |  GVS |      K      |  F   :  RS  :  RP  |  F   :  RS  :  RP  |
	|      |      |             |      :      :      |      :      :      |
	+------+------+------+------+------+------+------+------+------+------+
			                    |<------------------- GV ---------------->|

	USER PERMISSION RECORD (PERMISSION_RECORD)
	+------+------+------+------+
	|      |             |      |
	|  KS  |      K      |  P   |  No GVS
	|      |             |      |
	+------+------+------+------+
			             |<-GV->| = 1 byte

*/
package logbase

import (
	"os"
	"io"
	"path/filepath"
	"strings"
	"strconv"
	"sort"
	"fmt"
	"reflect"
)

const (
	FILENAME_DELIMITER      string = "."
	LOGFILE_NAME_FORMAT     string = "%09d"
	INDEX_FILE_EXTENSION    string = ".index"
	STARTING_LOGFILE_NUMBER LBUINT = 1
)

// The basic unit of a logbase.
type Logfile struct {
	*File
	fnum        LBUINT // log file number
	indexfile   *Indexfile // index for this logfile
}

// Init a Logfile.
func NewLogfile() *Logfile {
	return &Logfile{
		File: NewFile(),
		indexfile: NewIndexfile(),
	}
}

// Speed up initialisation of the master catalog.
type Indexfile struct {
	*File
	*Index
}

// Init an Indexfile.
func NewIndexfile() *Indexfile {
	return &Indexfile{
		File: NewFile(),
		Index: &Index{},
	}
}

// Allow persistence of master catalog.
type Masterfile struct {
	*File
}

// Init a Masterfile.
func NewMasterfile(file *File) *Masterfile {
	return &Masterfile{
		File: file,
	}
}

// Allow persistence of scheduled kv pair deletion.
type Zapfile struct {
	*File
}

// Init a Zapfile.
func NewZapfile(file *File) *Zapfile {
	return &Zapfile{
		File: file,
	}
}

// Allow persistence of user permissions.
type UserPermissionFile struct {
	*File
}

// Init a UserPermissionFile.
func NewUserPermissionFile(file *File) *UserPermissionFile {
	return &UserPermissionFile{
		File: file,
	}
}

// Logbase methods.

// Get a handle on a log file and its associated index file for read/write access.
// If none exist, create each.  Note that while we keep a register of Files,
// we do not keep a register of log or index files.
func (lbase *Logbase) GetLogfile(fnum LBUINT) (lfile *Logfile, err error) {
	fpath := lbase.MakeLogfileRelPath(fnum)
	var file *File
	file, err = lbase.GetFile(fpath)
	if err != nil {return}

	lfile = NewLogfile()
	lfile.File = file
	lfile.fnum = fnum

	// Identify index file
	ipath := lbase.MakeIndexfileRelPath(fnum)
	file, err = lbase.GetFile(ipath)

	// Link it to 
	lfif := NewIndexfile()
	lfif.File = file
	lfile.indexfile = lfif

	return
}

// If a livelog is not defined for the given logbase, open the last
// indexed log file in the logbase, designated as the live log,
// for read/write access.
func (lbase *Logbase) SetLiveLog() error {
	if lbase.livelog == nil {
		_, inds, err := lbase.GetLogfilePaths()
		if err != nil {return err}

		var lfile *Logfile
		if len(inds) == 0 {
			lfile, err = lbase.GetLogfile(STARTING_LOGFILE_NUMBER)
		} else {
			var last int = len(inds) - 1
			lfile, err = lbase.GetLogfile(inds[last])
		}
		if err != nil {return err}
		lbase.livelog = lfile
		lbase.debug.Fine("Set livelog as %q", lfile.abspath)
	}
	return nil
}

// Assemble a list of all log files in the current logbase, sorted by index.
// Uses filepath.Walk to find all files in the logbase directory
// (ignoring nested directories), and assembles the file indices and paths into
// a map and a separate index slice, which is used to sort the map, which in
// turn is split into a file path and associated index list.
func (lbase *Logbase) GetLogfilePaths() (fpaths []string, uinds32 []LBUINT, err error) {
	var nscan int = 0 // Number of file objects scanned
	fmap := make(map[int]string)
	var inds []int

	findLogfile := func(fpath string, fileInfo os.FileInfo, inerr error) (err error) {
		stat, err := os.Stat(fpath)
		if err != nil {return}

		if nscan > 0 && stat.IsDir() {
			return filepath.SkipDir
		}
		nscan++
		parts := strings.Split(filepath.Base(fpath), FILENAME_DELIMITER)
		if len(parts) == 2 && parts[1] == lbase.config.LOGFILE_NAME_EXTENSION {
			num64, err := strconv.ParseInt(parts[0], 10, 32)
			if err != nil {return WrapError("Problem interpreting path", err)}

			numint := int(num64)
			if reflect.ValueOf(numint).OverflowInt(num64) {
			    // In case we are on a 32 bit machine
			    err = FmtErrIntMismatch(num64, fpath, "native int", numint)
			    return err
			}

			inds = append(inds, numint)
			fmap[numint] = fpath
		}
		return
	}

	err = filepath.Walk(lbase.abspath, findLogfile)
	if err != nil {
		return nil, nil, err
	}

	// Now sort
	sort.Ints(inds)
	fpaths = make([]string, len(inds))
	uinds32 = make([]LBUINT, len(inds))
	for i, ind := range inds {
		fpaths[i] = fmap[ind]
		uinds32[i] = AsLBUINT(ind)
	}

	return
}

// Return the log file path associated with given the log file number.
func (lbase *Logbase) MakeLogfileRelPath(fnum LBUINT) string {
	return MakeLogfileName(fnum, lbase.config.LOGFILE_NAME_EXTENSION)
}

// Return the index file path associated with given the log file number.
func (lbase *Logbase) MakeIndexfileRelPath(fnum LBUINT) string {
	return MakeIndexfileName(fnum, lbase.config.INDEXFILE_NAME_EXTENSION)
}

// Assemble a list of all user permission files in the current logbase,
// unsorted.
func (lbase *Logbase) GetUserPermissionPaths() (usernames []string, err error) {
	var nscan int = 0 // Number of file objects scanned

	findPermissionFile := func(fpath string, fileInfo os.FileInfo, inerr error) (err error) {
		stat, err := os.Stat(fpath)
		if err != nil {return}

		if nscan > 0 && stat.IsDir() {
			return filepath.SkipDir
		}
		if nscan > 0 {usernames = append(usernames, filepath.Base(fpath))}
		nscan++
		return
	}

	err = filepath.Walk(lbase.UserPermissionDirPath(), findPermissionFile)
	return
}

// Save the master catalog, zapmap and user permission files for the logbase.  Only
// save each if there has been a change.
func (lbase *Logbase) Save() (err error) {
	if lbase.mcat.changed {
		err = lbase.debug.Error(lbase.mcat.Save(lbase.debug))
		if err != nil {return}
		lbase.mcat.changed = false
		lbase.debug.Advise("Saved master catalog for logbase %q", lbase.name)
	}
	if lbase.zmap.changed {
		err = lbase.debug.Error(lbase.zmap.Save(lbase.debug))
		if err != nil {return}
		lbase.zmap.changed = false
		lbase.debug.Advise("Saved zapmap for logbase %q", lbase.name)
	}
	for user, perm := range lbase.users.perm {
		if perm.changed {
			err = lbase.debug.Error(perm.Save(lbase.debug))
			if err != nil {return}
			perm.changed = false
			lbase.debug.Advise("Saved %q permissions for logbase %q", user, lbase.name)
		}
	}
	return
}

// Read the log file (given by the index) and build an associated index file.
func (lbase *Logbase) RefreshIndexfile(fnum LBUINT) (lfindex *Index, err error) {
	var lfile *Logfile
	lfile, err = lbase.GetLogfile(fnum)
	if err != nil {return}
	lfindex, err = lfile.Index()
	if err != nil {return}
	err = lfile.indexfile.Save(lfindex)
	return
}

// Read the log file (given by the index) and build an associated index file.
func (lbase *Logbase) ReadIndexfile(fnum LBUINT) (lfindex *Index, err error) {
	var lfile *Logfile
	lfile, err = lbase.GetLogfile(fnum)
	if err != nil {return}
	lfindex, err = lfile.indexfile.Load()
	return
}

// File name related functions.

func MakeLogfileName(fnum LBUINT, ext string) string {
	return fmt.Sprintf(
		LOGFILE_NAME_FORMAT +
		FILENAME_DELIMITER +
		ext,
		fnum)
}

func MakeIndexfileName(fnum LBUINT, ext string) string {
	return fmt.Sprintf(
		LOGFILE_NAME_FORMAT +
		FILENAME_DELIMITER +
		ext,
		fnum)
}

// Log file methods, that may include associated index file ops.

// Index the given log file.
func (lfile *Logfile) Index() (*Index, error) {
	index := new(Index)
	f := func(rec *GenericRecord) error {
		if rec.ksz > 0 {
			irec := rec.ToLogRecord().ToIndexRecord()
			index.list = append(index.list, irec)
		}
		return nil
	}
	err := lfile.Process(f, LOG_RECORD, true)
	return index, err
}

// Read log file into two slices of raw bytes containing the keys and values
// respectively.
func (lfile *Logfile) Load() ([]*LogRecord, error) {
	var lrecs []*LogRecord
	f := func(rec *GenericRecord) error {
		if rec.ksz > 0 {
			lrec := rec.ToLogRecord()
			lrecs = append(lrecs, lrec)
		}
		return nil
	}
	err := lfile.Process(f, LOG_RECORD, true)
	return lrecs, err
}

// Append data to log file and append a new index record to the index,
// both in-memory and on file.  Does not update the master catalog or
// zapmap.
func (lfile *Logfile) StoreData(lrec *LogRecord) (irec *IndexRecord, err error) {
	lfile.Open(CREATE | WRITE_ONLY | APPEND)
	defer lfile.Close()
	pos, _ := lfile.JumpFromEnd(0)
	var nwrite int
	nwrite, err = lfile.LockedWriteAt(lrec.Pack(), pos)
	lfile.size += nwrite
	if err != nil {return}

	// Create a new file index record
	irec = lrec.ToIndexRecord()
	hsz := LBUINT(ParamSize(lrec.ksz) + ParamSize(lrec.vsz))
	irec.vpos = pos + hsz + irec.ksz

	// Update the in-memory file index
	lfile.indexfile.list = append(lfile.indexfile.list, irec)

	// Write the index record to the index file
	lfile.indexfile.Open(CREATE | WRITE_ONLY | APPEND)
	defer lfile.indexfile.Close()
	pos, _ = lfile.indexfile.JumpFromEnd(0)
	nwrite, err = lfile.indexfile.LockedWriteAt(irec.Pack(), pos)
	lfile.indexfile.size += nwrite
	return
}

// Read a value from the log file.
func (lfile *Logfile) ReadVal(vpos, vsz LBUINT) ([]byte, error) {
	lfile.Open(READ_ONLY)
	defer lfile.Close()
	return lfile.LockedReadAt(vpos, vsz, "value")
}

// Zap stale values from the logfile, by copying the file to a tmp file while
// ignoring stale records as defined by the given Zapmap.
func (lfile *Logfile) Zap(zmap *Zapmap, bfrsz LBUINT) error {
	lfile.debug.Fine("Zapping %s", lfile.abspath)
	// Extract all zaprecords for this file and build a map between the logfile
	// record positions -> record size.
	rpos, rsz, err := zmap.Find(lfile.fnum)
	if err != nil {return err}
	if len(rpos) == 0 {
		lfile.debug.Fine(" Nothing to zap")
		return nil
	}
	lfile.debug.SuperFine(" zaplists: rpos = %v rsz = %v", rpos, rsz)

	// Create temporary file.
	err = lfile.tmp.Open(CREATE | WRITE_ONLY | APPEND)
	if lfile.debug.Error(err) != nil {return err}

	lfile.Open(READ_ONLY)
	last := len(rpos) - 1
	pos := int(rpos[last] + rsz[last])
	if pos > lfile.size {
		return FmtErrPositionExceedsFileSize(lfile.abspath, pos, lfile.size)
	}
	lfile.debug.SuperFine(" file size = %d", lfile.size)

	// Invert the zap lists to make position and size of chunks to preserve
	cpos, csz := InvertSequence(rpos, rsz, lfile.size)
	lfile.debug.SuperFine(" preserve: cpos = %v csz = %v", cpos, csz)

	// Transpose logfile (with gaps) to tmp file
	var bfr []byte // normal buffer
	bfr0 := make([]byte, int(bfrsz))
	var rem LBUINT // remainder buffer
	var hasRem bool // is there a remainder portion when chunk divided by bfr0?
	var size LBUINT // number of bytes to read/write
	var n LBUINT // number of buffer lengths returned by BufferChunk, +1 if rem
	var kr LBUINT // read position in logfile
	var kw LBUINT = 0 // write position in tmp file
	var j LBUINT
	var nr int

	lfile.RLock() // other reads ok while we transpose to tmp file

	for i := 0; i < len(cpos); i++ {
		// First, we need to determine the chunk that needs to be read
		kr = cpos[i]
		n, rem = DivideChunkByBuffer(csz[i], bfrsz)
		lfile.debug.SuperFine(
			" dividing chunk %d by %d yields n = %d rem = %d",
			csz[i], bfrsz, n, rem)
		hasRem = (rem > 0)
		if hasRem {n++}
		bfr = bfr0
		size = bfrsz
		for j = 0; j < n; j++ {
			if j == n - 1 && hasRem { // switch for the remainder portion
				bfr = make([]byte, rem)
				size = rem
			}
			// Read
			nr, err = lfile.gofile.ReadAt(bfr, int64(kr))
			bfr = bfr[0:nr]
			lfile.debug.SuperFine(
				" read = %s err = %v",
				FmtHexString(bfr), err)
			if err != nil && err != io.EOF {
				return WrapError(fmt.Sprintf(
					"Attempted to read %d bytes at position %d in file %q",
					size, kr, lfile.abspath), err)
			}
			kr = kr + size

			// Write
			_, err = lfile.tmp.gofile.Write(bfr) // Only use part of slice if near EOF
			lfile.debug.SuperFine(
				" wrote = %s err = %v",
				FmtHexString(bfr), err)
			if err != nil {
				return WrapError(fmt.Sprintf(
					"Attempted to write %d bytes at position %d in file %q",
					size, kw, lfile.tmp.abspath), err)
			}
			kw = kw + size
		}
	}

	lfile.RUnlock()
	lfile.Close()
	lfile.tmp.Close()

	if kw > 0 {
	    err = lfile.ReplaceWithTmpTwin()
		if lfile.debug.Error(err) != nil {return err}
		zmap.Purge(lfile.fnum, lfile.debug)
	} else {
		err = lfile.tmp.Remove()
		if lfile.debug.Error(err) != nil {return err}
	}

	return nil
}

// Log file index file methods.

// Read the index file.
func (ifile *Indexfile) Load() (lfindex *Index, err error) {
	lfindex = new(Index)
	f := func(rec *GenericRecord) error {
		if rec.ksz > 0 {
			irec := rec.ToIndexRecord()
			lfindex.list = append(lfindex.list, irec)
		}
		return nil
	}
	err = ifile.Process(f, INDEX_RECORD, false)
	return
}

// Write index file.
// TODO would it be faster to build a []byte and write once?
func (ifile *Indexfile) Save(lfindex *Index) (err error) {
	ifile.Open(CREATE | WRITE_ONLY | APPEND)
	defer ifile.Close()
	irsz := int(ParamSize(NewIndexRecord()))
	bytes := make([]byte, len(lfindex.list) * irsz)
	var start int = 0
	for _, rec := range lfindex.list {
		for j, b := range rec.Pack() {
			bytes[start + j] = b
		}
		start += irsz
	}
	_, err = ifile.LockedWriteAt(bytes, 0)
	return
}

// Zapmap file methods.

// Read zap file into a zapmap.
func (zmap *Zapmap) Load(debug *DebugLogger) (err error) {
	zmap.file.Open(READ_ONLY)
	defer zmap.file.Close()
	if zmap.file.size == 0 {return}
	zmap.Lock()
	f := func(rec *GenericRecord) error {
		if rec.ksz > 0 {
			key, zrecs := rec.ToZapRecordList(debug)
			zmap.zapmap[key] = zrecs // Don't need to use gateway because zmap is fresh
		}
		return nil
	}
	err = zmap.file.Process(f, ZAP_RECORD, true)
	zmap.Unlock()
	return
}

// Write zapmap file.
func (zmap *Zapmap) Save(debug *DebugLogger) (err error) {
	zmap.file.tmp.Open(CREATE | WRITE_ONLY)
	var nw int
	var pos LBUINT = 0
	zmap.RLock()
	for key, zrecs := range zmap.zapmap {
		nw, err = zmap.file.tmp.LockedWriteAt(PackZapRecord(key, zrecs, debug), pos)
		if err != nil {return}
		pos = pos.Plus(nw)
	}
	zmap.file.tmp.Close()
	err = zmap.file.ReplaceWithTmpTwin()
	zmap.RUnlock()
	return
}

// Master index file methods.

// Read master catalog file into a new master catalog.
func (mcat *MasterCatalog) Load(debug *DebugLogger) (err error) {
	ResetMCID()
	mcat.file.Open(READ_ONLY)
	defer mcat.file.Close()
	if mcat.file.size == 0 {return}
	mcat.Lock()
	f := func(rec *GenericRecord) error {
		if rec.ksz > 0 {
			key, mcr := rec.ToMasterCatalogRecord(debug)
			mcat.index[key] = mcr // Don't need to use gateway because mcat is fresh
			IncIfMCID(key) // Increment the NextMCI if necessary 
		}
		return nil
	}
	err = mcat.file.Process(f, MASTER_RECORD, false)
	mcat.Unlock()
	return
}

// Write master catalog file.
func (mcat *MasterCatalog) Save(debug *DebugLogger) (err error) {
	mcat.file.tmp.Open(CREATE | WRITE_ONLY)
	var nw int
	var pos LBUINT = 0
	mcat.RLock()
	for key, mcr := range mcat.index {
		nw, err = mcat.file.tmp.LockedWriteAt(mcr.Pack(key, debug), pos)
		if err != nil {return}
		pos = pos.Plus(nw)
	}
	mcat.file.tmp.Close()
	err = mcat.file.ReplaceWithTmpTwin()
	mcat.RUnlock()
	return
}

// User Permission index file methods.

// Read user permission file into a new user permission index.
func (up *UserPermissions) Load(debug *DebugLogger) (err error) {
	up.file.Open(READ_ONLY)
	defer up.file.Close()
	if up.file.size == 0 {return}
	up.Lock()
	f := func(rec *GenericRecord) error {
		if rec.ksz > 0 {
			key, upr := rec.ToUserPermissionRecord(debug)
			up.index[key] = upr // Don't need to use gateway because up is fresh
		}
		return nil
	}
	err = up.file.Process(f, PERMISSION_RECORD, false)
	up.Unlock()
	return
}

// Write user permission file.
func (up *UserPermissions) Save(debug *DebugLogger) (err error) {
	up.file.tmp.Open(CREATE | WRITE_ONLY)
	var nw int
	var pos LBUINT = 0
	up.RLock()
	for key, upr := range up.index {
		nw, err = up.file.tmp.LockedWriteAt(
					PackUserPermissionRecord(key, upr, debug), pos)
		if err != nil {return}
		pos = pos.Plus(nw)
	}
	up.file.tmp.Close()
	err = up.file.ReplaceWithTmpTwin()
	up.RUnlock()
	return
}

