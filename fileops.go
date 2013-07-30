/*
    File IO and management.

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
    C       Checksum

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
    +------+------+------+------+------+------+
    |      |             |      |      |      |
    |  KS  |      K      |  F   |  VS  |  VP  |    No GVS
    |      |             |      |      |      |
    +------+------+------+------+------+------+
                         |<------- GV ------->|

    ZAPMAP FILE RECORD (ZAP_RECORD)
    +------+------+------+------+------+------+------+------+------+------+
    |      |      |             |      :      :      |      :      :      |
    |  KS  |  GVS |      K      |  F   :  VS  :  VP  |  F   :  VS  :  VP  |
    |      |      |             |      :      :      |      :      :      |
    +------+------+------+------+------+------+------+------+------+------+
                                |<------------------- GV ---------------->|
*/
package logbase

import (
	"os"
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
        File: &File{},
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
        File: &File{},
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
func (lbase *Logbase) SetLiveLog() *Logbase {
    if lbase.livelog == nil {
        _, inds, err := lbase.GetLogfilePaths()
        if err != nil {return lbase.SetErr(err)}

        var lfile *Logfile
        if len(inds) == 0 {
            lfile, err = lbase.GetLogfile(STARTING_LOGFILE_NUMBER)
        } else {
            var last int = len(inds) - 1
            lfile, err = lbase.GetLogfile(inds[last])
        }
        if err != nil {return lbase.SetErr(err)}
        lbase.livelog = lfile
        lbase.debug.Fine(DEBUG_DEFAULT, "Set livelog as %q", lfile.abspath)
    }
    return lbase
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

    scanner := func(fpath string, fileInfo os.FileInfo, inerr error) (err error) {
        stat, err := os.Stat(fpath)
        if err != nil {
            return err
        }

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

    err = filepath.Walk(lbase.abspath, scanner)
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

// Save the master catalog and zapmap files for the logbase.
func (lbase *Logbase) Save() (err error) {
    err = lbase.mcat.Save()
    if err != nil {return}
    err = lbase.zmap.Save()
    if err != nil {return}
    lbase.debug.Advise(DEBUG_DEFAULT, "Saved master catalog and zapmap for logbase %q", lbase.name)
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
        irec := rec.ToLogRecord().ToIndexRecord()
        index.list = append(index.list, irec)
        return nil
    }
    err := lfile.Process(f, LOG_RECORD, true)
    return index, err
}

// Read log file into two slices of raw bytes containing the keys and values
// respectively.
func (lfile *Logfile) Load() ([][]byte, [][]byte, error) {
    var keys [][]byte
    var vals [][]byte
    f := func(rec *GenericRecord) error {
        lrec := rec.ToLogRecord()
        keys = append(keys, lrec.key)
        vals = append(vals, lrec.val)
        return nil
    }
    err := lfile.Process(f, LOG_RECORD, true)
    return keys, vals, err
}

// Append data to log file and append a new index record to the index,
// both in-memory and on file.  Does not update the master catalog or
// zapmap.
func (lfile *Logfile) StoreData(lrec *LogRecord) (irec *IndexRecord, err error) {
    lfile.Open()
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
    lfile.indexfile.Open()
    defer lfile.indexfile.Close()
    pos, _ = lfile.indexfile.JumpFromEnd(0)
	nwrite, err = lfile.indexfile.LockedWriteAt(irec.Pack(), pos)
    lfile.indexfile.size += nwrite
	return
}

// Read a value from the log file.
func (lfile *Logfile) ReadVal(vpos, vsz LBUINT) ([]byte, error) {
    lfile.Open()
    defer lfile.Close()
	return lfile.LockedReadAt(vpos, vsz, "value")
}

// Log file index file methods.

// Read the index file.
func (ifile *Indexfile) Load() (lfindex *Index, err error) {
    lfindex = new(Index)
    f := func(rec *GenericRecord) error {
        irec := rec.ToIndexRecord()
        lfindex.list = append(lfindex.list, irec)
        return nil
    }
    err = ifile.Process(f, INDEX_RECORD, false)
    return
}

// Write index file.
// TODO would it be faster to build a []byte and write once?
func (ifile *Indexfile) Save(lfindex *Index) (err error) {
    ifile.Open()
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
func (zmap *Zapmap) Load() (err error) {
    zmap.file.Open()
    defer zmap.file.Close()
    f := func(rec *GenericRecord) error {
        keystr, zrecs := rec.ToZapRecordList()
        zmap.zapmap[keystr] = zrecs
        return nil
    }
    err = zmap.file.Process(f, ZAP_RECORD, true)
    return
}

// Write zapmap file.
func (zmap *Zapmap) Save() (err error) {
    zmap.file.Open()
    defer zmap.file.Close()
    var nwrite int
    var pos LBUINT = 0
    for keystr, zrecs := range zmap.zapmap {
	    nwrite, err = zmap.file.LockedWriteAt(PackZapRecord(keystr, zrecs), pos)
        if err != nil {return}
        pos = pos.plus(nwrite)
    }
    return
}

// Master index file methods.

// Read master catalog file into a new master catalog.
func (mcat *MasterCatalog) Load() (err error) {
    mcat.file.Open()
    defer mcat.file.Close()
    f := func(rec *GenericRecord) error {
        mcr := rec.ToMasterCatalogRecord()
        mcat.index[string(rec.key)] = mcr
        return nil
    }
    err = mcat.file.Process(f, MASTER_RECORD, false)
    return
}

// Write master catalog file.
func (mcat *MasterCatalog) Save() (err error) {
    mcat.file.Open()
    defer mcat.file.Close()
    var pos LBUINT = 0
    for keystr, mcr := range mcat.index {
	    _, err = mcat.file.LockedWriteAt(PackMasterRecord(keystr, mcr), pos)
        if err != nil {return}
    }
    return
}

// Read the log file (given by the index) and build an associated index file.
func (lbase *Logbase) RefreshIndexfile(fnum LBUINT) (lfindex *Index) {
    lfile, err := lbase.GetLogfile(fnum)
    if err != nil {
        lbase.SetErr(err)
        return
    }
    lfindex, err = lfile.Index()
    if err != nil {
        lbase.SetErr(err)
        return
    }
    err = lfile.indexfile.Save(lfindex)
    if err != nil {lbase.SetErr(err)}
    return
}

// Read the log file (given by the index) and build an associated index file.
func (lbase *Logbase) ReadIndexfile(fnum LBUINT) (lfindex *Index) {
    lfile, err := lbase.GetLogfile(fnum)
    if err != nil {
        lbase.SetErr(err)
        return
    }
    lfindex, err = lfile.indexfile.Load()
    if err != nil {lbase.SetErr(err)}
    return
}
