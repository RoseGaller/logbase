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
    "path"
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
    MASTER_INDEX_FILENAME   string = "master"
    ZAPMAP_FILENAME         string = "zapmap"
    STARTING_LOGFILE_NUMBER LBUINT = 1
)

// The basic unit of a logbase.
type Logfile struct {
	*Gofile
    fnum        LBUINT // log file number
    indexfile   *Indexfile // index for this logfile
}

// Init a Logfile.
func NewLogfile() *Logfile {
	return &Logfile{
        Gofile: &Gofile{},
        indexfile: NewIndexfile(),
    }
}

// Speed up initialisation of the master catalog.
type Indexfile struct {
    *Gofile
    *Index
}

// Init an Indexfile.
func NewIndexfile() *Indexfile {
	return &Indexfile{
        Gofile: &Gofile{},
        Index: &Index{},
    }
}

// Allow persistence of master catalog.
type Masterfile struct {
    *Gofile
    *MasterCatalog
}

// Init a Masterfile.
func NewMasterfile() *Masterfile {
	return &Masterfile{
        Gofile: &Gofile{},
        MasterCatalog: NewMasterCatalog(),
    }
}

// Allow persistence of scheduled kv pair deletion.
type Zapfile struct {
    *Gofile
    *Zapmap
}

// Init a Zapfile.
func NewZapfile() *Zapfile {
	return &Zapfile{
        Gofile: &Gofile{},
        Zapmap: NewZapmap(),
    }
}

// Logbase methods.

// Open a log file and its associated index file for read/write access.
// If none exist, create each.
func (lbase *Logbase) OpenLogfile(fnum LBUINT) (lfile *Logfile, err error) {
    fpath := lbase.MakeLogfilePath(fnum)
    gfile, err := lbase.OpenGofile(fpath)
    if err != nil {return}

    lfile = NewLogfile()
    lfile.Gofile = gfile
    lfile.path = fpath
    lfile.fnum = fnum

    ipath := lbase.MakeIndexfilePath(fnum)
    gfile, err = lbase.OpenGofile(ipath)

    lfif := NewIndexfile()
    lfif.Gofile = gfile
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
            lfile, err = lbase.OpenLogfile(STARTING_LOGFILE_NUMBER)
        } else {
            var last int = len(inds) - 1
            lfile, err = lbase.OpenLogfile(inds[last])
        }
        if err != nil {return lbase.SetErr(err)}
        lbase.livelog = lfile
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

        nscan++
        if nscan > 0 && stat.IsDir() {
            return filepath.SkipDir
        }
        parts := strings.Split(fpath, FILENAME_DELIMITER)
        if len(parts) == 2 && parts[1] == lbase.config.LOGFILE_NAME_EXTENSION {
            num64, err := strconv.ParseInt(parts[0], 10, 32)
            if err != nil {
                return err
            }

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

    err = filepath.Walk(lbase.path, scanner)
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
func (lbase *Logbase) MakeLogfilePath(fnum LBUINT) string {
    return path.Join(
        lbase.path,
        MakeLogfileName(fnum, lbase.config.LOGFILE_NAME_EXTENSION))
}

// Return the index file path associated with given the log file number.
func (lbase *Logbase) MakeIndexfilePath(fnum LBUINT) string {
    return path.Join(
        lbase.path,
        MakeIndexfileName(fnum, lbase.config.LOGFILE_NAME_EXTENSION))
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

// Close the log file and associated index file.
func (lfile *Logfile) CloseAll() error {
    err := lfile.Close()
    if err != nil {return err}
    return lfile.indexfile.Close()
}

// Index the given log file.
func (lfile *Logfile) Index() (*Index, error) {
    index := new(Index)
    f := func(rec *GenericRecord) error {
        irec := rec.ToLogRecord().ToIndexRecord()
        index.list = append(index.list, *irec)
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
    pos, _ := lfile.JumpFromEnd(0)
	_, err = lfile.LockedWriteAt(lrec.Pack(), pos)
    if err != nil {return}

    // Create a new file index record
    irec = lrec.ToIndexRecord()
	hsz := LBUINT(ParamSize(lrec.ksz) + ParamSize(lrec.vsz))
	irec.vpos = pos + hsz + irec.ksz

    // Update the in-memory file index
    lfile.indexfile.list = append(lfile.indexfile.list, *irec)

    // Write the index record to the index file
    pos, _ = lfile.indexfile.JumpFromEnd(0)
	_, err = lfile.indexfile.LockedWriteAt(irec.Pack(), pos)
	return
}

// Read a value from the log file.
func (lfile *Logfile) ReadVal(vpos, vsz LBUINT) ([]byte, error) {
	return lfile.LockedReadAt(vpos, vsz, "value")
}

// Log file index file methods.

// Read the index file.
func (ifile *Indexfile) Load() (lfindex *Index, err error) {
    lfindex = new(Index)
    f := func(rec *GenericRecord) error {
        irec := rec.ToIndexRecord()
        lfindex.list = append(lfindex.list, *irec)
        return nil
    }
    err = ifile.Process(f, INDEX_RECORD, false)
    return
}

// Write index file.
// TODO would it be faster to build a []byte and write once?
func (ifile *Indexfile) Save(lfindex *Index) (err error) {
    /*
    var nwrite int
    var err error
    var pos LBUINT = 0
    for i, rec := range lfindex.list {
	    nwrite, err = ifile.LockedWriteAt(rec.Pack(), pos)
        if err != nil {return err}
        pos = pos.plus(nwrite)
    }
    */
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
func (zfile *Zapfile) Read() (*Zapmap, error) {
    zm := NewZapmap()
    f := func(rec *GenericRecord) error {
        keystr, zrecs := rec.ToZapRecordList()
        zm.zapmap[keystr] = zrecs
        return nil
    }
    err := zfile.Process(f, ZAP_RECORD, true)
    return zm, err
}

// Write zapmap file.
func (zfile *Zapfile) Write(zm *Zapmap) error {
    var nwrite int
    var err error
    var pos LBUINT = 0
    for keystr, zrecs := range zm.zapmap {
	    nwrite, err = zfile.LockedWriteAt(PackZapRecord(keystr, zrecs), pos)
        if err != nil {return err}
        pos = pos.plus(nwrite)
    }
    return nil
}

// Master index file methods.

// Read master catalog file into a new master catalog.
func (mfile *Masterfile) Read() (*MasterCatalog, error) {
    mcat := NewMasterCatalog()
    f := func(rec *GenericRecord) error {
        mcr := rec.ToMasterCatalogRecord()
        mcat.index[string(rec.key)] = mcr
        return nil
    }
    err := mfile.Process(f, MASTER_RECORD, false)
    return mcat, err
}

// Write master catalog file.
func (mfile *Masterfile) Write(mcat *MasterCatalog) error {
    var err error
    var pos LBUINT = 0
    for keystr, mcr := range mcat.index {
	    _, err = mfile.LockedWriteAt(PackMasterRecord(keystr, mcr), pos)
        if err != nil {return err}
    }
    return err
}

// Read the log file (given by the index) and build an associated index file.
func (lbase *Logbase) RefreshIndexfile(fnum LBUINT) (lfindex *Index, err error) {
    var lfile *Logfile
    lfile, err = lbase.OpenLogfile(fnum)
    if err != nil {return}
    lfindex, err = lfile.Index()
    if err != nil {return}
    err = lfile.indexfile.Save(lfindex)
    lfile.CloseAll()
    return
}

// Read the log file (given by the index) and build an associated index file.
func (lbase *Logbase) ReadIndexfile(fnum LBUINT) (lfindex *Index, err error) {
    var lfile *Logfile
    lfile, err = lbase.OpenLogfile(fnum)
    if err != nil {return}
    lfindex, err = lfile.indexfile.Load()
    lfile.CloseAll()
    return
}
