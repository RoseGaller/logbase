/*
    Logbase is a so-called "log-structured database" which aims to be simple and fast.  It is a key-value store which can be used to build more complex data stores by allowing multiple keys and aiming to be lightweight (minimising complexity and configurability).  

    A logbase directory is specified in the configuration file (logbase.cfg).  Each logbase has its own directory, containing binary data files.

    Like Bitcask, new data is appended to the current log file in binary.  When this file reaches a user-defined size, a new log file is created for appending.  Each log record has a key and a value.  The structure of each record is shown below.

   	+--------------------------------+ --+                          --+
	|           crc (LBUINT)         |   |                            |
    +--------------------------------+   +-- header                   |
    |    key size, bytes (LBUINT)    |   |   (RECORD_HEADER_SIZE)     +-- keyed header
    +--------------------------------+   |                            |
    |   value size, bytes (LBUINT)   |   |                            |
    +--------------------------------+ --+                            |
    |        key data ([]byte)       |                                |
    +--------------------------------+                              --+
    |       value data ([]byte)      |
    +--------------------------------+

    Each record starts with a checksum (crc).

    Log files are numbered sequentially from 0.  An associated index file is created for each log file which speeds up the initialisation of a single, in-memory master key index.

    Each log file has an associated index file containing records with the following structure:

    +--------------------------------+ --+
    |    key size, bytes (LBUINT)    |   |
    +--------------------------------+   |
    |   value size, bytes (LBUINT)   |   +-- IndexRecordHeader
    +--------------------------------+   |   (INDEX_HEADER_SIZE)
    | value position, bytes (LBUINT) |   |
    +--------------------------------+ --+
    |        key data ([]byte)       |
    +--------------------------------+


    In memory, we keep the master catalog as a map in which each entry includes the file number fnum:

    +--------------+    +------+------+------+
    | key (string) | -> | fnum | vsz  | vpos | (all LBUINT)
    +--------------+    +------+------+------+
                        |                    |
                        +---------+----------+
                                  |
                           MasterCatalogRecord     

    This master catalog is saved to file regularly in case of a surprise shutdown, with the following record format:

    +--------------------------------+ --+-- MasterCatalogHeader
    |    key size, bytes (LBUINT)    |   |   (MASTER_HEADER_SIZE)
    +--------------------------------+ --+
    |        key data ([]byte)       |
    +--------------------------------+
    |      file number (LBUINT)      |
    +--------------------------------+
    |   value size, bytes (LBUINT)   |
    +--------------------------------+
    | value position, bytes (LBUINT) |
    +--------------------------------+

    Over time, as changes are made to the values of existing keys, the old values become a file storage burden.  We maintain a map of stale key-value pair data to be "zapped" at some convenient time.  This map is saved to file regularly in case of a surprise shutdown.

    +--------------+    +------+------+------+
    | key (string) | -> | fnum | vsz  | vpos | --- ZapRecord
    +--------------+    +------+------+------+
                        | fnum | vsz  | vpos |
                        +------+------+------+
                        | fnum | vsz  | vpos |
                        +------+------+------+

    A new key-value pair is appended to the live log and a new entry is created in the master catalog.  A new value for an existing key is also appended to the live log, the master catalog map is updated and the old master catalog record is appended to the zapmap list for the key.  The records in the zapfile for a logbase are formatted according to:

    +--------------------------------+ --+
    |    key size, bytes (LBUINT)    |   |
    +--------------------------------+   +-- ZapRecordHeader
    |     n stale records (LBUINT)   |   |   (ZAP_HEADER_SIZE)
    +--------------------------------+ --+
    |        key data ([]byte)       |
    +--------------------------------+ --+
    |      file number (LBUINT)      |   |
    +--------------------------------+   |
    |   value size, bytes (LBUINT)   |   +-- repeat n times
    +--------------------------------+   |
    | value position, bytes (LBUINT) |   |
    +--------------------------------+ --+

    Thanks to André Luiz Alves Moraes for the gocask demonstration code from which I drew inspiration while learning Go.
*/
package logbase

import (
    "github.com/h00gs/toml"
	"os"
    "io"
	"path"
	"path/filepath"
    "strings"
    "fmt"
)

type LBUINT uint32 // Unsigned Logbase integer type used on file

const (
    APPNAME         string = "LOGBASE"
    DEBUG_FILENAME  string = "debug.log"
    CONFIG_FILENAME string = "logbase.cfg"
    LBUINT_SIZE     LBUINT = 4 // bytes 
    LBUINT_SIZE_x2  LBUINT = 2 * LBUINT_SIZE
    LBUINT_SIZE_x3  LBUINT = 3 * LBUINT_SIZE
    LBUINT_SIZE_x4  LBUINT = 4 * LBUINT_SIZE
    LBUINT_MAX      int64 = 4294967295
)

var logbases map[string]*Logbase = make(map[string]*Logbase)

// Logbase database instance.
type Logbase struct {
    name        string // Logbase name
	path        string // Logbase directory path
	livelog     *Logfile // The current (live) log file
    *FileRegister
	*MasterCatalog
    *Zapmap
    config      *Configuration
}

//  Index of all key-value pairs in a log file.
type Index struct {
    list    []IndexRecord
}

//  Master catalog of all live (not stale) key-value pairs.
type MasterCatalog struct {
    index   map[string]*MasterCatalogRecord // The in-memory index
}

// Init a MasterCatalog.
func NewMasterCatalog() *MasterCatalog {
	return &MasterCatalog{index: make(map[string]*MasterCatalogRecord)}
}

//  Stale key-value pairs scheduled to be deleted from log files.
type Zapmap struct {
    zapmap  map[string][]ZapRecord // "Zapmap"
}

// Init a Zapmap, which points to stale data scheduled for deletion.
func NewZapmap() *Zapmap {
	return &Zapmap{zapmap: make(map[string][]ZapRecord)}
}

// Per Logbase configuration

// User space constants
type Configuration struct {
    LOGFILE_NAME_EXTENSION  string // Postfix for binary data log file names
    INDEXFILE_NAME_EXTENSION string // Postfix for binary "hint" file names
    LOGFILE_MAXBYTES        int // Size of live log file before spawning a new one
    FILE_LOCKING_ON         bool // Check presence of lock files before r/w
}

// Default configuration.
func DefaultConfig() *Configuration {
    return &Configuration{
        LOGFILE_NAME_EXTENSION:     "logbase",
        INDEXFILE_NAME_EXTENSION:   "index",
        LOGFILE_MAXBYTES:           1048576, // 1 MB
        FILE_LOCKING_ON:            true,
    }
}

// Load optional logbase configuration file parameters.
func LoadConfig(path string) (config *Configuration, err error) {
    _, err = os.Stat(path)
    if os.IsNotExist(err) {
        config = DefaultConfig()
        err = nil
        return
    }
    if err != nil {return}
    config = new(Configuration)
    _, err = toml.Decode(path, &config)
    return
}

// Debugging

var Debug *DebugLogger

// Initialise a global debug logger writing to the screen and a file.
func InitDebugLogger() {
    file, err := OpenFile(DEBUG_FILENAME)
	if err != nil {WrapError("Could not open debug log: ", err).Fatal()}
    writers := []io.Writer{os.Stdout, file}
    level := DebugLevels["BASIC"]
    Debug = NewDebugLogger(level, writers)
    Debug.StampedPrintln(">>> LOGBASE DEBUG LOG STARTED")
    Debug.Println("")
    return
}

// Allow the debug level to be changed on the fly.
func SetDebugLevel(levelstr string) {
    level, ok := DebugLevels[strings.ToUpper(levelstr)]
	if !ok {FmtErrKeyNotFound(levelstr).Fatal()}
    Debug.level = level
    return
}

// Open an existing Logbase or create it if necessary, identified by a
// directory path.
func Open(lbPath string) (lbase *Logbase, err error) {
    fmt.Println("CHECK001")
    if Debug == nil {InitDebugLogger()}

    // Use existing Logbase if present
    lbase, present := logbases[lbPath]
    if present {return}

	err = os.MkdirAll(lbPath, DEFAULT_FILEMODE)
	if err != nil {return}

	lbase = &Logbase{
        name: filepath.Base(lbPath),
        path: lbPath,
    }

    cfgPath := path.Join(lbPath, CONFIG_FILENAME)

    config, errcfg := LoadConfig(cfgPath)
	if errcfg != nil {
        WrapError("Problem loading config file " + cfgPath, errcfg).Fatal()
    }

    lbase.config = config
	lbase.MasterCatalog = NewMasterCatalog()
	lbase.Zapmap = NewZapmap()
	lbase.FileRegister = NewFileRegister()

    err = lbase.Init()
	if err != nil {return}

    // Initialise livelog
	err = lbase.SetLiveLog()
	if err != nil {return}

	return
}

// Check whether a live log file has been defined.
func (lbase *Logbase) HasLiveLog() bool {
    return lbase.livelog != nil
}

// Execute an orderly shutdown including finalisation of index and
// zap files.
func (lbase *Logbase) Close() error {
	var err error
	if lbase.HasLiveLog() {
		err = lbase.livelog.file.Close()
	}
	return err
}

// Iterate through all log files in sequence.  Add each index file entry into
// the internal master catalog.  If the key already exists in the master,
// append the old master catalog record into a "zapmap" which schedules stale
// data for deletion.
func (lbase *Logbase) Init() error {
    fpaths, fnums, err := lbase.GetLogfilePaths()
    if err != nil {return err}

    var refresh bool
    for i, fnum := range fnums {
        refresh = false
        ipath := lbase.MakeIndexfilePath(fnum)
        istat, err := os.Stat(ipath)
        if os.IsNotExist(err) {
            refresh = true
        } else if err != nil {
            return err
        } else {
            if istat.Size() == 0 {
                if fstat, _ := os.Stat(fpaths[i]); fstat.Size() > 0 {
                    // The log file is not empty, so the index file
                    // should not be empty
                    refresh = true
                }
            }
        }
        if refresh {
            _, err = lbase.RefreshIndexfile(fnum)
        } else {
            _, err = lbase.ReadIndexfile(fnum)
        }
        if err != nil {return err}
    }

    return nil
}

// Update the master catalog map with an index record (usually) generated from
// an individual log file, and add an existing (stale) value entry to the
// zapmap.
func (lbase *Logbase) Update(irec *IndexRecord, fnum LBUINT) {
    keystr := string(irec.key)
    newmcr := NewMasterCatalogRecord()
    newmcr.FromIndexRecord(irec, fnum)
    oldmcr, exists := lbase.index[keystr]

    if exists {
        // Add to zapmap
        var zrecs []ZapRecord
        zrecs, _ = lbase.zapmap[keystr]
        zrec := NewZapRecord()
        zrec.FromMasterCatalogRecord(oldmcr)
        zrecs = append(zrecs, *zrec)
        lbase.zapmap[keystr] = zrecs
    }

    // Update the master catalog
    lbase.index[keystr] = newmcr
}

// Save the key-value pair in the live log.
func (lbase *Logbase) Put(keystr string, val []byte) error {
	if lbase.HasLiveLog() {
        nbytes :=
            int(LBUINT_SIZE_x3) +
            len([]byte(keystr)) +
            len(val) +
            4 // crc

        if nbytes > lbase.config.LOGFILE_MAXBYTES {
            lbase.NewLiveLog()
        }

        lrec := MakeLogRecord(keystr, val)
	    irec, err := lbase.livelog.StoreData(lrec)
        if err != nil {return err}
        lbase.Update(irec, lbase.livelog.fnum)
        return nil
	}
	return ErrFileNotFound("Live log file is not defined")
}

// Retrieve the value for the given key.
func (lbase *Logbase) Get(keystr string) (val []byte, err error) {
	mcr := lbase.index[keystr]
	if mcr == nil {
		err = FmtErrKeyNotFound(keystr)
		val = nil
	} else {
		return mcr.ReadVal(lbase)
	}

	return
}

func (lbase *Logbase) NewLiveLog() error {
    lbase.livelog.CloseAll()
    lfile, err := lbase.OpenLogfile(lbase.livelog.fnum + 1)
    if err != nil {return err}
    lbase.livelog = lfile
    return nil
}
