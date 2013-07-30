/*
    Logbase is a so-called "log-structured database" which aims to be simple and fast.  It is a key-value store which can be used to build more complex data stores by allowing multiple keys and aiming to be lightweight (minimising complexity and configurability).  

    A logbase directory is specified in the configuration file (logbase.cfg).  Each logbase has its own directory, containing binary data files.

    Like Bitcask, new data is appended to the current log file in binary.  When this file reaches a user-defined size, a new log file is created for appending.  Each log record has a key and a value.  The structure of each record is shown below.

    TODO this is a little outdated, the crc is now at the end of the record

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
                           ValueLocationRecord     

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
	"path"
	"path/filepath"
    //"fmt"
)

type LBUINT uint32 // Unsigned Logbase integer type used on file

const (
    APPNAME         string = "Logbase"
    CONFIG_FILENAME string = "logbase.cfg"
    MASTER_FILENAME string = ".master"
    ZAPMAP_FILENAME string = ".zapmap"
    LBUINT_SIZE     LBUINT = 4 // bytes 
    LBUINT_SIZE_x2  LBUINT = 2 * LBUINT_SIZE
    LBUINT_SIZE_x3  LBUINT = 3 * LBUINT_SIZE
    LBUINT_SIZE_x4  LBUINT = 4 * LBUINT_SIZE
    LBUINT_MAX      int64 = 4294967295
)

// Logbase database instance.
type Logbase struct {
    name        string // Logbase name
	abspath     string // Logbase directory path
	livelog     *Logfile // The current (live) log file
    freg        *FileRegister
	mcat        *MasterCatalog
    zmap        *Zapmap
    config      *LogbaseConfiguration
    debug       *DebugLogger
    err         error // permits a more "fluent" api
    masterfile  *Masterfile
    zapfile     *Zapfile
}

// Make a new Logbase instance based on the given directory path.
func MakeLogbase(abspath string, debug *DebugLogger) *Logbase {
    lbase := NewLogbase()
    lbase.name = filepath.Base(abspath)
    lbase.abspath = abspath
    lbase.debug = debug
    return lbase
}

// Initialise embedded fields.
func NewLogbase() *Logbase {
	return &Logbase{
	    freg:   NewFileRegister(),
	    mcat:   NewMasterCatalog(),
	    zmap:   NewZapmap(),
    }
}

//  Index of all key-value pairs in a log file.
type Index struct {
    list    []*IndexRecord
}

//  Master catalog of all live (not stale) key-value pairs.
type MasterCatalog struct {
    index   map[string]*MasterCatalogRecord // The in-memory index
    file    *Masterfile
}

// Init a MasterCatalog.
func NewMasterCatalog() *MasterCatalog {
	return &MasterCatalog{
        index: make(map[string]*MasterCatalogRecord),
    }
}

//  Stale key-value pairs scheduled to be deleted from log files.
type Zapmap struct {
    zapmap  map[string][]*ZapRecord // "Zapmap"
    file    *Zapfile
}

// Init a Zapmap, which points to stale data scheduled for deletion.
func NewZapmap() *Zapmap {
	return &Zapmap{
        zapmap: make(map[string][]*ZapRecord),
    }
}

// Per Logbase configuration

// User space constants
type LogbaseConfiguration struct {
    LOGFILE_NAME_EXTENSION  string // Postfix for binary data log file names
    INDEXFILE_NAME_EXTENSION string // Postfix for binary "hint" file names
    LOGFILE_MAXBYTES        int // Size of live log file before spawning a new one
    FILE_LOCKING_ON         bool // Check presence of lock files before r/w
}

// Default configuration in case file is absent.
func DefaultConfig() *LogbaseConfiguration {
    return &LogbaseConfiguration{
        LOGFILE_NAME_EXTENSION:     "logbase",
        INDEXFILE_NAME_EXTENSION:   "index",
        LOGFILE_MAXBYTES:           1048576, // 1 MB
        FILE_LOCKING_ON:            true,
    }
}

// Load optional logbase configuration file parameters.
func LoadConfig(path string) (config *LogbaseConfiguration, err error) {
    _, err = os.Stat(path)
    if os.IsNotExist(err) {
        config = DefaultConfig()
        err = nil
        return
    }
    if err != nil {return}
    config = new(LogbaseConfiguration)
    _, err = toml.DecodeFile(path, &config)
    return
}

func (lbase *Logbase) SetErr(err error) *Logbase {
    lbase.err = err
    return lbase
}

// Check whether a live log file has been defined.
func (lbase *Logbase) HasLiveLog() bool {
    return lbase.livelog != nil
}

// Execute an orderly shutdown including finalisation of index and
// zap files.
func (lbase *Logbase) Close() *Logbase {
    // TODO update indexes
	return lbase
}

// If a valid master and zapmap file exists, load them, otherwise
// iterate through all log files in sequence.  Add each index file entry into
// the internal master catalog.  If the key already exists in the master,
// append the old master catalog record into a "zapmap" which schedules stale
// data for deletion.
func (lbase *Logbase) Init() *Logbase {
    lbase.debug.Fine(DEBUG_DEFAULT, "Commence init of logbase %q", lbase.name)
    // Make dir if it does not exist
    err := os.MkdirAll(lbase.abspath, DEFAULT_FILEMODE)
	if err != nil {return lbase.SetErr(err)}

    // Load optional logbase config file
    cfgPath := path.Join(lbase.abspath, CONFIG_FILENAME)
    config, errcfg := LoadConfig(cfgPath)
	if errcfg != nil {
        WrapError("Problem loading config file " + cfgPath, errcfg).Fatal()
    }
    lbase.config = config

    // Wire up the Master and Zapmap files
    mfile, err3 := lbase.GetFile(MASTER_FILENAME)
    if err3 != nil {
        WrapError(
            "Problem reading or creating master index file " +
            MASTER_FILENAME, err3).Fatal()
    }
    mfile.Touch()
    lbase.mcat.file = NewMasterfile(mfile)
    zfile, err4 := lbase.GetFile(ZAPMAP_FILENAME)
    if err4 != nil {
        WrapError(
            "Problem reading or creating zapmap file " +
            ZAPMAP_FILENAME, err4).Fatal()
    }
    zfile.Touch()
    lbase.zmap.file = NewZapfile(zfile)

    var buildmasterzap bool = true
    if lbase.mcat.file.size > 0 {
        err = lbase.mcat.Load()
        if err == nil {
            lbase.debug.Advise(DEBUG_DEFAULT, "Loaded master file")
            if lbase.zmap.file.size > 0 {
                err = lbase.zmap.Load()
                if err == nil {
                    lbase.debug.Advise(DEBUG_DEFAULT, "Loaded zap file")
                    buildmasterzap = false
                }
            }
        }
    }

    if buildmasterzap {
        lbase.debug.Advise(
            DEBUG_DEFAULT,
            "Could not find or load master and zapmap files, " +
            "build from index files...")
        // Get logfile list
        fpaths, fnums, err := lbase.GetLogfilePaths()
        if err != nil {return lbase.SetErr(err)}
        lbase.debug.Fine(DEBUG_DEFAULT, "fpaths = %v", fpaths)

        // Iterate through all log files
        var refresh bool
        for i, fnum := range fnums {
            lbase.debug.Fine(DEBUG_DEFAULT, "Scan log file %d index", fnum)
            refresh = false
            ipath := path.Join(lbase.abspath, lbase.MakeIndexfileRelPath(fnum))
            istat, err := os.Stat(ipath)
            if os.IsNotExist(err) || istat.Size() == 0 {
                refresh = true
            } else if err != nil {
                return lbase.SetErr(err)
            }
            fstat, err2 := os.Stat(fpaths[i])
            if err2 != nil {return lbase.SetErr(err2)}
            var lfindex *Index
            if fstat.Size() > 0 {
                if refresh {
                    lbase.debug.Advise(DEBUG_DEFAULT, "Refreshing index file %s", ipath)
                    lfindex = lbase.RefreshIndexfile(fnum)
                } else {
                    lbase.debug.Advise(DEBUG_DEFAULT, "Reading index file %s", ipath)
                    lfindex = lbase.ReadIndexfile(fnum)
                }
            }
            if lbase.err != nil {return lbase}
            for _, irec := range lfindex.list {
                lbase.Update(irec, fnum)
            }
        }
    }

    // Initialise livelog
    result := lbase.SetLiveLog()
    lbase.debug.Fine(DEBUG_DEFAULT, "Completed init of logbase %q", lbase.name)
    return result
}

// Update the master catalog map with an index record (usually) generated from
// an individual log file, and add an existing (stale) value entry to the
// zapmap.
func (lbase *Logbase) Update(irec *IndexRecord, fnum LBUINT) *Logbase {
    keystr := string(irec.key)
    newmcr := NewMasterCatalogRecord()
    newmcr.FromIndexRecord(irec, fnum)
    oldmcr, exists := lbase.mcat.index[keystr]

    if exists {
        // Add to zapmap
        var zrecs []*ZapRecord
        zrecs, _ = lbase.zmap.zapmap[keystr]
        zrec := NewZapRecord()
        zrec.FromMasterCatalogRecord(oldmcr)
        zrecs = append(zrecs, zrec)
        lbase.zmap.zapmap[keystr] = zrecs
    }

    // Update the master catalog
    lbase.mcat.index[keystr] = newmcr
    return lbase
}

// Save the key-value pair in the live log.
func (lbase *Logbase) Put(keystr string, val []byte) *Logbase {
    lbase.debug.Fine(DEBUG_DEFAULT,
        "Putting (%s,[%d]byte) into logbase %q",
        keystr, len(val), lbase.name)
	if lbase.HasLiveLog() {
        aftersize :=
            lbase.livelog.size +
            int(LBUINT_SIZE_x3) +
            len([]byte(keystr)) +
            len(val) +
            4 // crc

        //lbase.debug.Fine(DEBUG_DEFAULT, "CHECK aftersize = %v", aftersize)
        if aftersize > lbase.config.LOGFILE_MAXBYTES {
            lbase.NewLiveLog()
        }

        lrec := MakeLogRecord(keystr, val)
	    irec, err := lbase.livelog.StoreData(lrec)
        if err != nil {return lbase.SetErr(err)}
        return lbase.Update(irec, lbase.livelog.fnum)
	}
	return lbase.SetErr(ErrFileNotFound("Live log file is not defined"))
}

// Retrieve the value for the given key.
func (lbase *Logbase) Get(keystr string) (val []byte, err error) {
	mcr := lbase.mcat.index[keystr]
	if mcr == nil {
		err = FmtErrKeyNotFound(keystr)
		val = nil
	} else {
		return mcr.ReadVal(lbase)
	}

	return
}

func (lbase *Logbase) NewLiveLog() *Logbase {
    lfile, err := lbase.GetLogfile(lbase.livelog.fnum + 1)
    if err != nil {return lbase.SetErr(err)}
    lbase.livelog = lfile
    return lbase
}
