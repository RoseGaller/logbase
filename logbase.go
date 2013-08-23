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
)

const (
	APPNAME         string = "Logbase"
	CONFIG_FILENAME string = "logbase.cfg"
	MASTER_FILENAME string = ".master"
	ZAPMAP_FILENAME string = ".zapmap"
	PERMISSIONS_DIR_NAME string = "users"
	LBUINT_SIZE_x2  LBUINT = 2 * LBUINT_SIZE
	LBUINT_SIZE_x3  LBUINT = 3 * LBUINT_SIZE
	LBUINT_SIZE_x4  LBUINT = 4 * LBUINT_SIZE
)

// Logbase database instance.
type Logbase struct {
	name        string // Logbase name
	abspath     string // Logbase directory path
	permdir		string // Logbase user permissions sub-dir
	config      *LogbaseConfiguration
	debug       *DebugLogger
	livelog     *Logfile // The current (live) log file
	freg        *FileRegister
	mcat        *MasterCatalog
	zmap        *Zapmap
	users		*Users
}

// Make a new Logbase instance based on the given directory path.
func MakeLogbase(abspath string, debug *DebugLogger) *Logbase {
	lbase := NewLogbase()
	lbase.name = filepath.Base(abspath)
	lbase.abspath = abspath
	lbase.permdir = PERMISSIONS_DIR_NAME
	lbase.debug = debug
	return lbase
}

// Initialise embedded fields.
func NewLogbase() *Logbase {
	return &Logbase{
	    freg:   NewFileRegister(),
	    mcat:   NewMasterCatalog(),
	    zmap:   NewZapmap(),
		users:	NewUsers(),
	}
}

// Per Logbase configuration

// User space constants
type LogbaseConfiguration struct {
	LOGFILE_NAME_EXTENSION  string // Postfix for binary data log file names
	INDEXFILE_NAME_EXTENSION string // Postfix for binary "hint" file names
	LOGFILE_MAXBYTES        int // Size of live log file before spawning a new one
	// Usually the Master Catalog holds only value locations, but if the
	// value is small enough, we can also keep it in RAM for speed 
	MCAT_VALUE_MAXSIZE		int
}

// Default configuration in case file is absent.
func DefaultConfig() *LogbaseConfiguration {
	return &LogbaseConfiguration{
		LOGFILE_NAME_EXTENSION:     "logbase",
		INDEXFILE_NAME_EXTENSION:   "index",
		LOGFILE_MAXBYTES:           1048576, // 1 MB
		MCAT_VALUE_MAXSIZE:         1024, // 1 KB
	}
}

// Load optional logbase configuration file parameters.
func LoadConfig(path string) (config *LogbaseConfiguration, err error) {
	config = DefaultConfig()
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		err = nil
		return
	}
	if err != nil {return}
	_, err = toml.DecodeFile(path, &config)
	return
}

// Check whether a live log file has been defined.
func (lbase *Logbase) HasLiveLog() bool {
	return lbase.livelog != nil
}

// Execute an orderly shutdown including finalisation of index and
// zap files.
func (lbase *Logbase) Close() error {
	lbase.debug.Advise("Closing logbase %q...", lbase.name)
	return lbase.Save()
}

// If a valid master and zapmap file exists, load them, otherwise
// iterate through all log files in sequence.  Add each index file entry into
// the internal master catalog.  If the key already exists in the master,
// append the old master catalog record into a "zapmap" which schedules stale
// data for deletion.
func (lbase *Logbase) Init(user, passhash string) error {
	lbase.debug.Basic("Commence init of logbase %q", lbase.name)
	// Make dir if it does not exist
	err := lbase.debug.Error(os.MkdirAll(lbase.abspath, 0777))
	if err != nil {return err}

	// Load optional logbase config file
	cfgPath := path.Join(lbase.abspath, CONFIG_FILENAME)
	config, errcfg := LoadConfig(cfgPath)
	lbase.debug.Error(errcfg)
	lbase.config = config

	// Wire up the Master and Zapmap files
	var mfile *File
	mfile, err = lbase.GetFile(MASTER_FILENAME)
	lbase.debug.Error(err)
	mfile.Touch()
	lbase.mcat.file = NewMasterfile(mfile)
	var zfile *File
	zfile, err = lbase.GetFile(ZAPMAP_FILENAME)
	lbase.debug.Error(err)
	zfile.Touch()
	lbase.zmap.file = NewZapfile(zfile)

	var buildmasterzap bool = true
	if lbase.mcat.file.size > 0 {
		if lbase.debug.Error(lbase.mcat.Load(lbase.debug)) == nil {
			lbase.debug.Advise("Loaded master file")
			if lbase.zmap.file.size > 0 {
				if lbase.debug.Error(lbase.zmap.Load(lbase.debug)) == nil {
					lbase.debug.Advise("Loaded zap file")
					buildmasterzap = false
				}
			}
		}
	}

	if buildmasterzap {
		ResetMCID()
		lbase.debug.Advise(
			"Could not find or load master and zapmap files, " +
			"build from index files...")
		// Get logfile list
		fpaths, fnums, err2 := lbase.GetLogfilePaths()
		if lbase.debug.Error(err2) != nil {return err2}
		if len(fnums) == 0 {
			lbase.debug.Advise("This appears to be a new logbase")
		}

		// Iterate through all log files
		var refresh bool
		for i, fnum := range fnums {
			lbase.debug.Fine("Scan log file %d index", fnum)
			refresh = false
			ipath := path.Join(lbase.abspath, lbase.MakeIndexfileRelPath(fnum))
			istat, err := os.Stat(ipath)
			if os.IsNotExist(err) || istat.Size() == 0 {
				refresh = true
			} else if err != nil {
				return err
			}
			fstat, err3 := os.Stat(fpaths[i])
			if lbase.debug.Error(err3) != nil {return err3}
			var lfindex *Index
			if fstat.Size() > 0 {
				if refresh {
					lbase.debug.Basic("Refreshing index file %s", ipath)
					lfindex, err = lbase.RefreshIndexfile(fnum)
				} else {
					lbase.debug.Basic("Reading index file %s", ipath)
					lfindex, err = lbase.ReadIndexfile(fnum)
				}
			}
			if lbase.debug.Error(err) != nil {return err}
			for _, irec := range lfindex.list {
				key, vloc := lbase.UpdateZapmap(irec, fnum)
				lbase.UpdateMasterCatalog(key, vloc)
			}
		}
	}

	// Initialise livelog
	if err = lbase.debug.Error(lbase.SetLiveLog()); err != nil {return err}

	// User
	err = lbase.InitSecurity(user, passhash)
	if err != nil {return err}

	lbase.debug.Advise("Completed init of logbase %q", lbase.name)
	return nil
}

// Save the key-value pair in the live log.  Handles the value type
// prepend into the value bytes.
func (lbase *Logbase) Put(key interface{}, val []byte, vtype LBTYPE) (MasterCatalogRecord, error) {
	//lbase.debug.SuperFine(
	//	"Putting (%v,[%d]byte) into logbase %s",
	//	key, len(val), lbase.name)
	if lbase.debug.GetLevel() > DEBUGLEVEL_ADVISE {
		lbase.debug.Basic(
			"Putting (%v,%s) into logbase %s",
			key, ValToString(val, vtype), lbase.name)
	}

	if lbase.HasLiveLog() {
		lrec := MakeLogRecord(key, val, vtype, lbase.debug)
		aftersize := lbase.livelog.size + len(lrec.Pack())
		if aftersize > lbase.config.LOGFILE_MAXBYTES {
			lbase.NewLiveLog()
		}

		// Store data immediately to file
	    irec, err := lbase.livelog.StoreData(lrec)
		if lbase.debug.Error(err) != nil {return nil, err}
		// Schedule old data for zapping
		_, vloc := lbase.UpdateZapmap(irec, lbase.livelog.fnum)

		// Update Master Catalog in RAM with value or its location
		storeValueInRAM := len(val) + LBTYPE_SIZE <= lbase.config.MCAT_VALUE_MAXSIZE
		var mcr MasterCatalogRecord
		if storeValueInRAM {
			v := NewValue()
			v.ValueLocation = vloc
			v.vbyts = val
			v.vtype = vtype
			mcr = lbase.UpdateMasterCatalog(key, v)
		} else {
			mcr = lbase.UpdateMasterCatalog(key, vloc)
		}
		return mcr, nil
	}
	return nil, ErrFileNotFound("Live log file is not defined")
}

// Retrieve the value for the given key.  Snips off the value type
// prepend from the value bytes.
func (lbase *Logbase) Get(key interface{}) (vbyts []byte, vtype LBTYPE, err error) {
	mcr := lbase.mcat.Get(key)
	if mcr == nil {
		err = FmtErrKeyNotFound(key)
		vbyts = nil
		vtype = LBTYPE_NIL
	} else {
		vbyts, vtype, err = mcr.ReadVal(lbase)
	}

	return
}

func (lbase *Logbase) NewLiveLog() error {
	lfile, err := lbase.GetLogfile(lbase.livelog.fnum + 1)
	if err != nil {return err}
	lbase.livelog = lfile
	return nil
}

func (lbase *Logbase) Zap(bufsz LBUINT) error {
	_, fnums, err := lbase.GetLogfilePaths()
	if err != nil {return err}
	for _, fnum := range fnums {
		lfile, err := lbase.GetLogfile(fnum)
		if err != nil {return err}
		err = lfile.Zap(lbase.zmap, bufsz)
		if err != nil {return err}
	}
	return nil
}
