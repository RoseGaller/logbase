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
	"github.com/h00gs/gubed"
	"os"
	"path"
	"path/filepath"
)

// Logbase database instance.
type Logbase struct {
	name        string // Logbase name
	abspath     string // Logbase directory path
	permdir		string // Logbase user permissions sub-dir
	config      *LogbaseConfiguration
	debug       *gubed.Logger
	livelog     *Logfile // The current (live) log file
	mcat        *Catalog
	zmap        *Zapmap
	users		*Users
	catcache    *Cache  // CatalogCache cache
	filecache   *Cache  // File cache
	nodecache   *Cache  // Node cache
}

// Getters.

func (lbase *Logbase) Name() string {return lbase.name}
func (lbase *Logbase) AbsPath() string {return lbase.abspath}
func (lbase *Logbase) PermissionsDir() string {return lbase.permdir}
func (lbase *Logbase) Config() *LogbaseConfiguration {return lbase.config}
func (lbase *Logbase) Debug() *gubed.Logger {return lbase.debug}
func (lbase *Logbase) Livelog() *Logfile {return lbase.livelog}
func (lbase *Logbase) MasterCatalog() *Catalog {return lbase.mcat}
func (lbase *Logbase) Zapmap() *Zapmap {return lbase.zmap}
func (lbase *Logbase) Users() *Users {return lbase.users}
func (lbase *Logbase) CatalogCache() *Cache {return lbase.catcache}
func (lbase *Logbase) FileCache() *Cache {return lbase.filecache}
func (lbase *Logbase) NodeCache() *Cache {return lbase.nodecache}

// Make a new Logbase instance based on the given directory path.
func MakeLogbase(abspath string, debug *gubed.Logger) *Logbase {
	lbase := NewLogbase(debug)
	lbase.name = filepath.Base(abspath)
	lbase.abspath = abspath
	lbase.permdir = PERMISSIONS_DIR_NAME
	lbase.debug = debug
	// Cache Master Catalog
	lbase.catcache.Put(lbase.mcat.Name(), lbase.mcat)
	return lbase
}

// Initialise embedded fields.
func NewLogbase(debug *gubed.Logger) *Logbase {
	return &Logbase{
	    mcat:		MakeMasterCatalog(debug),
	    zmap:		MakeZapmap(debug),
		users:		NewUsers(),
	    catcache:	NewCache(),
	    filecache:	NewCache(),
	    nodecache:	NewCache(),
	}
}

// Per Logbase configuration

// User space constants
type LogbaseConfiguration struct {
	LOGFILE_NAME_EXTENSION  string // Postfix for binary data log file names
	INDEXFILE_NAME_EXTENSION string // Postfix for binary "hint" file names
	LOGFILE_MAXBYTES        int // Size of live log file before spawning a new one
	// Usually a Catalog holds only value locations, but if the
	// value is small enough, we can also keep it in RAM for speed 
	CACHE_VALUES			bool
	CACHE_VALUE_MAXSIZE		int
}

// Default configuration in case file is absent.
func DefaultConfig() *LogbaseConfiguration {
	return &LogbaseConfiguration{
		LOGFILE_NAME_EXTENSION:     "logbase",
		INDEXFILE_NAME_EXTENSION:   "index",
		LOGFILE_MAXBYTES:           1048576, // 1 MB
		CACHE_VALUES:				true, // cache in RAM
		CACHE_VALUE_MAXSIZE:        1024, // 1 KB
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
func (lbase *Logbase) Init(makeit bool) error {
	lbase.debug.Basic("Commence init of logbase %q", lbase.name)
	stat, err := os.Stat(lbase.abspath)
	if os.IsNotExist(err) {
		if makeit {
			// Make dir if it does not exist
			err = lbase.debug.Error(os.MkdirAll(lbase.abspath, 0777))
			if err != nil {return err}
		} else {
			return FmtErrDirNotFound(lbase.abspath)
		}
	} else {
		if !stat.Mode().IsDir() {
			return FmtErrDirNotFound(lbase.abspath)
		}
	}

	// Load optional logbase config file
	cfgPath := path.Join(lbase.abspath, CONFIG_FILENAME)
	config, errcfg := LoadConfig(cfgPath)
	lbase.debug.Error(errcfg)
	lbase.config = config

	// Wire up the Master and Zapmap files
	lbase.debug.Error(lbase.mcat.InitFile(lbase))
	var zfile *File
	zfile, _, err = lbase.GetFile(ZAPMAP_FILENAME)
	lbase.debug.Error(err)
	zfile.Touch()
	lbase.zmap.file = NewZapfile(zfile)

	var buildmasterzap bool = true
	if lbase.mcat.file.size > 0 {
		if lbase.debug.Error(lbase.mcat.Load(lbase)) == nil {
			lbase.debug.Advise("Loaded master file")
			buildmasterzap = false
			if lbase.zmap.file.size > 0 {
				if lbase.debug.Error(lbase.zmap.Load()) == nil {
					lbase.debug.Advise("Loaded zap file")
					buildmasterzap = false
				} else {
					buildmasterzap = true
				}
			}
		}
	}

	if buildmasterzap {
		lbase.debug.Advise(
			"Could not find or load master and zapmap files, " +
			"build from index files if present...")
		if err = lbase.debug.Error(lbase.Refresh(false)); err != nil {return err}
	}

	// Initialise livelog
	if err = lbase.debug.Error(lbase.SetLiveLog()); err != nil {return err}

	// Load other Catalogs, order important, must be done after
	// Master Catalog since other catalogs will use pointers to
	// existing Values or ValueLocations.
	catnames, err := lbase.GetCatalogNames()
	if len(catnames) > 0 {
		for _, name := range catnames {
			lbase.GetCatalog(name)
		}
	}

	lbase.debug.Advise("Completed init of logbase %q", lbase.name)
	return nil
}

// Save the key-value pair in the live log.  Handles the value type
// prepend into the value bytes.
func (lbase *Logbase) Put(key interface{}, vbyts []byte, vtype LBTYPE) (CatalogRecord, error) {
	if lbase.debug.GetLevel() > gubed.DEBUGLEVEL_ADVISE {
		lbase.debug.Basic(
			"Putting (%v,%s) into logbase %s",
			key, ValBytesToString(vbyts, vtype), lbase.name)
	}

	if lbase.HasLiveLog() {
		lrec := MakeLogRecord(key, vbyts, vtype, lbase.debug)
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
		var mcr CatalogRecord
		if lbase.config.CACHE_VALUES && lbase.OkToCacheValue(vbyts, vtype) {
			v := vloc.ToValue(vbyts, vtype)
			mcr = lbase.mcat.Update(key, v)
		} else {
			mcr = lbase.mcat.Update(key, vloc)
		}
		return mcr, nil
	}
	return nil, FmtErrLiveLogUndefined()
}

// Retrieve the value for the given key.  Snips off the value type
// prepend from the value bytes.
func (lbase *Logbase) Get(key interface{}) (vbyts []byte, vtype LBTYPE, mcr CatalogRecord, err error) {
	mcr = lbase.mcat.Get(key)
	if mcr == nil {
		err = nil
		vbyts = nil
		vtype = LBTYPE_NIL
	} else {
		vbyts, vtype, err = mcr.ReadVal(lbase)
		if lbase.config.CACHE_VALUES && lbase.OkToCacheValue(vbyts, vtype) {
			if vloc, ok := mcr.(*ValueLocation); ok {
				mcr := vloc.ToValue(vbyts, vtype)
				lbase.mcat.Put(key, mcr)
			}
		}
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
	return err
}

// Regenerate the Master Catalog and Zapmap.  If the given switch
// forceIndexRefresh is on, refresh each logfile index file, otherwise only
// refresh each index if it is not present.
func (lbase *Logbase) Refresh(forceIndexRefresh bool) error {
	lbase.mcat.ResetId()
	// Get logfile list
	fpaths, fnums, err2 := lbase.GetLogfilePaths()
	if lbase.debug.Error(err2) != nil {return err2}
	if len(fnums) == 0 {
		lbase.debug.Advise("This appears to be a new logbase")
		return nil
	}

	// Iterate through all log files
	var refreshIndex bool
	for i, fnum := range fnums {
		lbase.debug.Fine("Scan log file %d index", fnum)
		refreshIndex = forceIndexRefresh
		ipath := path.Join(lbase.abspath, lbase.MakeIndexfileRelPath(fnum))
		istat, err := os.Stat(ipath)
		if os.IsNotExist(err) || istat.Size() == 0 {
			refreshIndex = true
		} else if err != nil {
			return err
		}
		fstat, err3 := os.Stat(fpaths[i])
		if lbase.debug.Error(err3) != nil {return err3}
		var lfindex *Index
		if fstat.Size() > 0 {
			if refreshIndex {
				lbase.debug.Basic("Refreshing index file %s", ipath)
				lfindex, err = lbase.RefreshIndexfile(fnum)
			} else {
				lbase.debug.Basic("Reading index file %s", ipath)
				lfindex, err = lbase.ReadIndexfile(fnum)
			}
		}
		if lbase.debug.Error(err) != nil {return err}
		for _, irec := range lfindex.List {
			key, vloc := lbase.UpdateZapmap(irec, fnum)
			lbase.mcat.Update(key, vloc)
		}
	}
	return nil
}
