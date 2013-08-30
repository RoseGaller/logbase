/*
	Defines constants whose specs are effectively frozen in the binary data of
	logbases.  Changes to these values are not backward compatible.
*/
package logbase

type LBTYPE		uint8 // Value type identifier
type CATID_TYPE uint64 // Catalog record id key type

// Keep these consistent!
const (
	LBTYPE_SIZE		int = 1 // bytes
	CATID_TYPE_SIZE	int = 8 // bytes
	LBUINT_MAX      int64 = 4294967295
	CRC_SIZE		LBUINT = 4
	VALOC_SIZE		LBUINT = LBUINT_SIZE_x3 + LBUINT(LBTYPE_SIZE)
)

const (
	APPNAME				string = "Logbase"
	CONFIG_FILENAME		string = "logbase.cfg"
	MASTER_CATALOG_NAME string = "master"
	ZAPMAP_FILENAME		string = ".zapmap"
	PERMISSIONS_DIR_NAME string = "users"
)

// Hard wire key/value types for all time.
const (
	// Fixed size types

	// Non-user space types (automated)
	LBTYPE_NIL			LBTYPE = 0
	LBTYPE_VALOC		LBTYPE = 10 // Location in log file of value bytes

	// User space types
	LBTYPE_UINT8		LBTYPE = 50
	LBTYPE_UINT16		LBTYPE = 51
	LBTYPE_UINT32		LBTYPE = 52
	LBTYPE_UINT64		LBTYPE = 53

	LBTYPE_INT8			LBTYPE = 70
	LBTYPE_INT16		LBTYPE = 71
	LBTYPE_INT32		LBTYPE = 72
	LBTYPE_INT64		LBTYPE = 74

	LBTYPE_FLOAT32		LBTYPE = 90
	LBTYPE_FLOAT64		LBTYPE = 91

	LBTYPE_COMPLEX64	LBTYPE = 110
	LBTYPE_COMPLEX128	LBTYPE = 111

	LBTYPE_CATID		LBTYPE = 121 // Catalog record id

	// Non-fixed size types
	// Only types with underlying []byte type after here

	// User space types
    LBTYPE_BYTES		LBTYPE = 170 // Cannot be a key type
	LBTYPE_STRING		LBTYPE = 171
	LBTYPE_LOCATION		LBTYPE = 173 // String of file path or URI

	LBTYPE_CATID_SET    LBTYPE = 180 // Set (no repeats) list of Catalog record ids
	LBTYPE_MAP			LBTYPE = 181 // map[string]*Field
	LBTYPE_LIST			LBTYPE = 182 // []interface{}

	// Non-user space types (automated)
	LBTYPE_CATKEY		LBTYPE = 190 // String Catalog Key
	LBTYPE_KIND			LBTYPE = 191 // Composite of LBTYPE_CATKEY and LBTYPE_CATID_SET
	LBTYPE_DOC			LBTYPE = 192 // Composite of LBTYPE_CATKEY and LBTYPE_MAP
)

