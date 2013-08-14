/*
	Defines constants whose specs are effectively frozen in the binary data of
	logbases.  Changes to these values are not backard compatible.
*/
package logbase

type LBUINT uint32 // Unsigned Logbase integer type used on file
type KVTYPE uint8 // Value type identifier

// Keep these consistent
const (
	LBUINT_SIZE		LBUINT = 4 // bytes 
	KVTYPE_SIZE		int = 1
	LBUINT_MAX      int64 = 4294967295
	CRC_SIZE		LBUINT = 4
)

// Hard wire key/value types for all time.
const (
	KVTYPE_NIL			KVTYPE = 0
	// Fixed size types
	KVTYPE_UINT8		KVTYPE = 1
	KVTYPE_UINT16		KVTYPE = 2
	KVTYPE_UINT32		KVTYPE = 3
	KVTYPE_UINT64		KVTYPE = 4

	KVTYPE_INT8			KVTYPE = 10
	KVTYPE_INT16		KVTYPE = 11
	KVTYPE_INT32		KVTYPE = 12
	KVTYPE_INT64		KVTYPE = 13

	KVTYPE_FLOAT32		KVTYPE = 20
	KVTYPE_FLOAT64		KVTYPE = 21

	KVTYPE_COMPLEX64	KVTYPE = 30
	KVTYPE_COMPLEX128	KVTYPE = 31

	// Only types with underlying []byte type after here
    KVTYPE_BYTEARRAY	KVTYPE = 150 // Cannot be a key type
	KVTYPE_STRING		KVTYPE = 151
	KVTYPE_GOB			KVTYPE = 152

	KVTYPE_LOCATION		KVTYPE = 160 // String of file path or URI
)
