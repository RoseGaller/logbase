/*
	Defines constants whose specs are effectively frozen in the binary data of
	logbases.  Changes to these values are not backward compatible.
*/
package logbase

import (
	"bytes"
	"bufio"
	"encoding/binary"
)

type LBUINT uint32 // Unsigned Logbase integer type used on file
type LBTYPE uint8 // Value type identifier
type MCID_TYPE uint64 // Master Catalog record id key type

// Keep these consistent
const (
	LBUINT_SIZE		LBUINT = 4 // bytes 
	LBTYPE_SIZE		int = 1
	LBUINT_MAX      int64 = 4294967295
	CRC_SIZE		LBUINT = 4
	VALOC_SIZE		LBUINT = LBUINT_SIZE_x3 + LBUINT(LBTYPE_SIZE)
)

// Hard wire key/value types for all time.
const (
	LBTYPE_NIL			LBTYPE = 0
	// Fixed size types
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

	LBTYPE_VALOC		LBTYPE = 120 // Location in log file of value bytes
	LBTYPE_MCID			LBTYPE = 121 // Master Catalog record id key type
	LBTYPE_MAP			LBTYPE = 125 // map[interface{}]interface{}
	LBTYPE_LIST			LBTYPE = 130 // []interface{}

	// Only types with underlying []byte type after here
    LBTYPE_BYTEARRAY	LBTYPE = 170 // Cannot be a key type
	LBTYPE_STRING		LBTYPE = 171
	LBTYPE_GOB			LBTYPE = 172
	LBTYPE_LOCATION		LBTYPE = 173 // String of file path or URI

	LBTYPE_MCK			LBTYPE = 190 // String Master Catalog Key
	LBTYPE_DOC			LBTYPE = 191 // Composite of LBTYPE_MCK and LBTYPE_MAP
)

// Keys

// Keys can only be a subset of the LBTYPEs.
func NewKey(ktype LBTYPE, debug *DebugLogger) interface{} {
	switch ktype {
	case LBTYPE_UINT8:
		var p uint8
		return interface{}(p)
	case LBTYPE_UINT16:
		var p uint16
		return interface{}(p)
	case LBTYPE_UINT32:
		var p uint32
		return interface{}(p)
	case LBTYPE_UINT64:
		var p uint64
		return interface{}(p)
	case LBTYPE_INT8:
		var p int8
		return interface{}(p)
	case LBTYPE_INT16:
		var p int16
		return interface{}(p)
	case LBTYPE_INT32:
		var p int32
		return interface{}(p)
	case LBTYPE_INT64:
		var p int64
		return interface{}(p)
	case LBTYPE_FLOAT32:
		var p float32
		return interface{}(p)
	case LBTYPE_FLOAT64:
		var p float64
		return interface{}(p)
	case LBTYPE_COMPLEX64:
		var p complex64
		return interface{}(p)
	case LBTYPE_COMPLEX128:
		var p complex128
		return interface{}(p)
	case LBTYPE_MCID:
		var p MCID_TYPE
		return interface{}(p)
	case LBTYPE_STRING:
		var p string
		return interface{}(p)
	default:
		debug.Error(FmtErrBadType("Bad key type: %d", ktype))
	}
    return nil
}

func GetKey(kbyts []byte, ktype LBTYPE, debug *DebugLogger) (key interface{}) {
	if IsFixedSizeType(ktype) {
		key = NewKey(ktype, debug)
		bfr := bufio.NewReader(bytes.NewBuffer(kbyts))
		binary.Read(bfr, BIGEND, &key)
	} else {
		// Must be a string
		key = string(kbyts)
	}
    return
}

func IsFixedSizeType(typ LBTYPE) bool {
    return typ < LBTYPE_BYTEARRAY
}

func GetKeyType(key interface{}, debug *DebugLogger) LBTYPE {
	switch ktype := key.(type) {
	case uint8:
		return LBTYPE_UINT8
	case uint16:
		return LBTYPE_UINT16
	case uint32:
		return LBTYPE_UINT32
	case uint64:
		return LBTYPE_UINT64
	case int8:
		return LBTYPE_INT8
	case int16:
		return LBTYPE_INT16
	case int32:
		return LBTYPE_INT32
	case int64:
		return LBTYPE_INT64
	case float32:
		return LBTYPE_FLOAT32
	case float64:
		return LBTYPE_FLOAT64
	case complex64:
		return LBTYPE_COMPLEX64
	case complex128:
		return LBTYPE_COMPLEX128
	case MCID_TYPE:
		return LBTYPE_MCID
	case string:
		return LBTYPE_STRING
	default:
		debug.Error(FmtErrBadType("Unrecognised key type: %d", ktype))
	}
    return LBTYPE_NIL
}

