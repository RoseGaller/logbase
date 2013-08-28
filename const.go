/*
	Defines constants whose specs are effectively frozen in the binary data of
	logbases.  Changes to these values are not backward compatible.
*/
package logbase

import (
	"fmt"
	"bytes"
	"encoding/binary"
)

type LBUINT uint32 // Unsigned Logbase integer type used on file
type LBTYPE uint8 // Value type identifier
type MCID_TYPE uint64 // Master Catalog record id key type

// Keep these consistent!
const (
	LBUINT_SIZE		LBUINT = 4 // bytes 
	LBTYPE_SIZE		int = 1 // bytes
	MCID_TYPE_SIZE	int = 8 // bytes
	LBUINT_MAX      int64 = 4294967295
	CRC_SIZE		LBUINT = 4
	VALOC_SIZE		LBUINT = LBUINT_SIZE_x3 + LBUINT(LBTYPE_SIZE)
)

const (
	LBUINT_SIZE_x2  LBUINT = 2 * LBUINT_SIZE
	LBUINT_SIZE_x3  LBUINT = 3 * LBUINT_SIZE
	LBUINT_SIZE_x4  LBUINT = 4 * LBUINT_SIZE
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

	LBTYPE_MCID		    LBTYPE = 121 // Master Catalog record id

	// Non-fixed size types
	// Only types with underlying []byte type after here

	// User space types
    LBTYPE_BYTES		LBTYPE = 170 // Cannot be a key type
	LBTYPE_STRING		LBTYPE = 171
	LBTYPE_LOCATION		LBTYPE = 173 // String of file path or URI

	LBTYPE_MCID_SET     LBTYPE = 180 // Set (no repeats) list of Master Catalog record ids
	LBTYPE_MAP			LBTYPE = 181 // map[string]*Field
	LBTYPE_LIST			LBTYPE = 182 // []interface{}

	// Non-user space types (automated)
	LBTYPE_MCK			LBTYPE = 190 // String Master Catalog Key
	LBTYPE_KIND			LBTYPE = 191 // Composite of LBTYPE_MCK and LBTYPE_MCID_SET
	LBTYPE_DOC			LBTYPE = 192 // Composite of LBTYPE_MCK and LBTYPE_MAP
)

// Keys

// Keys can only be a subset of the LBTYPEs.
func MakeKey(kbyts []byte, ktype LBTYPE, debug *DebugLogger) (interface{}, error) {
	if IsAllowableKey(ktype) {
		return MakeTypeFromBytes(kbyts, ktype)
	} else {
		err := debug.Error(FmtErrBadType("Bad key type: %d", ktype))
		return nil, err
	}
}

// Keys can only be a subset of the LBTYPEs.
func MakeTypeFromBytes(byts []byte, typ LBTYPE) (interface{}, error) {
	bfr := bytes.NewBuffer(byts)
	switch typ {
	case LBTYPE_UINT8:
		var v uint8
		err := binary.Read(bfr, BIGEND, &v)
		return v, err
	case LBTYPE_UINT16:
		var v uint16
		err := binary.Read(bfr, BIGEND, &v)
		return v, err
	case LBTYPE_UINT32:
		var v uint32
		err := binary.Read(bfr, BIGEND, &v)
		return v, err
	case LBTYPE_UINT64:
		var v uint64
		err := binary.Read(bfr, BIGEND, &v)
		return v, err
	case LBTYPE_INT8:
		var v int8
		err := binary.Read(bfr, BIGEND, &v)
		return v, err
	case LBTYPE_INT16:
		var v int16
		err := binary.Read(bfr, BIGEND, &v)
		return v, err
	case LBTYPE_INT32:
		var v int32
		err := binary.Read(bfr, BIGEND, &v)
		return v, err
	case LBTYPE_INT64:
		var v int64
		err := binary.Read(bfr, BIGEND, &v)
		return v, err
	case LBTYPE_FLOAT32:
		var v float32
		err := binary.Read(bfr, BIGEND, &v)
		return v, err
	case LBTYPE_FLOAT64:
		var v float64
		err := binary.Read(bfr, BIGEND, &v)
		return v, err
	case LBTYPE_COMPLEX64:
		var v complex64
		err := binary.Read(bfr, BIGEND, &v)
		return v, err
	case LBTYPE_COMPLEX128:
		var v complex128
		err := binary.Read(bfr, BIGEND, &v)
		return v, err
	case LBTYPE_MCID:
		var v MCID_TYPE
		err := binary.Read(bfr, BIGEND, &v)
		return v, err
	case LBTYPE_STRING,
		 LBTYPE_LOCATION,
		 LBTYPE_MCK:
		return string(byts), nil
	case LBTYPE_MCID_SET:
		v := NewMasterCatalogIdSet()
		err := v.FromBytes(bfr, ScreenLogger)
		return v, err
	case LBTYPE_KIND,
		 LBTYPE_DOC:
		v := MintNode(typ)
		err := v.FromBytes(bfr)
		return v, err
	default:
		return byts, nil
	}
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

func IsStringType(typ LBTYPE) bool {
	switch typ {
	case LBTYPE_STRING,
		 LBTYPE_LOCATION,
		 LBTYPE_MCK:
		return true
	}
	return false
}

func IsNumberType(typ LBTYPE) bool {
	switch typ {
	case LBTYPE_UINT8,
		 LBTYPE_UINT16,
		 LBTYPE_UINT32,
		 LBTYPE_UINT64,
		 LBTYPE_INT8,
		 LBTYPE_INT16,
		 LBTYPE_INT32,
		 LBTYPE_INT64,
		 LBTYPE_FLOAT32,
		 LBTYPE_FLOAT64,
		 LBTYPE_COMPLEX64,
		 LBTYPE_COMPLEX128,
		 LBTYPE_MCID:
		return true
	}
	return false
}

func IsAllowableKey(typ LBTYPE) bool {
	if IsNumberType(typ) || typ == LBTYPE_STRING {return true}
	return false
}

func ToBytes(val interface{}, vt LBTYPE, debug *DebugLogger) (byts []byte, err error) {
	bfr := new(bytes.Buffer)
	es := "Type mismatch, value is type %T but LBTYPE is %v"
	switch v := val.(type) {
    case uint8:
		if vt != LBTYPE_UINT8 {return nil, debug.Error(FmtErrBadType(es, v, vt))}
		binary.Write(bfr, BIGEND, v)
    case uint16:
		if vt != LBTYPE_UINT16 {return nil, debug.Error(FmtErrBadType(es, v, vt))}
		binary.Write(bfr, BIGEND, v)
    case uint32:
		if vt != LBTYPE_UINT32 {return nil, debug.Error(FmtErrBadType(es, v, vt))}
		binary.Write(bfr, BIGEND, v)
    case uint64:
		if vt != LBTYPE_UINT64 {return nil, debug.Error(FmtErrBadType(es, v, vt))}
		binary.Write(bfr, BIGEND, v)
    case int8:
		if vt != LBTYPE_INT8 {return nil, debug.Error(FmtErrBadType(es, v, vt))}
		binary.Write(bfr, BIGEND, v)
    case int16:
		if vt != LBTYPE_INT16 {return nil, debug.Error(FmtErrBadType(es, v, vt))}
		binary.Write(bfr, BIGEND, v)
    case int32:
		if vt != LBTYPE_INT32 {return nil, debug.Error(FmtErrBadType(es, v, vt))}
		binary.Write(bfr, BIGEND, v)
    case int64:
		if vt != LBTYPE_INT64 {return nil, debug.Error(FmtErrBadType(es, v, vt))}
		binary.Write(bfr, BIGEND, v)
    case float32:
		if vt != LBTYPE_FLOAT32 {return nil, debug.Error(FmtErrBadType(es, v, vt))}
		binary.Write(bfr, BIGEND, v)
    case float64:
		if vt != LBTYPE_FLOAT64 {return nil, debug.Error(FmtErrBadType(es, v, vt))}
		binary.Write(bfr, BIGEND, v)
    case complex64:
		if vt != LBTYPE_COMPLEX64 {return nil, debug.Error(FmtErrBadType(es, v, vt))}
		binary.Write(bfr, BIGEND, v)
    case complex128:
		if vt != LBTYPE_COMPLEX128 {return nil, debug.Error(FmtErrBadType(es, v, vt))}
		binary.Write(bfr, BIGEND, v)
    case MCID_TYPE:
		if vt != LBTYPE_MCID {return nil, debug.Error(FmtErrBadType(es, v, vt))}
		binary.Write(bfr, BIGEND, v)
    case []byte:
		if vt != LBTYPE_BYTES {
			return nil, debug.Error(FmtErrBadType(es, v, vt))
		}
		binary.Write(bfr, BIGEND, v)
    case string:
		if vt != LBTYPE_STRING && vt != LBTYPE_LOCATION {
			return nil, debug.Error(FmtErrBadType(es, v, vt))
		}
		binary.Write(bfr, BIGEND, v)
	}
	return bfr.Bytes(), nil
}

func ValBytesToString(vbyts []byte, vtype LBTYPE) string {
	v, err := MakeTypeFromBytes(vbyts, vtype)
	errstr := "" // Because its hard to get debugging in here
	if err != nil {errstr = "<ERROR " + err.Error() + ">"}
	return fmt.Sprintf("%v" + errstr, v)
}

