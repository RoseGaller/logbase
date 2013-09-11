/*
	Customised errors for the Logbase application.
*/
package logbase

import (
	"github.com/h00gs/gubed"
	"fmt"
	"os"
)

// App level error handling.
type AppError struct {
	caller      *gubed.GoCaller
	msg         string // Error message
	tag         string
}

const DEFAULT_JUMPS int = 3

// Print message to stdout and terminate app.
func (err *AppError) Fatal()  {
	fmt.Println("LOGBASE FATAL ERROR")
	fmt.Println(err)
	os.Exit(1)
}

// Make an AppError, capturing the callers details.
// Deliberately private function, to fix the number of jumps
// from the caller.
func makeAppError(jump int) *AppError {
	return &AppError{caller: gubed.CaptureCaller(DEFAULT_JUMPS + jump)}
}

// Produce a string to satisfy error interface.
func (err *AppError) Error() string {
	return fmt.Sprintf(
		"Error(#%s) %s %s",
		err.tag,
		err.caller,
		err.msg)
}

func (err *AppError) Message() string {return err.msg}

func (err *AppError) Equals(other *AppError) bool {
	return err.tag == other.tag
}

func (err *AppError) Describe(msg, tag string) *AppError {
	err.msg = msg
	err.tag = tag
	return err
}

func WrapError(msg string, in error) *AppError {
	return makeAppError(0).Describe(msg + ": " + in.Error(), "wrapped_error")
}

// Uncategorised.

func ErrNew(msg string) *AppError {
	return makeAppError(0).Describe(msg, "uncategorised")
}

// Int mismatch.

func FmtErrOutsideRange(num int, max int64) *AppError {
	return errIntMismatch(fmt.Sprintf(
		"The number %d does not fit within " +
		"[0, LBUINT_MAX] = [0, %d]",
		num, max), 1)
}

func FmtErrIntMismatch(num64 int64, path string, byA string, num int) *AppError {
	return errIntMismatch(fmt.Sprintf(
		"The index %d extracted from log file %q cannot be " +
		"properly represented by a %s, result is %d.",
		num64, path, byA, num), 1)
}

func errIntMismatch(msg string, jump int) *AppError {
	return makeAppError(jump).Describe(msg, "int_mismatch")
}

// Key mismatch.

func FmtErrKeyMismatch(msg string, a ...interface{}) *AppError {
	return errKeyMismatch(fmt.Sprintf(msg, a...), 1)
}

func errKeyMismatch(msg string, jump int) *AppError {
	return makeAppError(jump).Describe(msg, "key_mismatch")
}

// Data mismatch.

func FmtErrDataMismatch(msg string, a ...interface{}) *AppError {
	return errDataMismatch(fmt.Sprintf(msg, a...), 1)
}

func errDataMismatch(msg string, jump int) *AppError {
	return makeAppError(jump).Describe(msg, "data_mismatch")
}

// Key not found.

func FmtErrKeyNotFound(key interface{}) *AppError {
	return errKeyNotFound(fmt.Sprintf("Key %v not found.", key), 1)
}

func FmtErrUnknownCatalogKey(key interface{}, catname string) *AppError {
	return errKeyNotFound(fmt.Sprintf(
		"The key %v in catalog %q on file was not found in the Master Catalog",
		key, catname), 1)
}

func errKeyNotFound(msg string, jump int) *AppError {
	return makeAppError(jump).Describe(msg, "key_not_found")
}

// Key collision.

func FmtErrKeyExists(keystr string) *AppError {
	return errKeyExists(fmt.Sprintf("Key %q already exists.", keystr), 1)
}

func errKeyExists(msg string, jump int) *AppError {
	return makeAppError(jump).Describe(msg, "key_exists")
}

// Value not found.

func FmtErrValNotFound(valstr string) *AppError {
	return errValNotFound(fmt.Sprintf("Value %q not found.", valstr), 1)
}

func errValNotFound(msg string, jump int) *AppError {
	return makeAppError(jump).Describe(msg, "value_not_found")
}

// File not found.

func FmtErrLiveLogUndefined() *AppError {
	return errFileNotFound("Live log has not been defined", 1)
}

func FmtErrFileNotFound(path string) *AppError {
	return errFileNotFound(fmt.Sprintf("File %q not found.", path), 1)
}

func FmtErrDirNotFound(path string) *AppError {
	return errFileNotFound(fmt.Sprintf("Directory %q not found.", path), 1)
}

func FmtErrFileNotDefined(obj interface{}) *AppError {
	return errFileNotFound(fmt.Sprintf(
		"File not defined for %T object %q.", obj, obj), 1)
}

func errFileNotFound(msg string, jump int) *AppError {
	return makeAppError(jump).Describe(msg, "file_not_found")
}

// Bad argument.

func FmtErrBadArgs(msg string, a ...interface{}) *AppError {
	return errBadArgs(fmt.Sprintf(msg, a...), 1)
}

func errBadArgs(msg string, jump int) *AppError {
	return makeAppError(jump).Describe(msg, "bad_arguments")
}

// Bad type.

func FmtErrBadType(msg string, a ...interface{}) *AppError {
	return errBadType(fmt.Sprintf(msg, a...), 1)
}

func errBadType(msg string, jump int) *AppError {
	return makeAppError(jump).Describe(msg, "bad_type")
}

// Unexpected data size.

func FmtErrSliceTooSmall(slice []byte, size int) *AppError {
	return fmtErrDataSize(
		"Data slice %v is too small to contain an " +
		"LBTYPE which is of size %v",
		slice, size)
}

func FmtErrReadSize(desc, path string, size LBUINT, nread int) *AppError {
	return fmtErrDataSize(
		"Invalid %s size while reading record for file %q. " +
		"Expected %d got %d bytes.",
		desc, path, size, nread)
}

func FmtErrPositionExceedsFileSize(path string, pos, size int) *AppError {
	return fmtErrDataSize(
		"The position %d for file %q exceeds the file size %d.",
		pos, path, size)
}

func FmtErrPartialCATIDSet(size, divisor int) *AppError {
	return fmtErrDataSize(
		"The CATID set has byte length %d which is " +
		"not a multiple of the CATID type size of %d",
		size, divisor)
}

func FmtErrPartialLocationData(size, nread LBUINT) *AppError {
	return fmtErrDataSize(
		"A ValueLocationRecord has %d bytes but the GenericRecord read " +
		"%d bytes, so some data must be missing.",
		size, nread)
}

func fmtErrDataSize(msg string, a ...interface{}) *AppError {
	return errDataSize(fmt.Sprintf(msg, a...), 2)
}

func errDataSize(msg string, jump int) *AppError {
	return makeAppError(jump).Describe(msg, "unexpected_data_size")
}

// User problems.

func FmtErrUser(msg string, a ...interface{}) *AppError {
	return errUser(fmt.Sprintf(msg, a...), 1)
}

func errUser(msg string, jump int) *AppError {
	return makeAppError(jump).Describe(msg, "user")
}

// Bad command.

func FmtErrBadCommand(msg string, a ...interface{}) *AppError {
	return errBadCommand(fmt.Sprintf(msg, a...), 1)
}

func errBadCommand(msg string, jump int) *AppError {
	return makeAppError(jump).Describe(msg, "bad_command")
}

