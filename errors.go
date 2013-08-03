/*
    Customised errors for the Logbase application.
*/
package logbase

import (
    "fmt"
    "os"
)

// App level error handling.
type AppError struct {
    caller      *GoCaller
    msg         string // Error message
    tag         string
}

// Print message to stdout and terminate app.
func (err *AppError) Fatal()  {
    fmt.Println("LOGBASE FATAL ERROR")
    fmt.Println(err)
    os.Exit(1)
}

// Make an AppError, capturing the callers details.
// Deliberately private function, to fix the number of jumps
// from the caller.
func makeAppError() *AppError {
    return &AppError{caller: CaptureCaller(4)}
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
    return makeAppError().Describe(msg + ": " + in.Error(), "wrapped_error")
}

// Uncategorised.

func ErrNew(msg string) *AppError {
    return makeAppError().Describe(msg, "uncategorised")
}

// Int mismatch.

func FmtErrIntMismatch(num64 int64, path string, byA string, num int) *AppError {
    return ErrIntMismatch(fmt.Sprintf(
        "The index %d extracted from log file %q cannot be " +
        "properly represented by a %s, result is %d.",
        num64, path, byA, num))
}

func ErrIntMismatch(msg string) *AppError {
    return makeAppError().Describe(msg, "int_mismatch")
}

// Key not found.

func FmtErrKeyNotFound(keystr string) *AppError {
    return ErrKeyNotFound(fmt.Sprintf("Key %q not found.", keystr))
}

func ErrKeyNotFound(msg string) *AppError {
    return makeAppError().Describe(msg, "key_not_found")
}

// Key collision.

func FmtErrKeyExists(keystr string) *AppError {
    return ErrKeyExists(fmt.Sprintf("Key %q already exists.", keystr))
}

func ErrKeyExists(msg string) *AppError {
    return makeAppError().Describe(msg, "key_exists")
}

// Value not found.

func FmtErrValNotFound(valstr string) *AppError {
    return ErrValNotFound(fmt.Sprintf("Value %q not found.", valstr))
}

func ErrValNotFound(msg string) *AppError {
    return makeAppError().Describe(msg, "value_not_found")
}

// File not found.

func FmtErrFileNotFound(path string) *AppError {
    return ErrFileNotFound(fmt.Sprintf("File %q not found.", path))
}

func ErrFileNotFound(msg string) *AppError {
    return makeAppError().Describe(msg, "file_not_found")
}

// Bad argument.

func FmtErrBadArgs(msg string, a ...interface{}) *AppError {
    return ErrBadArgs(fmt.Sprintf(msg, a...))
}

func ErrBadArgs(msg string) *AppError {
    return makeAppError().Describe(msg, "bad_arguments")
}

// Unexpected data size.

func FmtErrDataSize(desc, path string, size LBUINT, nread int) *AppError {
    return ErrDataSize(fmt.Sprintf(
        "Invalid %s size while reading record for file %q. " +
        "Expected %d got %d bytes.",
        desc, path, size, nread))
}

func FmtErrPositionExceedsFileSize(path string, pos, size int) *AppError {
    return ErrDataSize(fmt.Sprintf(
        "The position %d for file %q exceeds the file size %d.",
        pos, path, size))
}

func FmtErrPartialLocationData(size, nread LBUINT) *AppError {
    return ErrDataSize(fmt.Sprintf(
        "A ValueLocationRecord has %d bytes but the GenericRecord read " +
        "%d bytes, so some data must be missing.",
        size, nread))
}

func ErrDataSize(msg string) *AppError {
    return makeAppError().Describe(msg, "unexpected_data_size")
}
