/*
    Customised errors for the Logbase application.
*/
package logbase

import (
    "runtime"
    "fmt"
    "os"
)

// App level error handling.
type AppError struct {
    filename    string
    fn          string
    line        int
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
func makeAppError() *AppError {
    pc, filename, line, _ := runtime.Caller(1)
    return &AppError{
        filename: filename,
        fn: runtime.FuncForPC(pc).Name(),
        line: line}
}

// Produce a string to satisfy error interface.
func (err *AppError) Error() string {
    return fmt.Sprintf(
        "%s Error(#%s) %s in %s.%d %q",
        APPNAME,
        err.tag,
        err.fn,
        err.filename,
        err.line,
        err.msg)
}

func (err *AppError) Message() string {return err.msg}

func (err *AppError) Equals(other *AppError) bool {
    return err.tag == other.tag
}

func WrapError(msg string, in error) (err *AppError) {
    err = makeAppError()
    err.msg = msg + ": " + in.Error()
    err.tag = "wrapped error"
    return
}

// Int mismatch

func FmtErrIntMismatch(num64 int64, path string, byA string, num int) *AppError {
    msg := fmt.Sprintf(
        "The index %d extracted from log file %q cannot be " +
        "properly represented by a %s, result is %d",
        num64, path, byA, num)
    return ErrIntMismatch(msg)
}

func ErrIntMismatch(msg string) (err *AppError) {
    err = makeAppError()
    err.msg = msg
    err.tag = "int mismatch"
    return
}

// Key not found.

func FmtErrKeyNotFound(keystr string) *AppError {
    msg := fmt.Sprintf("Key %q not found", keystr)
    return ErrKeyNotFound(msg)
}

func ErrKeyNotFound(msg string) (err *AppError) {
    err = makeAppError()
    err.msg = msg
    err.tag = "key not found"
    return
}

// File not found.

func FmtErrFileNotFound(path string) *AppError {
    msg := fmt.Sprintf("File %q not found", path)
    return ErrFileNotFound(msg)
}

func ErrFileNotFound(msg string) (err *AppError) {
    err = makeAppError()
    err.msg = msg
    err.tag = "file not found"
    return
}

// Unexpected data size.

func FmtErrDataSize(desc, path string, size LBUINT, nread int) *AppError {
    msg := fmt.Sprintf(
        "Invalid %s size while reading record for file %q. " +
        "Expected %d got %d bytes",
        desc, path, size, nread)
    return ErrDataSize(msg)
}

func FmtErrPartialZapData(size, nread LBUINT) *AppError {
    msg := fmt.Sprintf(
        "A ZapRecord has %d bytes but the GenericRecord read " +
        "%d bytes, so some data must be missing",
        size, nread)
    return ErrDataSize(msg)
}

func ErrDataSize(msg string) (err *AppError) {
    err = makeAppError()
    err.msg = msg
    err.tag = "Unexpected data size"
    return
}
