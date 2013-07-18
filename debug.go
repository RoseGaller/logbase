/*
    Logging for code debugging only.
*/
package logbase

import (
    "os"
	"io"
    "fmt"
    "time"
    "strings"
)

const (
    TIMESTAMP_FORMAT string = "2006-01-02 15:04:00.000000 MST "
)

const ( // order important
    DEBUGLEVEL_ADVISE = iota
    DEBUGLEVEL_BASIC = iota
    DEBUGLEVEL_FINE = iota
)

var DebugLevels = map[string]int{
    "ADVISE": DEBUGLEVEL_ADVISE,
    "BASIC": DEBUGLEVEL_BASIC,
    "FINE": DEBUGLEVEL_FINE,
}

type DebugLogger struct {
    level   int
    out     []io.Writer
}

// Init a DebugLogger.
func NewDebugLogger(level int, writers []io.Writer) *DebugLogger {
    return &DebugLogger{level, writers}
}

// Return a default DebugLogger writing to the screen and a file.
func ScreenFileLogger(fname string) *DebugLogger{
    writers := []io.Writer{
               os.Stdout,
               FileDebugWriter(fname)}
    return MakeLogger(writers)
}

// Return a default DebugLogger writing to the screen only.
func ScreenLogger() *DebugLogger{
    writers := []io.Writer{os.Stdout}
    return MakeLogger(writers)
}

// Return a DebugLogger with no writers.
func NilLogger() *DebugLogger{
    writers := []io.Writer{}
    return MakeLogger(writers)
}

// Return a file debug logger writer using the given fname.
func FileDebugWriter(fname string) io.Writer {
    file, err := OpenFile(fname)
	if err != nil {WrapError("Could not open debug log: ", err).Fatal()}
    return file
}

// Return a default DebugLogger using the given writers.
func MakeLogger(writers []io.Writer) *DebugLogger {
    level := DebugLevels["BASIC"]
    debug := NewDebugLogger(level, writers)
    debug.Advise("Debug logger started")
    return debug
}

func DebugLevelName(level int) string {
    for name, i := range DebugLevels {
        if i == level {return name}
    }
    FmtErrValNotFound(string(level)).Fatal()
    return ""
}

// Allow the debug level to be changed on the fly.
func (debug *DebugLogger) SetLevel(levelstr string) *DebugLogger {
    oldname := DebugLevelName(debug.level)
    newname := strings.ToUpper(levelstr)
    level, ok := DebugLevels[newname]
	if !ok {FmtErrKeyNotFound(levelstr).Fatal()}
    debug.level = level
    debug.Advise(fmt.Sprintf(
          "Debug level changed from %q to %q",
          oldname, newname))
    return debug
}

// Writes the debug message.  Any error encountered results in app termination.
func (debug *DebugLogger) output(msg string) {
    msg += "\n"
    for _, writer := range debug.out {
        _, err := writer.Write([]byte(msg))
       if err != nil {
            WrapError(fmt.Sprintf(
            "Error while trying to write %q to %q",
            msg, writer), err).Fatal()
       }
    }
    return
}

// Create a timestamped message for debug output.
func stamp(msg, prefix string) string {
    return time.Now().Format(TIMESTAMP_FORMAT) + " " + prefix + " " + msg
}

// Output time stamped debug message.
func (debug *DebugLogger) StampedPrintln(msg string) *DebugLogger {
    debug.output(stamp(msg, ""))
    return debug
}
// Output debug message.
func (debug *DebugLogger) Println(msg string) *DebugLogger {
    debug.output(msg)
    return debug
}

// Output debug message as long as current level is at least FINE.
func (debug *DebugLogger) Fine(msg string, a ...interface{}) *DebugLogger {
    if debug.level >= DEBUGLEVEL_FINE {
        debug.output(stamp(
            fmt.Sprintf(msg, a...),
            "FINE"))
    }
    return debug
}

// Output debug message as long as current level is at least BASIC.
func (debug *DebugLogger) Basic(msg string, a ...interface{}) *DebugLogger {
    if debug.level >= DEBUGLEVEL_BASIC {
        debug.output(stamp(
            fmt.Sprintf(msg, a...),
            "BASIC"))
    }
    return debug
}

// Output debug message as long as current level is at least ADVISE.
func (debug *DebugLogger) Advise(msg string, a ...interface{}) *DebugLogger {
    if debug.level >= DEBUGLEVEL_ADVISE {
        debug.output(stamp(
            fmt.Sprintf(msg, a...),
            "ADVISE"))
    }
    return debug
}

// Issue warning to debug output.
func (debug *DebugLogger) Warn(msg string, a ...interface{}) *DebugLogger {
    debug.output(stamp(
        fmt.Sprintf(msg, a...),
        "WARNING"))
    return debug
}

// Issue error to debug output.
func (debug *DebugLogger) Error(err *AppError) *DebugLogger {
    debug.output(stamp(err.Message(), "ERROR"))
    return debug
}
