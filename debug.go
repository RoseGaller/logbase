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
    "runtime"
)

const (
    TIMESTAMP_FORMAT string = "2006-01-02 15:04:00.000000 MST "
)

const ( // order important
    DEBUGLEVEL_ADVISE = iota
    DEBUGLEVEL_BASIC = iota
    DEBUGLEVEL_FINE = iota
)

const (
    CALLER_NIL = iota
    CALLER_FUNC = iota
    CALLER_FULL = iota
)

type DebugMessageConfig struct {
    callerDetail    int
}

var DEBUG_DEFAULT = &DebugMessageConfig{}

var DebugLevels = map[string]int{
    "ADVISE": DEBUGLEVEL_ADVISE,
    "BASIC": DEBUGLEVEL_BASIC,
    "FINE": DEBUGLEVEL_FINE,
}

// The map is small enough to reverse manually for speed/simplicity
var DebugLevelName = map[int]string{
    DEBUGLEVEL_ADVISE: "ADVISE",
    DEBUGLEVEL_BASIC: "BASIC",
    DEBUGLEVEL_FINE: "FINE",
}

type DebugLogger struct {
    level   int
    out     []io.Writer
}

// Init a DebugLogger.
func NewDebugLogger(level int, writers []io.Writer) *DebugLogger {
    return &DebugLogger{level, writers}
}

// Captures a Go caller identity and location.
type GoCaller struct {
    filename    string // Go code filename
    line        int // Line number within code
    fn          string // Go function
}

// Return a string representing the caller.
func (caller *GoCaller) String() string {
    return fmt.Sprintf(
        "%s %s in %s.%d",
        APPNAME,
        caller.fn,
        caller.filename,
        caller.line)
}

// Captures the callers details, accounting for jumps since the call.
func CaptureCaller(jumpsSinceCall int) *GoCaller {
    pc, filename, line, _ := runtime.Caller(jumpsSinceCall)
    return &GoCaller{
        filename: filename,
        line: line,
        fn: runtime.FuncForPC(pc).Name(),
    }
}

// Loggers

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
    gfile, err := OpenFile(fname)
	if err != nil {WrapError("Could not open debug log: ", err).Fatal()}
    return gfile
}

// Return a default DebugLogger using the given writers.
func MakeLogger(writers []io.Writer) *DebugLogger {
    level := DebugLevels["BASIC"]
    debug := NewDebugLogger(level, writers)
    debug.Advise(DEBUG_DEFAULT, "Debug logger started")
    return debug
}

// Allow the debug level to be changed on the fly.
func (debug *DebugLogger) SetLevel(levelstr string) *DebugLogger {
    oldname := DebugLevelName[debug.level]
    newname := strings.ToUpper(levelstr)
    level, ok := DebugLevels[newname]
	if !ok {FmtErrKeyNotFound(levelstr).Fatal()}
    debug.level = level
    debug.Advise(DEBUG_DEFAULT, fmt.Sprintf(
          "Debug level changed from %q to %q",
          oldname, newname))
    return debug
}

// Writes the debug message.  Any error encountered results in app termination.
func (debug *DebugLogger) output(msg string) *DebugLogger {
    msg += "\n"
    for _, writer := range debug.out {
        _, err := writer.Write([]byte(msg))
       if err != nil {
            WrapError(fmt.Sprintf(
            "Error while trying to write %q to %q",
            msg, writer), err).Fatal()
       }
    }
    return debug
}

// Create a timestamped message for debug output.
func stamp(msg, prefix string) string {
    return time.Now().Format(TIMESTAMP_FORMAT) + " " + prefix + " " + msg
}

// Output time stamped debug message.
func (debug *DebugLogger) StampedPrintln(msg string) *DebugLogger {
    return debug.output(stamp(msg, ""))
}
// Output debug message.
func (debug *DebugLogger) Println(msg string) *DebugLogger {
    return debug.output(msg)
}

// Output debug message as long as current level is at least FINE.
func (debug *DebugLogger) Fine(msgConfig *DebugMessageConfig, msg string, a ...interface{}) *DebugLogger {
    if debug.level >= DEBUGLEVEL_FINE {
        debug.messageHandler(msgConfig, msg, a...)
    }
    return debug
}

// Output debug message as long as current level is at least BASIC.
func (debug *DebugLogger) Basic(msgConfig *DebugMessageConfig, msg string, a ...interface{}) *DebugLogger {
    if debug.level >= DEBUGLEVEL_BASIC {
        debug.messageHandler(msgConfig, msg, a...)
    }
    return debug
}

// Output debug message as long as current level is at least ADVISE.
func (debug *DebugLogger) Advise(msgConfig *DebugMessageConfig, msg string, a ...interface{}) *DebugLogger {
    if debug.level >= DEBUGLEVEL_ADVISE {
        debug.messageHandler(msgConfig, msg, a...)
    }
    return debug
}

// A common handler for the debug message methods. Use of a DebugMessageConfig
// struct offers scope to enhance message functionality in the future.
func (debug *DebugLogger) messageHandler(msgConfig *DebugMessageConfig, msg string, a ...interface{}) {
    var out string
    var level string = DebugLevelName[debug.level]
    switch msgConfig.callerDetail {
    case CALLER_NIL:
        out = stamp(fmt.Sprintf(msg, a...), level)
    case CALLER_FUNC:
        out = stamp(fmt.Sprintf(CaptureCaller(3).fn + ": " + msg, a...), level)
    case CALLER_FULL:
        out = stamp(fmt.Sprintf(CaptureCaller(3).String() + ": " + msg, a...), level)
    }
    debug.output(out)
    return
}

// Issue warning to debug output.
func (debug *DebugLogger) Warn(msg string, a ...interface{}) *DebugLogger {
    return debug.output(stamp(fmt.Sprintf(msg, a...), "WARNING"))
}

// Issue error to debug output.
func (debug *DebugLogger) Error(err *AppError) *DebugLogger {
    return debug.output(stamp(err.Message(), "ERROR"))
}
