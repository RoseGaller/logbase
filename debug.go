/*
    Logging for code debugging only.
*/
package logbase

import (
	"io"
    "fmt"
    "time"
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
func (debug *DebugLogger) StampedPrintln(msg string) {
    debug.output(stamp(msg, ""))
    return
}
// Output debug message.
func (debug *DebugLogger) Println(msg string) {
    debug.output(msg)
    return
}

// Output debug message as long as current level is at least FINE.
func (debug *DebugLogger) Fine(msg string) {
    if debug.level >= DEBUGLEVEL_FINE {debug.output(stamp(msg, "FINE"))}
    return
}

// Output debug message as long as current level is at least BASIC.
func (debug *DebugLogger) Basic(msg string) {
    if debug.level >= DEBUGLEVEL_BASIC {debug.output(stamp(msg, "BASIC"))}
    return
}

// Output debug message as long as current level is at least ADVISE.
func (debug *DebugLogger) Advise(msg string) {
    if debug.level >= DEBUGLEVEL_ADVISE {debug.output(stamp(msg, "ADVISE"))}
    return
}

// Issue warning to debug output.
func (debug *DebugLogger) Warn(msg string) {
    debug.output(stamp(msg, "WARNING"))
}

// Issue error to debug output.
func (debug *DebugLogger) Error(err *AppError) {
    debug.output(stamp(err.Message(), "ERROR"))
}
