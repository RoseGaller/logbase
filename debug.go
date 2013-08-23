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
	"encoding/hex"
	"bytes"
	"path/filepath"
	"strconv"
)

const (
	TIMESTAMP_FORMAT string = "2006-01-02 15:04:00.000000 MST "
)

const ( // order important
	DEBUGLEVEL_ADVISE = iota
	DEBUGLEVEL_BASIC
	DEBUGLEVEL_FINE
	DEBUGLEVEL_SUPERFINE
)

const (
	CALLER_NIL = iota
	CALLER_PART
	CALLER_FULL
)

type DebugMessageConfig struct {
	callerDetail    int
}

var DEBUG_NIL = &DebugMessageConfig{CALLER_NIL}
var DEBUG_PART = &DebugMessageConfig{CALLER_PART}
var DEBUG_FULL = &DebugMessageConfig{CALLER_FULL}

var DebugLevels = map[string]int{
	"ADVISE": DEBUGLEVEL_ADVISE,
	"BASIC": DEBUGLEVEL_BASIC,
	"FINE": DEBUGLEVEL_FINE,
	"SUPERFINE": DEBUGLEVEL_SUPERFINE,
}

// The map is small enough to reverse manually for speed/simplicity
var DebugLevelName = map[int]string{
	DEBUGLEVEL_ADVISE: "ADVISE",
	DEBUGLEVEL_BASIC: "BASIC",
	DEBUGLEVEL_FINE: "FINE",
	DEBUGLEVEL_SUPERFINE: "SUPERFINE",
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

func (debug *DebugLogger) GetLevel() int {
	return debug.level
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
// TODO do something about protecting the user from appending to
// a very large logger file!
func FileDebugWriter(fname string) io.Writer {
	gfile, err := OpenFile(fname, CREATE | WRITE_ONLY | APPEND)
	if err != nil {WrapError("Could not open debug log: ", err).Fatal()}
	return gfile
}

// Return a default DebugLogger using the given writers.
func MakeLogger(writers []io.Writer) *DebugLogger {
	level := DebugLevels["BASIC"]
	debug := NewDebugLogger(level, writers)
	debug.Advise("Debug logger started")
	return debug
}

// Allow the debug level to be changed on the fly.
func (debug *DebugLogger) SetLevel(levelstr string) *DebugLogger {
	oldname := DebugLevelName[debug.level]
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

// Output debug message as long as current level is at least SUPERFINE.
func (debug *DebugLogger) SuperFine(msg string, a ...interface{}) *DebugLogger {
	if debug.level >= DEBUGLEVEL_SUPERFINE {
		debug.messageHandler(DEBUG_FULL, DebugLevelName[DEBUGLEVEL_SUPERFINE], msg, a...)
	}
	return debug
}

// Output debug message as long as current level is at least FINE.
func (debug *DebugLogger) Fine(msg string, a ...interface{}) *DebugLogger {
	if debug.level >= DEBUGLEVEL_FINE {
		debug.messageHandler(DEBUG_PART, DebugLevelName[DEBUGLEVEL_FINE], msg, a...)
	}
	return debug
}

// Output debug message as long as current level is at least BASIC.
func (debug *DebugLogger) Basic(msg string, a ...interface{}) *DebugLogger {
	if debug.level >= DEBUGLEVEL_BASIC {
		debug.messageHandler(DEBUG_PART, DebugLevelName[DEBUGLEVEL_BASIC], msg, a...)
	}
	return debug
}

// Output debug message as long as current level is at least ADVISE.
func (debug *DebugLogger) Advise(msg string, a ...interface{}) *DebugLogger {
	if debug.level >= DEBUGLEVEL_ADVISE {
		debug.messageHandler(DEBUG_NIL, DebugLevelName[DEBUGLEVEL_ADVISE], msg, a...)
	}
	return debug
}

// A common handler for the debug message methods. Use of a DebugMessageConfig
// struct offers scope to enhance message functionality in the future.
func (debug *DebugLogger) messageHandler(msgConfig *DebugMessageConfig, levelstr, msg string, a ...interface{}) *DebugLogger {
	var out string
	sep := ": "
	if len(msg) == 0 {sep = ""}
	switch msgConfig.callerDetail {
	case CALLER_NIL:
		out = stamp(fmt.Sprintf(msg, a...), levelstr)
	case CALLER_PART:
		caller := CaptureCaller(3)
		out = stamp(fmt.Sprintf(
			filepath.Base(caller.filename) + "[" +
			strconv.Itoa(caller.line) + "]" + sep +
			msg, a...), levelstr)
	case CALLER_FULL:
		out = stamp(fmt.Sprintf(
			CaptureCaller(3).String() + sep + msg, a...), levelstr)
	}
	debug.output(out)
	return debug
}

// Special methods.

// Issue warning to debug output.
func (debug *DebugLogger) Warn(msg string, a ...interface{}) *DebugLogger {
	return debug.messageHandler(DEBUG_PART, "WARNING", msg, a...)
}

// Issue error to debug output.  Always use full caller logging.
func (debug *DebugLogger) Error(err error) error {
	if err != nil {
		debug.messageHandler(DEBUG_FULL, "ERROR", err.Error())
	}
	return err
}

// Checkpoint a location in the code.
func (debug *DebugLogger) Check(msg string, a ...interface{}) *DebugLogger {
	return debug.messageHandler(DEBUG_PART, "CHECKPOINT", msg, a...)
}

// Dump a slice of lines to the logger.
func (debug *DebugLogger) Dump(lines []string, msg string, a ...interface{}) *DebugLogger {
	debug.messageHandler(DEBUG_PART, "DUMP", msg, a...)
	for _, line := range lines {
		debug.messageHandler(DEBUG_NIL, "", line)
	}
	return debug
}

// Format a byte slice as a hex string with spaces.
func FmtHexString(b []byte) string {
	h := []byte(hex.EncodeToString(b))
	var buf bytes.Buffer
	var c int = 1
	for i := 0; i < len(h); i = i + 2 {
		buf.WriteString(" ")
		buf.Write(h[i:i+2])
		c++
		if c == 5 {
			buf.WriteString(" ")
			c = 1
		}
	}
	return buf.String()
}

func (debug *DebugLogger) DumpMasterCatalog(lbase *Logbase) {
	debug.Advise("Master catalog records:")
	for key, mcr := range lbase.mcat.index {
		debug.Advise("%q %s", key, mcr.String())
	}
}

func (debug *DebugLogger) DumpZapmap(lbase *Logbase) {
	debug.Advise("Zapmap records:")
	for key, zrecs := range lbase.zmap.zapmap {
		var line string = ""
		for _, zrec := range zrecs {
			line += zrec.String()
		}
		debug.Advise("%q {%s}", key, line)
	}
}
