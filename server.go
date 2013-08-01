/*
    Manages a collection of logbases.
*/
package logbase

import (
    "os"
    "encoding/hex"
    "path"
    "github.com/h00gs/toml"
    //"bufio"
    "encoding/binary"
    "fmt"
)

const (
    ID_LENGTH               uint64 = 10 // Should be divisible by 2
    SERVER_CONFIG_FILENAME  string = "logbase_server.cfg"
    DEBUG_FILENAME          string = "debug.log"
)

type Server struct {
    id          string
    logbases    map[string]*Logbase
    Debug       *DebugLogger
}

func NewServer() *Server {
    return &Server{
        id:         GenerateRandomHexStrings(1, ID_LENGTH, ID_LENGTH)[0],
        logbases:   make(map[string]*Logbase),
        Debug:      ScreenFileLogger(DEBUG_FILENAME),
    }
}

// Server configuration

// User space constants
type ServerConfiguration struct {
    DEBUG_LEVEL     string
}

// Default configuration in case file is absent.
func DefaultServerConfig() *ServerConfiguration {
    return &ServerConfiguration{
        DEBUG_LEVEL:     "ADVISE",
    }
}

// Load optional server configuration file parameters.
func LoadServerConfig(path string) (config *ServerConfiguration, err error) {
    _, err = os.Stat(path)
    if os.IsNotExist(err) {
        config = DefaultServerConfig()
        err = nil
        return
    }
    if err != nil {return}
    config = new(ServerConfiguration)
    _, err = toml.DecodeFile(path, &config)
    return
}

func (server *Server) Start() *Server {
    cfgPath := path.Join(".", SERVER_CONFIG_FILENAME)
    config, errcfg := LoadServerConfig(cfgPath)
	if errcfg != nil {
        WrapError(
            "Problem loading server config file " +
            cfgPath, errcfg).Fatal()
    }
    server.Debug.SetLevel(config.DEBUG_LEVEL)
    server.Debug.Advise(DEBUG_DEFAULT, "Server id = " + server.Id())
    return server
}

func (server *Server) Id() string {return server.id}

// Open an existing Logbase or create it if necessary, identified by a
// directory path.
func (server *Server) Open(lbPath string) (lbase *Logbase, err error) {
    // Use existing Logbase if present
    lbase, present := server.logbases[lbPath]
    if present {return}
    lbase = MakeLogbase(lbPath, server.Debug)
    err = lbase.Init()
    return
}

// Generate a slice of random hex strings of random length within the given
// range of lengths.
// Credit to Russ Cox https://groups.google.com/forum/#!topic/golang-nuts/d0nF_k4dSx4
// for the idea of using /dev/urandom.
// TODO check cross compatibility of /dev/urandom
func GenerateRandomHexStrings(n, minsize, maxsize uint64) (result []string) {
    frnd, _ := os.OpenFile("/dev/urandom", os.O_RDONLY, 0)
    defer frnd.Close()

    maxuint := float64(^uint64(0))
    rng := float64(maxsize - minsize)
    if rng <= 0 {
        ErrNew(fmt.Sprintf("maxsize %d must be >= minsize %d", maxsize, minsize)).Fatal()
    }
    var adjlen, rawlen uint64
    result = make([]string, n)
    for i := 0; i < int(n); i++ {
        binary.Read(frnd, binary.BigEndian, &rawlen)
        adjlen = uint64(float64(rawlen)*rng/maxuint) + minsize
        rndval := make([]byte, int(adjlen)/2)
        frnd.Read(rndval)
        result[i] = hex.EncodeToString(rndval)
    }
    return
}

