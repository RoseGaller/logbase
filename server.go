/*
    Manages a collection of logbases.
*/
package logbase

import (
    "os"
    "encoding/hex"
    "path"
    "github.com/h00gs/toml"
)

const (
    ID_LENGTH               int = 10 // Should be divisible by 2
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
        id:         GenerateRandomHexStr(ID_LENGTH),
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
func (server *Server) Open(lbPath string) *Logbase {
    // Use existing Logbase if present
    lbase, present := server.logbases[lbPath]
    if present {return lbase}
    lbase = MakeLogbase(lbPath, server.Debug)
    return lbase.Init()
}

// Generate a random id.
// Credit to Russ Cox https://groups.google.com/forum/#!topic/golang-nuts/d0nF_k4dSx4
// for the idea of using /dev/urandom.
// TODO check cross compatibility of /dev/urandom
func GenerateRandomHexStr(length int) string {
    frnd, _ := os.OpenFile("/dev/urandom", os.O_RDONLY, 0)
    rnd := make([]byte, length/2)
    frnd.Read(rnd)
    frnd.Close()
    return hex.EncodeToString(rnd)
}

