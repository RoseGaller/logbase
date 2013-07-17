/*
    Manages a collection of logbases.
*/
package main

import lb "github.com/h00gs/logbase"
import (
    "os"
    "github.com/h00gs/toml"
    "encoding/hex"
    "fmt"
)

var logbases map[string]*lb.Logbase = make(map[string]*lb.Logbase)
var id string // Randomly generated (practically unique) server id

const (
    ID_HALFLENGTH       int = 5 // full length is 2 * ID_HALFLENGTH
)

func main() {
    fmt.Println("Starting Logbase Server instance")
    id = GenerateRandomHexId(ID_HALFLENGTH)
    fmt.Println(id)
}

// Generate a random id.
// Credit to Russ Cox https://groups.google.com/forum/#!topic/golang-nuts/d0nF_k4dSx4
// for the idea of using /dev/urandom.
// TODO check cross compatibility of /dev/urandom
func GenerateRandomHexId(halflen int) string {
    frnd, _ := os.OpenFile("/dev/urandom", os.O_RDONLY, 0)
    rnd := make([]byte, halflen)
    frnd.Read(rnd)
    frnd.Close()
    return hex.EncodeToString(rnd)
}

// Server configuration

// User space constants
type ServerConfiguration struct {
    SOME_KEY string
}

// Default configuration.
func DefaultConfig() *ServerConfiguration {
    return &ServerConfiguration{
        SOME_KEY:     "hello",
    }
}

// Load optional server configuration file parameters.
func LoadConfig(path string) (config *ServerConfiguration, err error) {
    _, err = os.Stat(path)
    if os.IsNotExist(err) {
        config = DefaultConfig()
        err = nil
        return
    }
    if err != nil {return}
    config = new(ServerConfiguration)
    _, err = toml.Decode(path, &config)
    return
}

/*
// Open an existing Logbase or create it if necessary, identified by a
// directory path.
func Open(lbPath string) (lbase *lb.Logbase, err error) {
    if Debug == nil {InitDebugLogger()}

    // Use existing Logbase if present
    lbase, present := logbases[lbPath]
    if present {return}

}
*/
