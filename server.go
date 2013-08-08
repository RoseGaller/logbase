/*
	Manages a collection of logbases.
*/
package logbase

import (
	"os"
	"github.com/h00gs/toml"
	"github.com/garyburd/go-websocket/websocket"
	"net/http"
	"io"
//	"encoding/json"
	"path"
//	"encoding/binary"
	"bytes"
//	"bufio"
	"strconv"
	"path/filepath"
	"strings"
	"fmt"
	"time"
)

const (
	SERVER_ID_LENGTH        uint64 = 10 // Should be divisible by 2
	SESSION_ID_LENGTH       uint64 = 10 // Should be divisible by 2
	SERVER_CONFIG_FILENAME  string = "logbase_server.cfg"
	DEBUG_FILENAME          string = "debug.log"
	WS_READ_BUFF_SIZE		int = 1024
	WS_WRITE_BUFF_SIZE		int = 1024
)

type Server struct {
	id          string
	logbases    map[string]*Logbase
	config      *ServerConfiguration
	Debug       *DebugLogger
	basedir		string
}

type Session struct {
	id			string
	start		time.Time
}

// Messages.

type CMD uint8
const CMDSIZE = 1

const (
	CLOSE CMD = iota
	OPEN_LOGBASE
	CLOSE_LOGBASE
	LIST_LOGBASES
	PUT_PAIR // k-v pair
	GET_VALUE // k-v pair
)

var Cmdmap = map[string]CMD{
	"CLOSE":			CLOSE,
	"OPEN_LOGBASE":		OPEN_LOGBASE,
	"CLOSE_LOGBASE":	CLOSE_LOGBASE,
	"LIST_LOGBASE":		LIST_LOGBASES,
    "PUT_PAIR":			PUT_PAIR,
	"GET_VALUE":		GET_VALUE,
}

const (
	WS_SUCCESS string = "SUCCESS"
	WS_FAIL string = "FAIL"
)

//type JsonMessage struct {
//	cmd		string
//	args	string
//}

func NewSession() *Session {
	return &Session{
		id:         GenerateRandomHexStrings(1, SESSION_ID_LENGTH, SESSION_ID_LENGTH)[0],
		start:		time.Now(),
	}
}

func NewServer() *Server {
	return &Server{
		id:         GenerateRandomHexStrings(1, SERVER_ID_LENGTH, SERVER_ID_LENGTH)[0],
		logbases:   make(map[string]*Logbase),
		Debug:      ScreenFileLogger(DEBUG_FILENAME),
	}
}

// Server configuration.

// User space constants
type ServerConfiguration struct {
	DEBUG_LEVEL			string
	WEBSOCKET_PORT		int
	DEFAULT_BASEDIR		string
	SERVER_PASS_HASH	string
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

// Initialise configuration and start TCP server.
func (server *Server) Start(passhash string) {

	// Init
	cfgPath := path.Join(".", SERVER_CONFIG_FILENAME)
	config, err := LoadServerConfig(cfgPath)
	if err != nil {
		WrapError(
			"Problem loading server config file " +
			cfgPath, err).Fatal()
	}
	server.config = config

	if passhash != config.SERVER_PASS_HASH {
		fmt.Println("Incorrect passphrase")
		os.Exit(1)
	} else {
		fmt.Println("Passphrase is good")
	}

	server.Debug.SetLevel(config.DEBUG_LEVEL)
	server.Debug.Advise(DEBUG_DEFAULT, "Server id = %s", server.Id())
	server.Debug.Advise(DEBUG_DEFAULT, "config = %+v", config)
	server.basedir = config.DEFAULT_BASEDIR
	server.Debug.Advise(
		DEBUG_DEFAULT,
		"Default dir in which to look for logbases = %s",
		server.basedir)

	// TCP server
	service := ":" + strconv.Itoa(config.WEBSOCKET_PORT)
	http.Handle("/script/", http.FileServer(http.Dir("./web")))
	http.Handle("/css/", http.FileServer(http.Dir("./web")))
	http.Handle("/", http.FileServer(http.Dir("./web")))
	http.HandleFunc("/logbase", server.SocketSession)
	server.Debug.Advise(DEBUG_DEFAULT, "Listening on port %s...", service)
	err = http.ListenAndServe(service, nil)
	if err != nil {
		WrapError(
			"Problem starting tcp server at port " +
			service, err).Fatal()
	}

	return
}

func (server *Server) Id() string {return server.id}
func (session *Session) Id() string {return session.id}

// Open an existing Logbase or create it if necessary, identified by a
// directory path.
func (server *Server) Open(lbPath, user, passhash string) (lbase *Logbase, err error) {
	// Use existing Logbase if present
	lbase, present := server.logbases[lbPath]
	if present {return}
	lbase = MakeLogbase(lbPath, server.Debug)
	err = lbase.Init(user, passhash)
	return
}

// Collect and respond to socket messages.  When this function finishes, the
// websocket is closed.
func (server *Server) SocketSession(w http.ResponseWriter, r *http.Request) {
	//session := NewSession()
	server.Debug.Fine(DEBUG_DEFAULT, "Enter SocketSession")
	ws, err :=
		websocket.Upgrade(
			w,                    // any responder that supports http.Hijack
			r.Header,             // request header string->string map
			nil,                  // response header string->string map
			WS_READ_BUFF_SIZE,    // buffer sizes for read...
			WS_WRITE_BUFF_SIZE)   // and write
	if err != nil {
		http.Error(w, err.Error(), 400)
		server.Debug.Error(err)
		return
	}
	defer ws.Close()
	//inbyts := make([]byte, WS_READ_BUFF_SIZE)
	//var n int
	for {
		op, r, err := ws.NextReader()
		if err != nil {
			server.Debug.Error(err)
			return
		}
		if op != websocket.OpBinary && op != websocket.OpText {
			continue
		}
		w, err := ws.NextWriter(op)
		if err != nil {
			server.Debug.Error(err)
			return
		}
		/*
		if op == websocket.OpBinary {
			n, err = r.Read(inbyts)
			server.Debug.Fine(DEBUG_DEFAULT, "Msg rx: %v", inbyts[:n])
			bfr := bufio.NewReader(bytes.NewBuffer(inbyts[:n]))
			binary.Read(bfr, binary.BigEndian, &cmd)
			if cmd == CLOSE {
				server.Debug.Fine(DEBUG_DEFAULT, "SocketSession closed by client")
				break
			}
			go server.RespondToBinary(cmd, inbyts[CMDSIZE:n], w)
		}
		*/
		if op == websocket.OpText {
			buf := new(bytes.Buffer)
			buf.ReadFrom(r)
			intxt := buf.String()
			server.Debug.Fine(DEBUG_DEFAULT, "SocketSession incoming: %q", intxt)
			words := strings.Split(intxt, " ")
			//decoder := json.NewDecoder(r)
			//err = decoder.Decode(&intxt)
			//if err != nil {
			//	server.Debug.Error(err)
			//	return
			//}
			cmd := Cmdmap[words[0]]
			if cmd == CLOSE {
				server.Debug.Fine(DEBUG_DEFAULT, "SocketSession closed by client")
				break
			}
			go server.Respond(cmd, words[1:], w)
		}
	}
	return
}

func (server *Server) Respond(cmd CMD, args []string, w io.WriteCloser) {
	defer w.Close()
	switch cmd {
	case OPEN_LOGBASE:
		return
	case CLOSE_LOGBASE:
		return
	case LIST_LOGBASES:
		list, err := server.ListLogbases()
		server.Debug.Error(err)
		server.Debug.Basic(DEBUG_DEFAULT, "List logbases: %s", list)
		return
	case PUT_PAIR:
		return
	case GET_VALUE:
		return
	}
	return
}

func (server *Server) ListLogbases() (paths []string, err error) {
	var nscan int = 0
	server.Debug.Basic(
		DEBUG_DEFAULT, "Compiling list of logbases in %s", server.basedir)
	findTopLevelDir :=
			func(fpath string, fileInfo os.FileInfo, inerr error) (err error) {
			stat, err := os.Stat(fpath)
			if err != nil {return}

			if nscan > 0 && stat.IsDir() {
				paths = append(paths, fpath)
				return filepath.SkipDir
			}
			nscan++
			return
		}

	err = filepath.Walk(server.basedir, findTopLevelDir)
	return
}
