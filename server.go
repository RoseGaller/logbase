/*
	Manages a collection of logbases.
*/
package logbase

import (
	"os"
	"github.com/h00gs/toml"
	"github.com/garyburd/go-websocket/websocket"
	"net"
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
	ADMIN_USER				string = "Admin"
	CHECK_CLOSEFILE_SECS	int = 5 // Check for close file every x secs
	CLOSEFILE_PATH			string = "./.close"
)

type Server struct {
	id          string
	logbases    map[string]*Logbase
	config      *ServerConfiguration
	Debug       *DebugLogger
	basedir		string
	users		*Logbase
	shutdown	bool
	listener	net.Listener
}

type WebsocketSession struct {
	id			string
	start		time.Time
	connection	*websocket.Conn
}

// Messages.

type CMD uint8
const CMDSIZE = 1

const (
	LOGIN CMD = iota
	CLOSE
	OPEN_LOGBASE
	CLOSE_LOGBASE
	LIST_LOGBASES
	PUT_PAIR // k-v pair
	GET_VALUE // k-v pair
)

var Cmdmap = map[string]CMD{
	"LOGIN":			LOGIN,
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

func NewWebsocketSession() *WebsocketSession {
	return &WebsocketSession{
		id:         GenerateRandomHexStrings(1, SESSION_ID_LENGTH, SESSION_ID_LENGTH)[0],
		start:		time.Now(),
	}
}

func NewServer() *Server {
	return &Server{
		id:         GenerateRandomHexStrings(1, SERVER_ID_LENGTH, SERVER_ID_LENGTH)[0],
		logbases:   make(map[string]*Logbase),
		Debug:      MakeScreenFileLogger(DEBUG_FILENAME),
		shutdown:	false,
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

// Initialise server and start TCP server.
func (server *Server) Start(passhash string) {

	server.Init(passhash)

	// TCP server
	service := ":" + strconv.Itoa(server.config.WEBSOCKET_PORT)
	http.Handle("/script/", http.FileServer(http.Dir("./web")))
	http.Handle("/css/", http.FileServer(http.Dir("./web")))
	http.Handle("/", http.FileServer(http.Dir("./web")))
	http.HandleFunc("/logbase", server.WebsocketSession)
	server.Debug.Advise("Listening on port %s...", service)
	listener, err := net.Listen("tcp", service)
	if server.Debug.Error(err) == nil {
		server.listener = listener
		err = http.Serve(listener, nil) // for{} loop
		server.Debug.Error(err)
		if server.shutdown {
			server.GracefulShutdown()
		}
	}
	return
}

func (server *Server) GracefulShutdown() {
	server.Debug.Advise("Gracefully shutting down...")
	return
}

// Initialise server and configuration.
func (server *Server) Init(passhash string) {
	cfgPath := path.Join(".", SERVER_CONFIG_FILENAME)
	config, err := LoadServerConfig(cfgPath)
	if server.Debug.Error(err) != nil {
		WrapError("Problem loading server config file", err).Fatal()
	}
	server.config = config

	if passhash != config.SERVER_PASS_HASH {
		fmt.Println("Incorrect passphrase")
		os.Exit(1)
	} else {
		fmt.Println("Passphrase is good")
	}

	server.Debug.SetLevel(config.DEBUG_LEVEL)
	server.Debug.Advise("Server id = %s", server.Id())
	server.Debug.Advise("config = %+v", config)
	server.basedir = config.DEFAULT_BASEDIR
	server.Debug.Advise(
		"Directory in which to look for logbases = %s",
		server.basedir)

	// User logbase
    users := MakeLogbase(server.UsersLogbasePath(), server.Debug)
	err = users.Init(ADMIN_USER, passhash)
	if server.Debug.Error(err) != nil {
		WrapError("Problem initialising Users logbase", err).Fatal()
	}
    server.users = users

	// Close file
	go server.CloseFileChecker()

	return
}

// Continually checks to see if close file exists, if so, switches
// server shutdown flag on.
func (server *Server) CloseFileChecker() {
	server.Debug.Error(os.RemoveAll(CLOSEFILE_PATH))
	server.Debug.Basic(
		"Started close file checker (triggers shutdown if %q file appears)",
		CLOSEFILE_PATH)
	var err error
	for {
		<-time.After(time.Duration(CHECK_CLOSEFILE_SECS) * time.Second)
		_, err = os.Stat(CLOSEFILE_PATH)
		if !os.IsNotExist(err) {
			server.Debug.Advise("Close file detected, triggering shutdown")
			server.shutdown = true
			server.listener.Close()
			break
		}
	}
	return
}

func (server *Server) UsersLogbasePath() string {
	return path.Join(server.basedir, "ServerUsers")
}
func (server *Server) Id() string {return server.id}
func (session *WebsocketSession) Id() string {return session.id}

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
func (server *Server) WebsocketSession(w http.ResponseWriter, r *http.Request) {
	server.Debug.Fine("Enter SocketSession")
	ws, err :=
		websocket.Upgrade(
			w,                    // any responder that supports http.Hijack
			r.Header,             // request header string->string map
			nil,                  // response header string->string map
			WS_READ_BUFF_SIZE,    // buffer sizes for read...
			WS_WRITE_BUFF_SIZE)   // and write
	if server.Debug.Error(err) != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	defer ws.Close()
	session := NewWebsocketSession()
	session.connection = ws
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
			server.Debug.Fine("Msg rx: %v", inbyts[:n])
			bfr := bufio.NewReader(bytes.NewBuffer(inbyts[:n]))
			binary.Read(bfr, binary.BigEndian, &cmd)
			if cmd == CLOSE {
				server.Debug.Fine("SocketSession closed by client")
				break
			}
			go server.RespondToBinary(cmd, inbyts[CMDSIZE:n], w)
		}
		*/
		if op == websocket.OpText {
			buf := new(bytes.Buffer)
			buf.ReadFrom(r)
			intxt := buf.String()
			server.Debug.Fine("SocketSession incoming: %q", intxt)
			words := strings.Split(intxt, " ")
			//decoder := json.NewDecoder(r)
			//err = decoder.Decode(&intxt)
			//if err != nil {
			//	server.Debug.Error(err)
			//	return
			//}
			cmd := Cmdmap[words[0]]
			if cmd == CLOSE {
				server.Debug.Fine("SocketSession closed by client")
				break
			}
			server.Respond(session, cmd, words[1:], w)
		}
	}
	return
}

func (server *Server) Respond(session *WebsocketSession, cmd CMD, args []string, w io.WriteCloser) {
	defer w.Close()
	switch cmd {
	case LOGIN:
		return
	case OPEN_LOGBASE:
		return
	case CLOSE_LOGBASE:
		return
	case LIST_LOGBASES:
		list, err := server.ListLogbases()
		server.Debug.Error(err)
		server.Debug.Basic("List logbases: %s", list)
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
	server.Debug.Basic("Compiling list of logbases in %s", server.basedir)
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
