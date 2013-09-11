/*
	Manages a collection of logbases.
*/
package logbase

import (
	"os"
	"github.com/h00gs/toml"
	"github.com/h00gs/gubed"
	"github.com/garyburd/go-websocket/websocket"
	"net"
	"net/http"
	"io"
//	"encoding/json"
	"path"
//	"encoding/binary"
	"bytes"
	"strconv"
	"path/filepath"
	"strings"
	"fmt"
	"time"
	"runtime"
)

const (
	SERVER_ID_LENGTH        uint64 = 10 // Should be divisible by 2
	SESSION_ID_LENGTH       uint64 = 10 // Should be divisible by 2
	SERVER_CONFIG_FILENAME  string = "logbase_server.cfg"
	DEBUG_FILENAME          string = "debug.log"
	USERS_LOGBASE_NAME		string = ".users_logbase"
	WS_READ_BUFF_SIZE		int = 1024
	WS_WRITE_BUFF_SIZE		int = 1024
	ADMIN_USER				string = "Admin"
	CLOSEFILE_PATH			string = "./.close"
	KILOBYTE				uint64 = 1024
	MEGABYTE				uint64 = KILOBYTE * KILOBYTE
)

// Check intervals.
const (
	CHECK_CLOSEFILE_SECS	int = 5 // Check for close file every x secs
	CHECK_MEMORY_SECS		int = 10 // Check memory usage every x secs
)

type Server struct {
	id          string
	logbases    map[string]*Logbase
	config      *ServerConfiguration
	Debug       *gubed.Logger
	basedir		string
	users		*Logbase
	shutdown	bool
	listener	net.Listener
}

type WebsocketIO struct {
	in			io.Reader
	out			io.WriteCloser
}

type WebsocketSession struct {
	id			string
	start		time.Time
	ws			*websocket.Conn
	io          *WebsocketIO
	ok			bool // Session has been authorised
	user		string
}

// Messages.

type CMD uint8
const CMDSIZE = 1

//const (
//	WS_SUCCESS string = "SUCCESS"
//	WS_FAIL string = "FAIL"
//)

const (
	LOGIN CMD = iota
	CLOSE
	OPEN_LOGBASE
	CLOSE_LOGBASE
	LIST_LOGBASES
	PUT_PAIR // k-v pair
	GET_VALUE // k-v pair
)

var CommandCode = map[string]CMD{
	"LOGIN":			LOGIN,
	"CLOSE":			CLOSE,
	"OPEN_LOGBASE":		OPEN_LOGBASE,
	"CLOSE_LOGBASE":	CLOSE_LOGBASE,
	"LIST_LOGBASES":	LIST_LOGBASES,
    "PUT_PAIR":			PUT_PAIR,
	"GET_VALUE":		GET_VALUE,
}

var CommandName map[CMD]string = make(map[CMD]string)

func init() {
	for name, cmd := range CommandCode {
		CommandName[cmd] = name
	}
}

//type JsonMessage struct {
//	cmd		string
//	args	string
//}

func NewWebsocketSession() *WebsocketSession {
	return &WebsocketSession{
		id:         GenerateRandomHexStrings(1, SESSION_ID_LENGTH, SESSION_ID_LENGTH)[0],
		start:		time.Now(),
		io:			new(WebsocketIO),
		ok:			false,
	}
}

func NewServer() *Server {
	return &Server{
		id:         GenerateRandomHexStrings(1, SERVER_ID_LENGTH, SERVER_ID_LENGTH)[0],
		logbases:   make(map[string]*Logbase),
		Debug:      gubed.MakeScreenFileLogger(DEBUG_FILENAME),
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
func (server *Server) Start(passhash string) error {

	err := server.Init(passhash)
	if err != nil {return err}

	// TCP server
	service := ":" + strconv.Itoa(server.config.WEBSOCKET_PORT)
	http.Handle("/script/", http.FileServer(http.Dir("./web")))
	http.Handle("/css/", http.FileServer(http.Dir("./web")))
	//http.Handle("/", http.FileServer(http.Dir("./web")))
	http.HandleFunc("/", server.WebsocketSession)
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
	return nil
}

// Take steps for a graceful shutdown.
func (server *Server) GracefulShutdown() {
	server.Debug.Advise("Gracefully shutting down...")
	return
}

// Initialise server and configuration.
func (server *Server) Init(passhash string) error {
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
	server.basedir = config.DEFAULT_BASEDIR
	server.Debug.Advise(
		"Directory in which to look for logbases = %s",
		server.basedir)

	// Make dir if it does not exist
	err = server.Debug.Error(os.MkdirAll(server.basedir, 0777))
	if err != nil {return err}

	// User logbase
    users := MakeLogbase(server.UsersLogbasePath(), server.Debug)
	err = server.Debug.Error(users.Init(true))
	if err != nil {return err}
	err = server.Debug.Error(users.InitSecurity(ADMIN_USER, passhash))
	if err != nil {return err}
    server.users = users

	// Start close file checker
	go server.CloseFileChecker(CHECK_CLOSEFILE_SECS, CLOSEFILE_PATH)
	// Start memory checker
	go server.MemoryChecker(CHECK_MEMORY_SECS)

	return nil
}

// Regularly checks memory usage does not exceed limits.
func (server *Server) MemoryChecker(secs int) {
	server.Debug.Basic(
		"Started memory checker (monitors and moderates server memory use)")
	memstats := &runtime.MemStats{}
	for {
		<-time.After(time.Duration(secs) * time.Second)
		runtime.GC()
		runtime.ReadMemStats(memstats)
		server.Debug.Basic(
			"Memory (KB): total = %v allocated = %v",
			memstats.TotalAlloc / KILOBYTE,
			memstats.Alloc / KILOBYTE,
			)
	}
	return
}

// Continually checks to see if close file exists, if so, switches
// server shutdown flag on.
func (server *Server) CloseFileChecker(secs int, fpath string) {
	server.Debug.Error(os.RemoveAll(fpath))
	server.Debug.Basic(
		"Started close file checker (triggers shutdown if %q file appears)",
		fpath)
	var err error
	for {
		<-time.After(time.Duration(secs) * time.Second)
		_, err = os.Stat(fpath)
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
	return path.Join(server.basedir, USERS_LOGBASE_NAME)
}
func (server *Server) Id() string {return server.id}
func (session *WebsocketSession) Id() string {return session.id}

// Open an existing Logbase or create it if necessary, identified by a
// directory path.
func (server *Server) Open(lbPath, user, passhash string) (*Logbase, error) {
	// Use existing Logbase if present
	lbase, present := server.logbases[lbPath]
	if present {return lbase, nil}
	lbase = MakeLogbase(lbPath, server.Debug)
	err := lbase.Init(true)
	if err != nil {return nil, err}
	err = lbase.InitSecurity(user, passhash)
	return lbase, err
}

// Main entry point.  Collect and respond to socket messages.  When this
// function finishes, the websocket is closed.
func (server *Server) WebsocketSession(w http.ResponseWriter, r *http.Request) {
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
	session.ws = ws
	server.Debug.Basic("Enter SocketSession with id = %v", session.Id())
	//inbyts := make([]byte, WS_READ_BUFF_SIZE)
	//var n int
	for {
		op, r, err := ws.NextReader()
		if err != nil {
			server.Debug.Error(err)
			return
		}
		session.io.in = r
		if op != websocket.OpBinary && op != websocket.OpText {
			continue
		}
		w, err := ws.NextWriter(op)
		if err != nil {
			server.Debug.Error(err)
			return
		}
		session.io.out = w
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
			bfr := new(bytes.Buffer)
			bfr.ReadFrom(r)
			intxt := bfr.String()
			server.Debug.Basic("SocketSession incoming: %q", intxt)
			words := strings.Split(intxt, " ")
			//decoder := json.NewDecoder(r)
			//err = decoder.Decode(&intxt)
			//if err != nil {
			//	server.Debug.Error(err)
			//	return
			//}
			cmd, ok := CommandCode[words[0]]
			if !ok {
				server.Debug.Error(FmtErrBadCommand("Command %q not recognised", words[0]))
			}
			if cmd == CLOSE {
				server.Debug.Basic("SocketSession closed by user %s", session.user)
				break
			}
			server.Respond(session, cmd, words[1:])
		}
	}
	return
}

func (server *Server) Respond(session *WebsocketSession, cmd CMD, args []string) {
	defer session.io.out.Close()
	if !session.ok {
		if cmd == LOGIN {
			user := args[0]
			pass := args[1]
			if !server.users.IsValidUser(user, pass) {
				// TODO throttle attempts
				msg := fmt.Sprintf("Invalid credentials for user %q", user)
				server.Debug.Error(FmtErrUser(msg))
				server.Debug.Error(session.SendText(msg))
                return
			}
			session.ok = true
			session.user = user
			server.Debug.Advise("User %s logged in", user)
		} else {
			server.Debug.Error(FmtErrUser(
				"Session user not authorised to execute command %q",
				CommandName[cmd] + " " + strings.Join(args, " ")))
		}
		return
	}
	switch cmd {
	case LOGIN:
		server.Debug.Error(session.SendText("Already logged in"))
		return
	case OPEN_LOGBASE:
		return
	case CLOSE_LOGBASE:
		return
	case LIST_LOGBASES:
		list, err := server.ListLogbases()
		server.Debug.Error(err)
		server.Debug.Basic("List logbases: %s", list)
		bfr := bytes.NewBuffer([]byte(strings.Join(list, ";")))
        n, err := bfr.WriteTo(session.io.out)
		server.Debug.Error(err)
		server.Debug.Basic("Wrote %v bytes to socket", n)
		return
	case PUT_PAIR:
		return
	case GET_VALUE:
		return
	}
	return
}

func (server *Server) ListLogbases() ([]string, error) {
	var names []string
	var nscan int = 0
	server.Debug.Basic("Compiling list of logbases in %s", server.basedir)
	findTopLevelDir :=
			func(fpath string, fileInfo os.FileInfo, inerr error) error {
			stat, err := os.Stat(fpath)
			if err != nil {return err}

			if nscan > 0 && stat.IsDir() {
				name := filepath.Base(fpath)
				if name != USERS_LOGBASE_NAME {
					names = append(names, name)
				}
				return filepath.SkipDir
			}
			nscan++
			return nil
		}

	err := filepath.Walk(server.basedir, findTopLevelDir)
	return names, err
}

// Websocket Session.

func (session *WebsocketSession) SendText(msg string) error {
	bfr := bytes.NewBuffer([]byte(msg))
    _, err := bfr.WriteTo(session.io.out)
    return err
}
