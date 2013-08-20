/*
	Collects all access permission related stuff.
*/
package logbase

import (
	"fmt"
	"code.google.com/p/gopass"
	"crypto/sha256"
	"encoding/hex"
	"encoding/binary"
	"os"
	"path/filepath"
)

const (
	PERMISSION_CREATE uint8 = 1
	PERMISSION_READ   uint8 = 2
	PERMISSION_UPDATE uint8 = 4
	PERMISSION_DELETE uint8 = 8
)

type Permission struct {
	Create	bool
	Read	bool
	Update	bool
	Delete	bool
}

func NewAdmin() *Permission {
	return &Permission{
		Create: true,
		Read:	true,
		Update: true,
		Delete: true,
	}
}

func NewReader() *Permission {
	return &Permission{
		Create: false,
		Read:	true,
		Update: false,
		Delete: false,
	}
}

func NewWriter() *Permission {
	return &Permission{
		Create: false,
		Read:	true,
		Update: false,
		Delete: false,
	}
}

func (p *Permission) String() string {
	return fmt.Sprintf(
		"(C:%d R:%d U:%d D:%d)",
		BoolToUint8(p.Create),
		BoolToUint8(p.Read),
		BoolToUint8(p.Update),
		BoolToUint8(p.Delete))
}

// Initialise security by loading all user permissions if the given user
// is authorised.
func (lbase *Logbase) InitSecurity(user, passhash string) (err error) {
	lbase.debug.Advise("Initialising security")
	// Make dir if it does not exist
	permpath := lbase.UserPermissionDirPath()
	isnew := !Exists(permpath)
	if err = lbase.debug.Error(os.MkdirAll(permpath, 0777)); err != nil {return}

	if lbase.IsUser(user) {
        if !lbase.IsValidUser(user, passhash) {
			return lbase.debug.Error(FmtErrUser("%q is not a valid user"))
		}
		// Load permission indexes
		usernames, err := lbase.GetUserPermissionPaths()
		if lbase.debug.Error(err) != nil {return err}
		for _, name := range usernames {
			if lbase.IsUser(name) {
				up := NewUserPermissions(NewReader())
				ufile, err := lbase.GetUserPermissionFile(name)
				lbase.debug.Error(err)
				up.file = NewUserPermissionFile(ufile)
				if up.file.size > 0 {
					err = lbase.debug.Error(up.Load(lbase.debug))
					if err != nil {return err}
					lbase.users.perm[name] = up
				}
			} else {
				lbase.debug.Error(FmtErrUser(
					"User %q has a permission index file but is not in the logbase, " +
					"add them in order to permit authentication",
					name))
			}
		}
	} else {
		// Add user to main logbase
		if isnew {
			p := NewAdmin()
			lbase.AddUser(user, passhash, p)
			lbase.Save()
		}
	}

    return
}

func (lbase *Logbase) UserPermissionDirPath() string {
	return filepath.Join(lbase.abspath, lbase.permdir)
}

func (lbase *Logbase) UserPermissionRelPath(user string) string {
	return filepath.Join(lbase.permdir, user)
}

func (lbase *Logbase) GetUserPermissionFile(user string) (ufile *File, err error) {
	return lbase.GetFile(lbase.UserPermissionRelPath(user))
}

func (lbase *Logbase) AddUserPass(user, passhash string) error {
	// Add user name and passhash to logbase	
	_, err := lbase.Put(UserPassKey(user), []byte(passhash), LBTYPE_STRING)
	return err
}

func (lbase *Logbase) AddUserPermissions(user string, defperm *Permission) (err error) {
	// Add user permission file
	ufile, err := lbase.GetUserPermissionFile(user)
	lbase.debug.Error(err)
	up := NewUserPermissions(defperm)
	up.file = NewUserPermissionFile(ufile)
	lbase.users.perm[user] = up
	//if up.file.size > 0 {
	//	err = up.Load(lbase.debug)
	//}
    return
}

func (lbase *Logbase) AddUser(user, passhash string, defperm *Permission) (err error) {
	err = lbase.debug.Error(lbase.AddUserPass(user, passhash))
	if err != nil {return err}
	err = lbase.debug.Error(lbase.AddUserPermissions(user, defperm))
	return err
}

func (lbase *Logbase) IsUser(user string) bool {
	val, _, _ := lbase.Get(UserPassKey(user))
	if val == nil {return false}
    return true
}

func (lbase *Logbase) IsValidUser(user, passhash string) bool {
	val, _, _ := lbase.Get(UserPassKey(user))
	if val == nil {return false}
	if string(val) == passhash {return true}
	lbase.debug.Check("key=%q expected = %q actual = %v", UserPassKey(user), passhash, val)
    return false
}

func UserKey(user string) string {
	return "User." + user
}

func UserPassKey(user string) string {
	return UserKey(user) + ".pass"
}

// Hiding user text input requires a linux system using gopass.
func AskForPass() string {
	pass, err := gopass.GetPass("Enter passphrase> ")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return pass
}

// Interact with user to generate a passphrase hash.
func MakePassHash() {
	fmt.Println("Generate a passphrase hash")
	pass := AskForPass()
	fmt.Printf("Copy/paste the next line into the %s\n", SERVER_CONFIG_FILENAME)
	fmt.Printf("SERVER_PASS_HASH = \"%s\"\n", GeneratePassHash(pass))
	os.Exit(0)
}

func GeneratePassHash(pass string) string {
	hash := sha256.New()
	hash.Write([]byte(pass))
	md := hash.Sum(nil)
	return hex.EncodeToString(md)
}

// Random numbers.

func TrueRandomSource() *os.File {
	frnd, err := os.OpenFile("/dev/urandom", os.O_RDONLY, 0)
	if err != nil {
		fmt.Println("TrueRandomSource: ", err)
	}
    return frnd
}

// Generate a slice of random hex strings of random length within the given
// range of lengths.
// Credit to Russ Cox https://groups.google.com/forum/#!topic/golang-nuts/d0nF_k4dSx4
// for the idea of using /dev/urandom.
// TODO check cross compatibility of /dev/urandom
func GenerateRandomHexStrings(n, minsize, maxsize uint64) (result []string) {
	frnd := TrueRandomSource()
	defer frnd.Close()

	maxuint := float64(^uint64(0))
	rng := float64(maxsize - minsize)
	if rng < 0 {
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
