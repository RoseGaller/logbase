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
)

type Permissions struct {
	Create	bool
	Read	bool
	Update	bool
	Delete	bool
	Active	bool // Admin can deactivate
	Admin	bool
}

func NewAdmin() *Permissions {
	return &Permissions{
		Create: true,
		Read:	true,
		Update: true,
		Delete: true,
		Active: true,
		Admin:	true,
	}
}

func NewReader() *Permissions {
	return &Permissions{
		Create: false,
		Read:	true,
		Update: false,
		Delete: false,
		Active: true,
		Admin:	false,
	}
}

func NewWriter() *Permissions {
	return &Permissions{
		Create: false,
		Read:	true,
		Update: false,
		Delete: false,
		Active: true,
		Admin:	false,
	}
}

func (lbase *Logbase) InitUsers(user, passhash string) {
	lbase.Put(
		"User." + user + ".pass",
		VALTYPE_STRING,
		[]byte(passhash))
	lbase.Put(
		"User." + user + ".permissions",
		VALTYPE_GOB,
		Gobify(NewAdmin(), lbase.debug))
    return
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
