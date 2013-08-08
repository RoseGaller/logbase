/*
	Manages a collection of logbases.
*/
package main

import lb "github.com/h00gs/logbase"
import (
	"os"
	"fmt"
)

const (
	killname string = "./.kill"
)

func main() {
	fmt.Println("=== LOGBASE SERVER ===")

	if len(os.Args) > 1 {
		if os.Args[1] == "-p" {lb.MakePassHash()}
	}

	pass := lb.AskForPass()
	MakeKillFile()
	lb.NewServer().Start(lb.GeneratePassHash(pass))
	os.RemoveAll(killname)
}

// Currently makes a linux kill file.
func MakeKillFile() error {
	err := os.RemoveAll(killname)
	if err != nil {return err}
	file, err2 :=
		os.OpenFile(
			killname,
			os.O_CREATE |
			os.O_APPEND |
			os.O_RDWR,
			0744)
    defer file.Close()
	if err2 != nil {return err2}
	_, err = fmt.Fprintln(file, "#!/bin/bash")
    if err != nil {
		fmt.Println("Problem creating the kill file: ", err)
		return err
	}
	host, _ := os.Hostname()
	pid := os.Getpid()
	fmt.Fprintf(file, "# host %s\n", host)
    fmt.Fprintf(file, "pkill -SIGKILL -P %d # Terminate subprocesses\n", pid)
	_, err = fmt.Fprintf(file, "kill -SIGKILL %d # Terminate parent\n", pid)
    return err
}
