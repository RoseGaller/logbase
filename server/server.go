/*
    Manages a collection of logbases.
*/
package main

import lb "github.com/h00gs/logbase"
import (
    "fmt"
)

func main() {
    fmt.Println("Starting Logbase Server instance")
    server := lb.NewServer().Start()
    server.Debug.Advise("Hello!")
}

