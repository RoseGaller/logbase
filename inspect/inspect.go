/*
	Command line utility to dump logbase contents as plain text.
*/
package main

import (
	lb "github.com/h00gs/logbase"
	flags "github.com/jessevdk/go-flags"
	"fmt"
	"os"
)

var opts struct {
	Path string `short:"p" default:"." description:"Path to logbase"`
	DumpLogfile lb.LBUINT `short:"l" description:"Dump logfile" value-name:"INT" optional:"false"`
	DumpIndexfile lb.LBUINT `short:"i" description:"Dump index file" value-name:"INT" optional:"false"`
	DumpMaster bool `short:"m" description:"Dump master catalog"`
	DumpZapmap bool `short:"z" description:"Dump zapmap"`
}

func main() {
	// Parse flags
	args, err := flags.Parse(&opts)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if opts.DumpMaster {

		os.Exit(0)
	}

	if opts.DumpZapmap {

		os.Exit(0)
	}

	if opts.DumpLogfile > 0 {

		os.Exit(0)
	}

	if opts.DumpIndexfile > 0 {

		os.Exit(0)
	}
}
