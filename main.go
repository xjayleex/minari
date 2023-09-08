package main

import (
	"fmt"
	"os"

	"github.com/xjayleex/minari/cmd"
)

func main() {
	command := cmd.NewCommand()
	if err := command.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

}
