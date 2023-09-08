package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/xjayleex/minari/shipper/control"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "minari-shipper-server [subcommand]",
	}

	run := runCmd()

	cmd.AddCommand(run)
	cmd.Run = run.Run
	return cmd
}

func runCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Start the minari-shipper-server",
		Run: func(_ *cobra.Command, _ []string) {
			if err := control.LoadAndRun(); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n\n", err)
				os.Exit(1)
			}
		},
	}
}
