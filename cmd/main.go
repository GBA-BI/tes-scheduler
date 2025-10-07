package main

import (
	"fmt"
	"os"

	"github.com/GBA-BI/tes-scheduler/pkg/app"
)

func main() {
	command, err := app.NewSchedulerCommand()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	if err = command.Execute(); err != nil {
		os.Exit(1)
	}
}
