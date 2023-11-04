package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	connect "github.com/pianoyeg94/pg-wal-to-rmq-stream-experiment/internal/rmq_connect"
)

var (
	cfgPath string
	cmd     = cobra.Command{
		Use:  "",
		RunE: run,
	}
)

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func init() {
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Lshortfile | log.LUTC)
	cmd.PersistentFlags().StringVarP(&cfgPath, "config", "c", "config.yaml", "Path to file")
}

func main() {
	if err := cmd.Execute(); err != nil {
		if err, ok := err.(stackTracer); ok {
			for _, f := range err.StackTrace() {
				log.Printf("%v | func %n()\n", f, f)
			}
		}
		log.Fatal(err.Error())
	}
}

func run(_ *cobra.Command, _ []string) error {
	interruptCh := make(chan os.Signal, 1)
	signal.Notify(interruptCh, syscall.SIGINT, syscall.SIGTERM)

	rmqConnect := connect.NewRmqConnect(cfgPath)
	if err := rmqConnect.Start(); err != nil {
		return err
	}

	<-interruptCh
	rmqConnect.Stop()

	return nil
}
