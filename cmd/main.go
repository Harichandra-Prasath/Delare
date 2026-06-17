package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Harichandra-Prasath/Delare/internal/logging"
)

type Config struct {
	LogLevel string // Log level for delared
	Containers string // Comma Seperated Container Names
}

func ParseConfig(args []string) (*Config, error) {
	fs := flag.NewFlagSet("delared",flag.ContinueOnError)
	cfg := &Config{}
	
	fs.StringVar(&cfg.Containers,"containers","","Comma Seperated list of containers")
	fs.StringVar(&cfg.LogLevel,"log-level","info","Log Level for Delare")

	err := fs.Parse(args); if err!=nil{
		return nil, err
	}
	
	if cfg.Containers==""{
		return nil, fmt.Errorf("delared atleast needs one container..")
	}

	return cfg,nil
}


func main() {
	
	cfg,err := ParseConfig(os.Args[1:])
	if err!=nil{
		panic(err)
	}
	
	logging.InitialiseLogger(cfg.LogLevel)

	containers := strings.Split(cfg.Containers,",")
	for _,cntr := range containers{
		logging.Logger.Info("Container Registerd in Delare","Name",cntr)
	}
}
