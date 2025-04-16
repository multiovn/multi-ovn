package main

import (
	"os"

	"github.com/multiovn/multi-ovn/pkg/daemon"

	"k8s.io/klog"
)

func main() {
	klog.SetOutput(os.Stdout)
	defer klog.Flush()

	config, err := daemon.ParseFlags()
	if err != nil {
		klog.Errorf("parse config failed %v", err)
		os.Exit(1)
	}

	daemon.RunServer(config)
}
