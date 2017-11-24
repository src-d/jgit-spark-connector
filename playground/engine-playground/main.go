package main

import (
	"github.com/src-d/go-compose-installer"
)

func main() {
	cfg := installer.NewDefaultConfig()
	cfg.ProjectName = "engine-playground"

	cfg.Compose = [][]byte{
		MustAsset("docker-compose.yml.tmpl"),
	}

	cfg.Install.Execs = []*installer.Exec{{
		Service: "bblfshd",
		Cmd:     []string{"bblfshctl", "driver", "install", "--all", "--update"},
	}}
	cfg.Install.Execs = []*installer.Exec{{
		Service: "jupyter",
		Cmd:     []string{"cp", "-rf", "/opt/engine/samples", "/opt/engine/workspace"},
	}}
	p, err := installer.New(cfg.ProjectName, cfg)
	if err != nil {
		panic(err)
	}

	p.Run()
}
