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
	cfg.Install.Messages.Success = "" +
		"engine-playground was successfully installed!\n\n" +

		"This playground is based on Jupyter notebooks, you can start at:\n" +
		"http://localhost:8080/notebooks/notebooks/playground.ipynb\n\n" +

		"A workspace has being created at `{{.Home}}/.engine/`.\n\n" +

		"The `notebooks` directories should be used to store all the notebooks, any\n" +
		"notebooks outside of this path can't be accessed. The `repositories` directory\n" +
		"is the path to store all the repositories to be analyzed.\n\n" +

		"To uninstall this the playground please execute:\n" +
		"./engine-playground uninstall\n"

	cfg.Install.Execs = []*installer.Exec{
		{Service: "bblfshd", Cmd: "bblfshctl driver install --all --update"},
		{Service: "jupyter", Cmd: "cp -rf /opt/engine/samples/* /opt/engine/workspace/"},
	}

	p, err := installer.New(cfg.ProjectName, cfg)
	if err != nil {
		panic(err)
	}

	p.Run()
}
