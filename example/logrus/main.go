package main

import (
	"time"

	"github.com/gadavy/elw"
	"github.com/sirupsen/logrus"
)

func main() {
	writer, err := elw.NewElasticWriter(elw.Config{
		NodeURIs:    []string{"http://127.0.0.1:9200"},
		DropStorage: true,
	})
	if err != nil {
		panic(err)
	}

	defer writer.Close()

	log := logrus.New()
	log.SetFormatter(&logrus.JSONFormatter{TimestampFormat: time.RFC3339Nano})
	log.SetOutput(writer)

	log.Info("info message to elastic")

	log.Warning("warning message to elastic")

	log.Error("error message to elastic")
}
