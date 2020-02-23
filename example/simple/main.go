package main

import (
	"fmt"
	"time"

	"github.com/gadavy/elw"
)

func main() {
	writer, err := elw.NewElasticWriter(elw.Config{
		NodeURIs:    []string{"http://127.0.0.1:9200"},
		DropStorage: true,
	})
	if err != nil {
		panic(err)
	}

	defer writer.Close() // flushes storage, if any

	msg := fmt.Sprintf(`{"message": "test message", "time": "%s"}`, time.Now().Format(time.RFC3339Nano))

	_, _ = writer.Write([]byte(msg))
}
