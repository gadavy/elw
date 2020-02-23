# ElasticLogWriter

This writer delivers the data to Elasticsearch, a NoSQL search engine. 

## Installation

`go get github.com/gadavy/elw`

## Quick Start

Basic usage

```go
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
```

Usage with [Logrus](https://github.com/sirupsen/logrus)


```go
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
    
    log.Info("test message to elastic")
}
```
