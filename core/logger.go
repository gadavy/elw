package core

type Logger interface {
	Printf(format string, v ...interface{})
}
