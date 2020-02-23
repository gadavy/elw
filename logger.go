package elw

type Logger interface {
	Printf(format string, v ...interface{})
}
