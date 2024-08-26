package log

const (
	DEBUG string = "DEBUG"
	INFO  string = "INFO"
	ERROR string = "ERROR"
)

type Logger interface {
	Infof(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
	Lvl() string
}
