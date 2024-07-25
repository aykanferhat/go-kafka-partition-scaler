package log

import (
	"fmt"
)

const (
	INFO  string = "INFO"
	DEBUG string = "DEBUG"
)

type Log interface {
	Infof(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
	Lvl() string
}

var Logger Log = NewConsoleLog(INFO)

func Infof(format string, args ...interface{}) {
	Logger.Printf(fmt.Sprintf(format, args...))
}

func Errorf(format string, args ...interface{}) {
	Logger.Printf(fmt.Sprintf(format, args...))
}
