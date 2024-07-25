package log

import (
	"fmt"
	goLog "log"
	"os"
)

type ConsoleLog struct {
	saramaLog *goLog.Logger
	scalerLog *goLog.Logger
	level     string
}

func NewConsoleLog(level string) *ConsoleLog {
	return &ConsoleLog{
		level:     level,
		saramaLog: goLog.New(os.Stdout, "[Sarama] ", goLog.LstdFlags),
		scalerLog: goLog.New(os.Stdout, "[KafkaPartitionScaler] ", goLog.LstdFlags),
	}
}

func (c *ConsoleLog) Infof(format string, args ...interface{}) {
	c.scalerLog.Printf(fmt.Sprintf(format, args...))
}

func (c *ConsoleLog) Debugf(format string, args ...interface{}) {
	c.scalerLog.Printf(fmt.Sprintf(format, args...))
}

func (c *ConsoleLog) Errorf(format string, args ...interface{}) {
	c.scalerLog.Printf(fmt.Sprintf(format, args...))
}

func (c *ConsoleLog) Print(v ...interface{}) {
	c.saramaLog.Print(v...)
}

func (c *ConsoleLog) Printf(format string, v ...interface{}) {
	c.saramaLog.Printf(format, v...)
}

func (c *ConsoleLog) Println(v ...interface{}) {
	c.saramaLog.Println(v...)
}

func (c *ConsoleLog) Lvl() string {
	return c.level
}
