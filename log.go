package partitionscaler

type Logger interface {
	Infof(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Lvl() string
	// Print for sarama logger
	Print(v ...interface{})
	// Printf for sarama logger
	Printf(format string, v ...interface{})
	// Println for sarama logger
	Println(v ...interface{})
}
