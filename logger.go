package go_actor

import (
	"fmt"
	"log"
)

type Logger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Errorf(template string, args ...interface{})
}

type defaultLog struct {
	logger *log.Logger
}

var defaultLogger *defaultLog

func init() {
	defaultLogger = &defaultLog{
		logger: log.Default(),
	}
}

func DefaultLogger() Logger {
	return defaultLogger
}

func (logger *defaultLog) Debugf(template string, args ...interface{}) {
	str := fmt.Sprintf("[Debug] "+template, args...)
	_ = logger.logger.Output(2, str)
}
func (logger *defaultLog) Infof(template string, args ...interface{}) {
	str := fmt.Sprintf("[Info] "+template, args...)
	_ = logger.logger.Output(2, str)
}
func (logger *defaultLog) Warnf(template string, args ...interface{}) {
	str := fmt.Sprintf("[Warn] "+template, args...)
	_ = logger.logger.Output(2, str)
}
func (logger *defaultLog) Errorf(template string, args ...interface{}) {
	str := fmt.Sprintf("[Error] "+template, args...)
	_ = logger.logger.Output(2, str)
}
