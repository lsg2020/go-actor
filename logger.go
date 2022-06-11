package goactor

import "go.uber.org/zap"

var defaultLogger *zap.Logger

func init() {
	defaultLogger, _ = zap.NewDevelopment()
}

func DefaultLogger() *zap.Logger {
	return defaultLogger
}
