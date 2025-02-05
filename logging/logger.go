package logging

import (
	"github.com/phuslu/log"
)

func CreateDebugLogger() *log.Logger {
	return &log.Logger{
		Level:  log.DebugLevel,
		Caller: 0,
		Writer: &log.ConsoleWriter{
			ColorOutput:    false,
			EndWithMessage: true,
		},
	}
}
