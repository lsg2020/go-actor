package gactor

import (
	"bytes"
	"fmt"
	"runtime"
)

const (
	ErrCodeSystem            = 0
	ErrCodeCallTimeOut       = 1
	ErrCodeProtocolNotExists = 2
	ErrCodeResponseTypeErr   = 3
	ErrCodePackErr           = 4
	ErrCodeCmdNotExists      = 5
	ErrCodeNeedDestination   = 6
	ErrCodeActorMiss         = 7
	ErrCodeTransportMiss     = 8
	ErrCodeResponseMiss      = 8
)

var ErrorCodes = []string{
	"System",
	"CallTimeOut",
	"ProtocolNotExists",
	"ResponseTypeErr",
	"PackErr",
	"CmdNotExists",
	"NeedDestination",
	"ActorMiss",
	"TransportMiss",
	"ResponseMiss",
}

var (
	ErrCallTimeOut       = ErrorSimple(ErrCodeCallTimeOut, "")
	ErrProtocolNotExists = ErrorSimple(ErrCodeProtocolNotExists, "")
	ErrNeedDestination   = ErrorSimple(ErrCodeNeedDestination, "")
	ErrActorMiss         = ErrorSimple(ErrCodeActorMiss, "")
	ErrTransportMiss     = ErrorSimple(ErrCodeTransportMiss, "")
	ErrResponseMiss      = ErrorSimple(ErrCodeResponseMiss, "")
)

type ErrFrames []ErrFrame
type ActorError struct {
	Code   int
	Msg    string
	Frames ErrFrames
}

// Frame is a single step in stack trace.
type ErrFrame struct {
	// Func contains a function name.
	Func string
	// Line contains a line number.
	Line int
	// Path contains a file path.
	Path string
}

// String formats Frame to string.
func (f ErrFrame) String() string {
	return fmt.Sprintf("%s:%d %s", f.Path, f.Line, f.Func)
}

func (fs ErrFrames) String() string {
	var buffer bytes.Buffer
	for _, f := range fs {
		buffer.WriteString(f.String())
	}
	return buffer.String()
}

func TraceFrames(skip int) ErrFrames {
	frames := make([]ErrFrame, 0, 20)
	for {
		pc, path, line, ok := runtime.Caller(skip)
		if !ok {
			break
		}
		fn := runtime.FuncForPC(pc)
		frame := ErrFrame{
			Func: fn.Name(),
			Line: line,
			Path: path,
		}
		frames = append(frames, frame)
		skip++
	}
	return frames
}

func (e *ActorError) Error() string {
	if e.Code < len(ErrorCodes) {
		return fmt.Sprintf("code:[%d](%s) msg:%s stack:%s", e.Code, ErrorCodes[e.Code], e.Msg, e.Frames.String())
	}

	return fmt.Sprintf("code:[%d] msg:%s stack:%s", e.Code, e.Msg, e.Frames.String())

}

func ErrorSimple(code int, msg string) *ActorError {
	return &ActorError{
		Code: code,
		Msg:  msg,
	}
}

func ErrorActor(code int, msg string) *ActorError {
	return &ActorError{
		Code:   code,
		Msg:    msg,
		Frames: TraceFrames(2),
	}
}

func Errorf(format string, args ...interface{}) *ActorError {
	return ErrorSimple(ErrCodeSystem, fmt.Sprintf(format, args...))
}

func ErrorWrap(err error) *ActorError {
	return &ActorError{
		Code:   ErrCodeSystem,
		Msg:    err.Error(),
		Frames: TraceFrames(2),
	}
}
