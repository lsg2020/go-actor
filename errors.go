package goactor

import (
	"fmt"

	"github.com/pkg/errors"
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
	ErrCodeResponseMiss      = 9
	ErrCodeNodeMiss          = 10
	ErrCodeActorStop         = 11
	ErrCodeForgetResponse    = 12
	ErrCodeMessageErr        = 13
)

var (
	ErrInitNeedName       = Error(ErrCodeSystem, "actor system name not set")
	ErrInitNeedInstanceId = Error(ErrCodeSystem, "actor system instance id not set")
	ErrTransportMiss      = Error(ErrCodeTransportMiss, "transport miss")
	ErrNeedDestination    = Error(ErrCodeNeedDestination, "need destination")
	ErrActorMiss          = Error(ErrCodeActorMiss, "actor miss")
	ErrProtocolNotExists  = Error(ErrCodeProtocolNotExists, "protocol not exists")
	ErrCallTimeOut        = Error(ErrCodeCallTimeOut, "call timeout")
	ErrResponseTypeErr    = Error(ErrCodeResponseTypeErr, "response type error")
	ErrResponseMiss       = Error(ErrCodeResponseMiss, "response miss")
	ErrPackErr            = Error(ErrCodePackErr, "pack err")
	ErrCmdNotExists       = Error(ErrCodeCmdNotExists, "cmd not exists")
	ErrNodeMiss           = Error(ErrCodeNodeMiss, "node miss")
	ErrActorStop          = Error(ErrCodeActorStop, "actor stop")
	ErrForgetResponse     = Error(ErrCodeForgetResponse, "forget response")
	ErrMessageErr         = Error(ErrCodeMessageErr, "message error")
)

type ActorError struct {
	Code int
	Msg  string
}

func (e *ActorError) Error() string {
	return fmt.Sprintf("[%d]%s", e.Code, e.Msg)
}

func Error(code int, msg string) *ActorError {
	return &ActorError{
		Code: code,
		Msg:  msg,
	}
}

func ErrorWrapf(err error, format string, args ...interface{}) *ActorError {
	if e, ok := err.(*ActorError); ok {
		e.Msg = fmt.Sprintf(format, args...) + ": " + e.Msg
		return e
	}

	return &ActorError{
		Code: ErrCodeSystem,
		Msg:  errors.Wrapf(err, format, args...).Error(),
	}
}
