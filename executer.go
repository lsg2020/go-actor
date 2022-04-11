package gactor

import "context"

type SessionCancel func()
type Executer interface {
	NewSession() int
	OnMessage(msg *DispatchMessage)
	OnResponse(session int, err *ActorError, data interface{})
	StartWait(session int, asyncCB func(msg *DispatchMessage)) SessionCancel
	Wait(ctx context.Context, session int) (interface{}, *ActorError)
}
