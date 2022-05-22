package executer

import (
	"context"
	"sync"
	"sync/atomic"

	goactor "github.com/lsg2020/go-actor"
)

type MultipleGoroutineResponseInfo struct {
	ch      chan *goactor.DispatchMessage
	asyncCB func(msg *goactor.DispatchMessage)
}

// MultipleGoroutine 是一个多协执行器
// 注册在该执行器上的actor每个请求消息均会创建一个协程来执行
type MultipleGoroutine struct {
	context     context.Context
	nextSession int32

	waitMutex    sync.Mutex
	waitResponse map[int]MultipleGoroutineResponseInfo
}

func (executer *MultipleGoroutine) PreWait(session int, asyncCB func(msg *goactor.DispatchMessage)) goactor.SessionCancel {
	waitInfo := MultipleGoroutineResponseInfo{asyncCB: asyncCB}
	if asyncCB == nil {
		waitInfo.ch = make(chan *goactor.DispatchMessage, 1)
	}
	executer.waitMutex.Lock()
	executer.waitResponse[session] = waitInfo
	executer.waitMutex.Unlock()
	return func() {
		executer.waitMutex.Lock()
		delete(executer.waitResponse, session)
		executer.waitMutex.Unlock()
	}
}

func (executer *MultipleGoroutine) Wait(ctx context.Context, session int) (interface{}, error) {
	executer.waitMutex.Lock()
	waitInfo, ok := executer.waitResponse[session]
	executer.waitMutex.Unlock()

	if !ok || waitInfo.ch == nil {
		return nil, goactor.ErrResponseMiss
	}

	select {
	case msg := <-waitInfo.ch:
		return msg.Content, msg.ResponseErr
	case <-ctx.Done():
		return nil, goactor.ErrCallTimeOut
	}
}

func (executer *MultipleGoroutine) Start(ctx context.Context) {
	executer.nextSession = 0

	executer.context = ctx
	executer.waitResponse = make(map[int]MultipleGoroutineResponseInfo)

}

func (executer *MultipleGoroutine) OnMessage(msg *goactor.DispatchMessage) {
	cb := msg.Actor.Callback()
	if cb != nil {
		go cb(msg)
	}
}

func (executer *MultipleGoroutine) OnResponse(session int, err error, data interface{}) {
	executer.waitMutex.Lock()
	waitInfo, ok := executer.waitResponse[session]
	executer.waitMutex.Unlock()
	if !ok {
		return
	}

	msg := &goactor.DispatchMessage{
		ResponseErr: err,
		Content:     data,
	}
	msg.Headers.Put(
		goactor.BuildHeaderInt(goactor.HeaderIdProtocol, goactor.ProtocolResponse),
		goactor.BuildHeaderInt(goactor.HeaderIdSession, session),
	)
	if waitInfo.asyncCB != nil {
		go waitInfo.asyncCB(msg)
		return
	}

	waitInfo.ch <- msg
}

func (executer *MultipleGoroutine) NewSession() int {
	session := atomic.AddInt32(&executer.nextSession, 1)
	if session == 0 {
		session = atomic.AddInt32(&executer.nextSession, 1)
	}
	return int(session)
}
