package executer

import (
	"context"
	"sync"
	"sync/atomic"

	go_actor "github.com/lsg2020/go-actor"
)

type MultipleGoroutineResponseInfo struct {
	ch      chan *go_actor.DispatchMessage
	asyncCB func(msg *go_actor.DispatchMessage)
}

// MultipleGoroutine 是一个多协执行器
// 注册在该执行器上的actor每个请求消息均会创建一个协程来执行
type MultipleGoroutine struct {
	context     context.Context
	nextSession int32

	waitMutex    sync.Mutex
	waitResponse map[int]MultipleGoroutineResponseInfo
}

func (executer *MultipleGoroutine) StartWait(session int, asyncCB func(msg *go_actor.DispatchMessage)) go_actor.SessionCancel {
	waitInfo := MultipleGoroutineResponseInfo{asyncCB: asyncCB}
	if asyncCB == nil {
		waitInfo.ch = make(chan *go_actor.DispatchMessage, 1)
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
		return nil, go_actor.ErrResponseMiss
	}

	select {
	case msg := <-waitInfo.ch:
		return msg.Content, msg.ResponseErr
	case <-ctx.Done():
		return nil, go_actor.ErrCallTimeOut
	}
}

func (executer *MultipleGoroutine) Start(ctx context.Context) {
	executer.nextSession = 0

	executer.context = ctx
	executer.waitResponse = make(map[int]MultipleGoroutineResponseInfo)

}

func (executer *MultipleGoroutine) OnMessage(msg *go_actor.DispatchMessage) {
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

	msg := &go_actor.DispatchMessage{
		ResponseErr: err,
		Content:     data,
	}
	msg.Headers.Put(
		go_actor.BuildHeaderInt(go_actor.HeaderIdProtocol, go_actor.ProtocolResponse),
		go_actor.BuildHeaderInt(go_actor.HeaderIdSession, session),
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
