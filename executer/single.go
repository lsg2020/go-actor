package executer

import (
	"context"
	"sync"
	"sync/atomic"

	go_actor "github.com/lsg2020/go-actor"
)

type SingleGoroutineResponseInfo struct {
	cond    *sync.Cond
	asyncCB func(msg *go_actor.DispatchMessage)
}

// SingleGoroutine 是一个单协执行器
// 注册在该执行器上的actor消息同时只会有一个协程运行
// 并支持协程的同步等待
type SingleGoroutine struct {
	context     context.Context
	cond        *sync.Cond
	ch          chan *go_actor.DispatchMessage
	nextSession int32

	workId     int
	workAmount int
	waitAmount int

	responseWait map[int]SingleGoroutineResponseInfo
	responseMsg  *go_actor.DispatchMessage
}

func (executer *SingleGoroutine) work(initWg *sync.WaitGroup, workId int) {
	finish := false
	defer func() {
		if finish {
			return
		}
		go executer.work(nil, workId)
		if r := recover(); r != nil {
		}
	}()

	if initWg != nil {
		executer.cond.L.Lock()
		initWg.Done()
		executer.cond.Wait()
	}

	executer.workId = workId
	for {
		select {
		case msg := <-executer.ch:
			protocol := msg.Headers.GetInt(go_actor.HeaderIdProtocol)
			if protocol == go_actor.ProtocolResponse {
				session := msg.Headers.GetInt(go_actor.HeaderIdSession)
				responseWaitInfo, ok := executer.responseWait[session]
				if ok {
					if responseWaitInfo.asyncCB != nil {
						responseWaitInfo.asyncCB(msg)
					} else {
						executer.responseMsg = msg
						responseWaitInfo.cond.Signal()
						executer.cond.Wait()
						executer.workId = workId
					}
				} else {
					logger := go_actor.DefaultLogger()
					if msg.Actor != nil {
						logger = msg.Actor.Logger()
					}
					logger.Warnf("dispatch response miss %d\n", session)
				}
				continue
			}

			cb := msg.Actor.Callback()
			if cb != nil {
				cb(msg)
			} else {
				logger := go_actor.DefaultLogger()
				if msg.Actor != nil {
					logger = msg.Actor.Logger()
				}
				logger.Errorf("actor message callback not set\n")
			}
		case <-executer.context.Done():
			finish = true
			break
		}
	}
}

func (executer *SingleGoroutine) Wait(ctx context.Context, session int) (interface{}, error) {
	responseWaitInfo, ok := executer.responseWait[session]
	if !ok {
		return nil, go_actor.ErrResponseMiss
	}

	executer.waitAmount++

	currentWorkId := executer.workId

	if executer.waitAmount == executer.workAmount {
		executer.workAmount++
		initWg := &sync.WaitGroup{}
		initWg.Add(1)
		go executer.work(initWg, executer.workAmount)

		executer.cond.L.Unlock()
		initWg.Wait()
		executer.cond.L.Lock()
	}

	executer.cond.Signal()
	responseWaitInfo.cond.Wait()

	executer.workId = currentWorkId
	executer.waitAmount--
	delete(executer.responseWait, session)

	if executer.responseMsg.ResponseErr != nil {
		return nil, executer.responseMsg.ResponseErr
	}

	return executer.responseMsg.Content, nil
}

func (executer *SingleGoroutine) Start(ctx context.Context, initWork int) {
	executer.cond = sync.NewCond(new(sync.Mutex))
	executer.nextSession = 0

	executer.context = ctx
	executer.ch = make(chan *go_actor.DispatchMessage, 256)
	executer.responseWait = make(map[int]SingleGoroutineResponseInfo)

	executer.workAmount = initWork

	initWg := &sync.WaitGroup{}
	for i := 0; i < executer.workAmount; i++ {
		initWg.Add(1)
		go executer.work(initWg, i+1)
	}
	initWg.Wait()
	executer.cond.L.Lock()
	executer.cond.Signal()
	executer.cond.L.Unlock()
}

func (executer *SingleGoroutine) OnMessage(msg *go_actor.DispatchMessage) {
	executer.ch <- msg
}

func (executer *SingleGoroutine) OnResponse(session int, err error, data interface{}) {
	msg := &go_actor.DispatchMessage{
		ResponseErr: err,
		Content:     data,
	}
	msg.Headers.Put(
		go_actor.BuildHeaderInt(go_actor.HeaderIdProtocol, go_actor.ProtocolResponse),
		go_actor.BuildHeaderInt(go_actor.HeaderIdSession, session),
	)
	executer.ch <- msg
}

func (executer *SingleGoroutine) NewSession() int {
	session := atomic.AddInt32(&executer.nextSession, 1)
	if session == 0 {
		session = atomic.AddInt32(&executer.nextSession, 1)
	}

	return int(session)
}

func (executer *SingleGoroutine) StartWait(session int, asyncCB func(msg *go_actor.DispatchMessage)) go_actor.SessionCancel {
	cancel := func() {
		delete(executer.responseWait, session)
	}
	if asyncCB != nil {
		executer.responseWait[session] = SingleGoroutineResponseInfo{asyncCB: asyncCB}
		return cancel
	}
	cond := sync.NewCond(executer.cond.L)
	executer.responseWait[session] = SingleGoroutineResponseInfo{cond: cond}
	return cancel
}
