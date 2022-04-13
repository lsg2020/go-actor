package executer

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/lsg2020/gactor"
)

type SingleGoroutineResponseInfo struct {
	cond    *sync.Cond
	asyncCB func(msg *gactor.DispatchMessage)
}

// SingleGoroutine 是一个单协执行器
// 注册在该执行器上的actor消息同时只会有一个协程运行
// 并支持协程的同步等待
type SingleGoroutine struct {
	context     context.Context
	cond        *sync.Cond
	ch          chan *gactor.DispatchMessage
	nextSession int32

	workId     int
	workAmount int
	waitAmount int

	responseWait map[int]SingleGoroutineResponseInfo
	responseMsg  *gactor.DispatchMessage
}

func (executer *SingleGoroutine) work(initWg *sync.WaitGroup, workId int) {
	finish := false
	for !finish {
		executer.cond.L.Lock()
		initWg.Done()
		executer.cond.Wait()

		executer.workId = workId
		for !finish {
			select {
			case msg := <-executer.ch:
				protocol := msg.Headers.GetInt(gactor.HeaderIdProtocol)
				if protocol == gactor.ProtocolResponse {
					session := msg.Headers.GetInt(gactor.HeaderIdSession)
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
						msg.System.Logger().Warnf("dispatch response miss %d\n", session)
					}
					continue
				}

				cb := msg.Actor.Callback()
				if cb != nil {
					cb(msg)
				} else {
					msg.System.Logger().Errorf("actor message callback not set\n")
				}
			case <-executer.context.Done():
				finish = true
				break
			}
		}

		executer.cond.L.Unlock()
	}
}

func (executer *SingleGoroutine) Wait(ctx context.Context, session int) (interface{}, *gactor.ActorError) {
	responseWaitInfo, ok := executer.responseWait[session]
	if !ok {
		return nil, gactor.ErrResponseMiss
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
	executer.ch = make(chan *gactor.DispatchMessage, 256)
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

func (executer *SingleGoroutine) OnMessage(msg *gactor.DispatchMessage) {
	executer.ch <- msg
}

func (executer *SingleGoroutine) OnResponse(session int, err *gactor.ActorError, data interface{}) {
	msg := &gactor.DispatchMessage{
		ResponseErr: err,
		Content:     data,
	}
	msg.Headers.Put(
		gactor.BuildHeaderInt(gactor.HeaderIdProtocol, gactor.ProtocolResponse),
		gactor.BuildHeaderInt(gactor.HeaderIdSession, session),
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

func (executer *SingleGoroutine) StartWait(session int, asyncCB func(msg *gactor.DispatchMessage)) gactor.SessionCancel {
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
