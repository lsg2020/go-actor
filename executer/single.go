package executer

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	goactor "github.com/lsg2020/go-actor"
	"go.uber.org/zap"
)

type responseInfo struct {
	syncCond *sync.Cond
	asyncCB  func(msg *goactor.DispatchMessage)
}

// SingleGoroutine 是一个单协程执行器
// 注册在该执行器上的actor消息同时只会有一个协程运行
// 并支持协程的同步等待
type SingleGoroutine struct {
	context     context.Context
	cond        *sync.Cond
	ch          chan *goactor.DispatchMessage
	nextSession int32

	workId     int
	workAmount int
	waitAmount int

	responseWait map[int]responseInfo
	responseMsg  *goactor.DispatchMessage

	waitMutex         sync.Mutex
	waitChangeContext context.Context
	waitChangeCancel  context.CancelFunc
	waitContextMap    map[int]context.Context
}

func (executer *SingleGoroutine) work(initWg *sync.WaitGroup, workId int) {
	finish := false
	if initWg != nil {
		executer.cond.L.Lock()
		initWg.Done()
		executer.cond.Wait()
	}

	executer.workId = workId
	for !finish {
		select {
		case msg := <-executer.ch:
			protocol := msg.ProtocolId
			if protocol == goactor.ProtocolResponse {
				session := msg.SessionId
				responseWaitInfo, ok := executer.responseWait[session]
				if ok {
					if responseWaitInfo.asyncCB != nil {
						responseWaitInfo.asyncCB(msg)
					} else {
						executer.responseMsg = msg
						responseWaitInfo.syncCond.Signal()
						executer.cond.Wait()
						executer.workId = workId
					}
				} else {
					logger := goactor.DefaultLogger()
					logger.Warn("dispatch response miss", zap.Int("session", session))
				}
				continue
			}

			cb := msg.Actor.Callback()
			if cb == nil {
				msg.MaybeResponseErr(goactor.ErrActorMiss)

				logger := goactor.DefaultLogger()
				if msg.Actor != nil {
					logger = msg.Actor.Logger()
				}
				logger.Error("actor state stop")
				continue
			}

			cb(msg)
		case <-executer.context.Done():
			finish = true
			break
		}
	}
}

func (executer *SingleGoroutine) Wait(ctx context.Context, session int) (interface{}, error) {
	responseWaitInfo, ok := executer.responseWait[session]
	if !ok {
		return nil, goactor.ErrResponseMiss
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

	executer.waitMutex.Lock()
	executer.waitContextMap[session] = ctx
	if executer.waitChangeCancel != nil {
		executer.waitChangeCancel()
		executer.waitChangeCancel = nil
	}
	executer.waitMutex.Unlock()

	executer.cond.Signal()
	responseWaitInfo.syncCond.Wait()

	executer.waitMutex.Lock()
	delete(executer.waitContextMap, session)
	if executer.waitChangeCancel != nil {
		executer.waitChangeCancel()
		executer.waitChangeCancel = nil
	}
	executer.waitMutex.Unlock()

	executer.workId = currentWorkId
	executer.waitAmount--
	delete(executer.responseWait, session)

	if executer.responseMsg.ResponseErr != nil {
		return nil, executer.responseMsg.ResponseErr
	}

	return executer.responseMsg.Content, nil
}

func (executer *SingleGoroutine) Start(ctx context.Context, initWorkAmount int) {
	executer.cond = sync.NewCond(new(sync.Mutex))
	executer.nextSession = 0

	executer.context = ctx
	executer.ch = make(chan *goactor.DispatchMessage, 256)
	executer.responseWait = make(map[int]responseInfo)

	executer.workAmount = initWorkAmount

	executer.waitChangeContext, executer.waitChangeCancel = context.WithCancel(ctx)
	executer.waitContextMap = make(map[int]context.Context)
	go func() {
		cases := make([]reflect.SelectCase, 0, 128)
		sessions := make([]int, 0, 128)
		for {
			cases = cases[:0]
			sessions = sessions[:0]

			executer.waitMutex.Lock()
			executer.waitChangeContext, executer.waitChangeCancel = context.WithCancel(ctx)
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
			sessions = append(sessions, 0)
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(executer.waitChangeContext.Done())})
			sessions = append(sessions, 0)

			for session, ctx := range executer.waitContextMap {
				cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
				sessions = append(sessions, session)
			}
			executer.waitMutex.Unlock()

			chosen, _, _ := reflect.Select(cases)

			if chosen == 0 {
				break
			}
			if chosen == 1 {
				time.Sleep(time.Millisecond * 100)
				continue
			}

			session := sessions[chosen]
			executer.waitMutex.Lock()
			_, exists := executer.waitContextMap[session]
			if exists {
				delete(executer.waitContextMap, session)
			}
			executer.waitMutex.Unlock()
			if exists {
				executer.OnResponse(session, goactor.ErrCallTimeOut, nil)
			}
		}
	}()

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

func (executer *SingleGoroutine) OnMessage(msg *goactor.DispatchMessage) {
	executer.ch <- msg
}

func (executer *SingleGoroutine) OnResponse(session int, err error, data interface{}) {
	msg := goactor.NewDispatchMessage(nil, nil, nil, goactor.ProtocolResponse, session, nil, data, err, nil)
	executer.ch <- msg
}

func (executer *SingleGoroutine) NewSession() int {
	session := atomic.AddInt32(&executer.nextSession, 1)
	if session == 0 {
		session = atomic.AddInt32(&executer.nextSession, 1)
	}

	return int(session)
}

func (executer *SingleGoroutine) PreWait(session int, asyncCB func(msg *goactor.DispatchMessage)) goactor.SessionCancel {
	cancel := func() {
		delete(executer.responseWait, session)
	}
	if asyncCB != nil {
		executer.responseWait[session] = responseInfo{asyncCB: asyncCB}
		return cancel
	}
	cond := sync.NewCond(executer.cond.L)
	executer.responseWait[session] = responseInfo{syncCond: cond}
	return cancel
}
