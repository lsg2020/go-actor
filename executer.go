package goactor

import "context"

// SessionCancel 取消等待
type SessionCancel func()

// Executer actor的消息执行器,消息处理及协调等待
type Executer interface {
	// NewSession 分配一个session
	NewSession() int
	// OnMessage 接收到某个消息,分配执行
	OnMessage(msg *DispatchMessage)
	// OnResponse 接收到某个回复,触发唤醒
	OnResponse(session int, err error, data interface{})
	// PreWait 触发session同步/异步等待,返回取消函数
	PreWait(session int, asyncCB func(msg *DispatchMessage)) SessionCancel
	// Wait 同步等待session返回结果
	Wait(ctx context.Context, session int) (interface{}, error)
}
