package gactor

type Transport interface {
	Name() string
	URI() string
	Init(system *ActorSystem) error
	Send(msg *DispatchMessage) (SessionCancel, *ActorError)
}
