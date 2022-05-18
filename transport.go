package goactor

// Transport 处理节点间的请求/回复消息传输
type Transport interface {
	// Name 传输名,同一ActorSystem下需要唯一
	Name() string
	// URI 注册给别的节点的连接信息,如tcp连接端口
	URI() string
	// Init 初始化,监听端口等
	Init(system *ActorSystem) error
	// Send 发送一个消息到远程节点,一般需要设置HeaderIdDestination 目标节点的地址
	Send(msg *DispatchMessage) (SessionCancel, error)
}
