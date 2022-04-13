# go-actor
* go-actor is a lightweight message framework using actor model  

## 初衷
* 想能在代码逻辑上方便的写无锁的同步rpc调用代码,同时又不会阻塞住其他服务对这个Actor的调用
* 一个Actor可以有多种身份,想能比较方便的分类管理Actor,分类通过Actor地址调用目标Actor
* 节点间使用多种传输方式,比如游戏玩家移动这种同步可以使用tcp实现快速通知消息丢失也不太紧要,平台奖励类的可以使用mq通知

## 快速开始
* `创建ActorSystem` ActorSystem用来分类管理Actor,调用某个Actor的时候需要基于他所在的同名ActorSystem
    * `gactor.WithName`: 指定ActorSystem名字,同类同名
    * `gactor.WithInstanceId`: 指定节点id,同类型下唯一
    * `gactor.WithEtcd`: etcd注册地址,同类型使用同组etcd
    * `gactor.WithTransport`: 关联传输器
```go
	system, err := gactor.NewActorSystem(
		gactor.WithName("hello"),
		gactor.WithInstanceId(1),
		gactor.WithEtcd("http://127.0.0.1:2379"),
		gactor.WithTransport(trans),
	)
```

* `创建执行器`
    * `SingleGoroutine`: 单协程执行器，每个执行器同时只有一个协程在执行逻辑代码，当执行到`Executer.Wait`时会让出执行权给同执行器的其他协程处理消息，自己挂起直到`Executer.OnResponse`对应的session唤醒并等待其他协程让出执行权
```go
	single := &executer.SingleGoroutine{}
	single.Start(context.Background(), 1)
```

* `创建协议`
    * `protobuf`
        * go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
        * go install github.com/lsg2020/go-actor/tools/protoc-gen-gactor@latest
        * 定义协议文件[例如](https://github.com/lsg2020/go-actor/tree/examples/pb/hello.proto)
        * 生成 `protoc -I . --go_out=. --gactor_out=. *.proto`
```go
	proto := protocols.NewProtobuf(1)
	hello.RegisterHelloService(&HelloService{}, proto)
```

* `创建Actor`并指定个名字注册到ActorSystem
```go
	func (hello *HelloActor) OnInit(a gactor.Actor) {
		hello.actor = a
		hello.addr = system.Register(a, "hello")
	}

	gactor.NewActor(&HelloActor{}, single, gactor.ActorWithProto(proto))
```

