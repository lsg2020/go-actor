package main

import (
	"context"
	"log"
	"time"

	goactor "github.com/lsg2020/go-actor"
	hello "github.com/lsg2020/go-actor/examples/pb"
	"github.com/lsg2020/go-actor/executer"
	"github.com/lsg2020/go-actor/protocols"
	"github.com/lsg2020/go-actor/transports/tcp"
)

var system *goactor.ActorSystem
var selector goactor.Selector
var client *hello.HelloServiceClient

type NodeActor struct {
}

func (p *NodeActor) OnInit(a goactor.Actor) {
	for i := int32(0); i < 40; i++ {
		client.Send(system, a, selector.Addr(), &hello.Request{A: i, B: 10}, nil)
		rsp, err := client.TestCallAdd(system.Context(), system, a, selector.Addr(), &hello.Request{A: i, B: 10}, nil)
		log.Println("test call resource", rsp, err)
		a.Sleep(time.Second)
	}
}

func (p *NodeActor) OnRelease(a goactor.Actor) {
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	var err error
	trans := tcp.NewTcp("0.0.0.0:8082", "127.0.0.1:8082")
	system, err = goactor.NewActorSystem(
		goactor.WithName("hello"),
		goactor.WithInstanceId(2),
		goactor.WithEtcd("http://10.21.248.213:2379"),
		goactor.WithContext(ctx),
		goactor.WithTransport(trans),
	)
	if err != nil {
		panic(err)
	}

	single := &executer.SingleGoroutine{}
	single.Start(context.Background(), 1)

	proto := protocols.NewProtobuf(1)
	hello.RegisterHelloService(nil, proto)
	client = hello.NewHelloServiceClient(proto)

	selector, err = goactor.NewRandomSelector(system, "hello")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		goactor.NewActor(&NodeActor{}, single, goactor.ActorWithProto(proto))
	}

	select {
	case <-system.Context().Done():
	}
}
