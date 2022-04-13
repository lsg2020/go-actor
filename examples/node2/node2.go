package main

import (
	"context"
	"log"
	"time"

	go_actor "github.com/lsg2020/go-actor"
	hello "github.com/lsg2020/go-actor/examples/pb"
	"github.com/lsg2020/go-actor/executer"
	"github.com/lsg2020/go-actor/protocols"
	"github.com/lsg2020/go-actor/transports/tcp"
)

var system *go_actor.ActorSystem
var single *executer.SingleGoroutine
var selector go_actor.Selector
var client *hello.HelloServiceClient

type NodeActor struct {
}

func (p *NodeActor) OnInit(a go_actor.Actor) {
	for i := int32(0); i < 40; i++ {
		client.Send(system, a, selector.Addr(), &hello.Request{A: i, B: 10}, nil)
		rsp, err := client.TestCallAdd(system.Context(), system, a, selector.Addr(), &hello.Request{A: i, B: 10}, nil)
		log.Println("test call resource", rsp, err)
		a.Sleep(time.Second)
	}
}

func (p *NodeActor) OnRelease(a go_actor.Actor) {
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	var err error
	trans := tcp.NewTcp("0.0.0.0:8082", "127.0.0.1:8082")
	system, err = go_actor.NewActorSystem(
		go_actor.WithName("hello"),
		go_actor.WithInstanceId(2),
		go_actor.WithEtcd("http://10.21.248.213:2379"),
		go_actor.WithContext(ctx),
		go_actor.WithTransport(trans),
	)
	if err != nil {
		panic(err)
	}

	single = &executer.SingleGoroutine{}
	single.Start(context.Background(), 1)

	proto := protocols.NewProtobuf(1)
	hello.RegisterHelloService(nil, proto)
	client = hello.NewHelloServiceClient(proto)

	selector, err = go_actor.NewRandomSelector(system, "hello")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		go_actor.NewActor(&NodeActor{}, single, go_actor.ActorWithProto(proto))
	}

	select {
	case <-system.Context().Done():
	}
}
