package main

import (
	"context"
	"log"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/lsg2020/gactor"
	hello "github.com/lsg2020/gactor/examples/pb"
	"github.com/lsg2020/gactor/executer"
	"github.com/lsg2020/gactor/protocols"
	"github.com/lsg2020/gactor/transports/tcp"
)

var system *gactor.ActorSystem
var single *executer.SingleGoroutine
var selector gactor.Selector
var client *hello.HelloServiceClient

type NodeActor struct {
}

func (p *NodeActor) OnInit(a gactor.Actor) {
	for i := int32(0); i < 40; i++ {
		client.Send(system, a, selector.Addr(), &hello.Request{A: i, B: 10}, nil)
		rsp, err := client.TestCallAdd(system.Context(), system, a, selector.Addr(), &hello.Request{A: i, B: 10}, nil)
		log.Println("test call resource", proto.MarshalTextString(rsp), err)
		a.Sleep(time.Second)
	}
}

func (p *NodeActor) OnRelease(a gactor.Actor) {
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	var err error
	trans := tcp.NewTcp("0.0.0.0:8082", "127.0.0.1:8082")
	system, err = gactor.NewActorSystem(
		gactor.WithName("hello"),
		gactor.WithInstanceId(2),
		gactor.WithEtcd("http://10.21.248.213:2379"),
		gactor.WithContext(ctx),
		gactor.WithTransport(trans),
	)
	if err != nil {
		panic(err)
	}

	single = &executer.SingleGoroutine{}
	single.Start(context.Background(), 1)

	proto := protocols.NewProtobuf(1)
	hello.RegisterHelloService(nil, proto)
	client = hello.NewHelloServiceClient(proto)

	selector, err = gactor.NewRandomSelector(system, "hello")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		gactor.NewActor(&NodeActor{}, single, gactor.ActorWithProto(proto))
	}

	select {
	case <-system.Context().Done():
	}
}
