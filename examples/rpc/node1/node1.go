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

type HelloActor struct {
	actor gactor.Actor
	addr  *gactor.ActorAddr
}

func (hello *HelloActor) OnInit(a gactor.Actor) {
	hello.actor = a
	hello.addr = system.Register(a, "hello")
	log.Println("actor init", *hello.addr)
}

func (hello *HelloActor) OnRelease(a gactor.Actor) {

}

type HelloService struct {
}

func (s *HelloService) OnSend(ctx *gactor.DispatchMessage, req *hello.Request) *gactor.ActorError {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnSend in actor:%#v req:%#v\n", a.addr, proto.MarshalTextString(req))
	return nil
}

func (s *HelloService) OnAdd(ctx *gactor.DispatchMessage, req *hello.Request) (*hello.Response, *gactor.ActorError) {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnAdd in actor:%#v req:%#v\n", a.addr, proto.MarshalTextString(req))
	return &hello.Response{R: req.A + req.B}, nil
}

func (s *HelloService) OnCallSourceAdd(ctx *gactor.DispatchMessage, req *hello.Request) (*hello.Response, *gactor.ActorError) {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnCallSourceAdd in actor:%#v req:%#v\n", a.addr, proto.MarshalTextString(req))

	rsp, err := client.Add(system.Context(), system, a.actor, selector.Addr(), req, nil)
	return rsp, err
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	var err error
	trans := tcp.NewTcp("0.0.0.0:8081", "127.0.0.1:8081")
	system, err = gactor.NewActorSystem(
		gactor.WithName("hello"),
		gactor.WithInstanceId(1),
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
	_ = hello.NewHelloServiceServer(&HelloService{}, proto)
	client = hello.NewHelloServiceClient(proto)

	selector, err = gactor.NewRandomSelector(system, "hello")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		gactor.NewActor(&HelloActor{}, single, gactor.ActorWithProto(proto))
	}

	select {
	case <-system.Context().Done():
	}
}
