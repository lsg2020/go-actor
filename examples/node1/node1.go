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
var selector go_actor.Selector
var client *hello.HelloServiceClient

type HelloActor struct {
	actor go_actor.Actor
	addr  *go_actor.ActorAddr
}

func (hello *HelloActor) OnInit(a go_actor.Actor) {
	hello.actor = a
	hello.addr = system.Register(a, "hello")
	log.Println("actor init", *hello.addr)
}

func (hello *HelloActor) OnRelease(a go_actor.Actor) {

}

type HelloService struct {
}

func (s *HelloService) OnSend(ctx *go_actor.DispatchMessage, req *hello.Request) error {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnSend in actor:%#v req:%#v\n", a.addr, req.String())
	return nil
}

func (s *HelloService) OnAdd(ctx *go_actor.DispatchMessage, req *hello.Request) (*hello.Response, error) {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnAdd in actor:%#v req:%#v\n", a.addr, req.String())
	return &hello.Response{R: req.A + req.B}, nil
}

func (s *HelloService) OnTestCallAdd(ctx *go_actor.DispatchMessage, req *hello.Request) (*hello.Response, error) {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnCallSourceAdd in actor:%#v req:%#v\n", a.addr, req.String())

	rsp, err := client.Add(system.Context(), system, a.actor, selector.Addr(), req, nil)
	return rsp, err
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	trans := tcp.NewTcp("0.0.0.0:8081", "127.0.0.1:8081")
	var err error
	system, err = go_actor.NewActorSystem(
		go_actor.WithName("hello"),
		go_actor.WithInstanceId(1),
		go_actor.WithEtcd("http://10.21.248.213:2379"),
		go_actor.WithContext(ctx),
		go_actor.WithTransport(trans),
	)
	if err != nil {
		panic(err)
	}

	single := &executer.SingleGoroutine{}
	single.Start(context.Background(), 1)

	proto := protocols.NewProtobuf(1)
	hello.RegisterHelloService(&HelloService{}, proto)
	client = hello.NewHelloServiceClient(proto)

	selector, err = go_actor.NewRandomSelector(system, "hello")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		go_actor.NewActor(&HelloActor{}, single, go_actor.ActorWithProto(proto))
	}

	select {
	case <-system.Context().Done():
	}
}
