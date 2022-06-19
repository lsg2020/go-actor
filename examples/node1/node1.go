package main

import (
	"context"
	"log"
	"time"

	goactor "github.com/lsg2020/go-actor"
	hello "github.com/lsg2020/go-actor/examples/pb"
	"github.com/lsg2020/go-actor/executer"
	"github.com/lsg2020/go-actor/protocols/protobuf"
	"github.com/lsg2020/go-actor/transports/tcp"
)

var system *goactor.ActorSystem
var selector goactor.Selector
var client *hello.HelloServiceClient

type HelloActor struct {
	actor goactor.Actor
	addr  *goactor.ActorAddr
}

func (hello *HelloActor) OnInit(a goactor.Actor) {
	hello.actor = a
	hello.addr = system.Register(a, "hello")
	log.Println("actor init", *hello.addr)
}

func (hello *HelloActor) OnRelease(a goactor.Actor) {

}

type HelloService struct {
}

func (s *HelloService) OnSend(ctx *goactor.DispatchMessage, req *hello.Request) error {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnSend in actor:%#v req:%#v\n", a.addr, req.String())
	return nil
}

func (s *HelloService) OnAdd(ctx *goactor.DispatchMessage, req *hello.Request) (*hello.Response, error) {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnAdd in actor:%#v req:%#v\n", a.addr, req.String())
	return &hello.Response{R: req.A + req.B}, nil
}

func (s *HelloService) OnTestCallAdd(ctx *goactor.DispatchMessage, req *hello.Request) (*hello.Response, error) {
	a := ctx.Actor.Instance().(*HelloActor)
	log.Printf("OnCallSourceAdd in actor:%#v req:%#v\n", a.addr, req.String())

	rsp, err := client.Add(system.Context(), system, a.actor, selector.Addr(), req, nil)
	return rsp, err
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	trans := tcp.NewTcp("0.0.0.0:8081", "127.0.0.1:8081")
	var err error
	system, err = goactor.NewActorSystem(
		goactor.WithName("hello"),
		goactor.WithInstanceId(1),
		goactor.WithEtcd("http://10.21.248.213:2379"),
		goactor.WithContext(ctx),
		goactor.WithTransport(trans),
	)
	if err != nil {
		panic(err)
	}

	single := &executer.SingleGoroutine{}
	single.Start(context.Background(), 1)

	proto := protobuf.NewProtobuf(1)
	hello.RegisterHelloService(&HelloService{}, proto)
	client = hello.NewHelloServiceClient(proto)

	selector, err = goactor.NewRandomSelector(system, "hello")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		goactor.NewActor(&HelloActor{}, single, goactor.ActorWithProto(proto))
	}

	select {
	case <-system.Context().Done():
	}
}
