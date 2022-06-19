package main

import (
	"context"
	"fmt"
	"log"
	"time"

	goactor "github.com/lsg2020/go-actor"
	hello "github.com/lsg2020/go-actor/examples/pb"
	"github.com/lsg2020/go-actor/executer"
	"github.com/lsg2020/go-actor/protocols/protobuf"
	"github.com/lsg2020/go-actor/protocols/protobuf/tracing"
	"github.com/opentracing/opentracing-go"
)

var system *goactor.ActorSystem
var client *hello.HelloServiceClient
var selector goactor.Selector

type HelloActor struct {
	actor goactor.Actor
	addr  *goactor.ActorAddr
	name  string
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

	rsp1, err1 := client.Add(ctx.Context(), system, a.actor, selector.Addr(), req, nil)
	if err1 != nil {
		return nil, err1
	}
	rsp2, err2 := client.Add(ctx.Context(), system, a.actor, selector.Addr(), req, nil)
	if err2 != nil {
		return nil, err2
	}
	return &hello.Response{R: rsp1.R + rsp2.R}, nil
}

type ClientActor struct {
}

func (c *ClientActor) OnInit(a goactor.Actor) {
	for i := int32(0); true; i++ {
		client.Send(system, a, selector.Addr(), &hello.Request{A: i, B: 10}, nil)
		rsp, err := client.TestCallAdd(system.Context(), system, a, selector.Addr(), &hello.Request{A: i, B: 10}, nil)
		log.Println("test call resource", rsp, err)
		a.Sleep(time.Second)
	}
}

func (c *ClientActor) OnRelease(a goactor.Actor) {
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	var err error
	system, err = goactor.NewActorSystem(
		goactor.WithName("tracing"),
		goactor.WithInstanceId(1),
		goactor.WithEtcd("http://10.21.248.213:2379"),
		goactor.WithContext(ctx),
	)
	if err != nil {
		panic(err)
	}

	single := &executer.SingleGoroutine{}
	single.Start(context.Background(), 1)

	tracer, _, err := tracing.NewTracer("hello_4", "10.21.248.28:5775")
	if err != nil {
		panic(err)
	}
	opentracing.SetGlobalTracer(tracer)

	tagName := func(msg *goactor.DispatchMessage) opentracing.Tags {
		if msg.Actor != nil {
			if hello, ok := msg.Actor.Instance().(*HelloActor); ok {
				tags := opentracing.Tags{}
				tags["name"] = hello.name
				return tags
			}
		}
		return nil
	}

	proto := protobuf.NewProtobuf(1, tracing.InterceptorCallTags(tracer, tagName), tracing.InterceptorDispatchTags(tracer, tagName), tracing.InterceptorCall(tracer), tracing.InterceptorDispatch(tracer))
	hello.RegisterHelloService(&HelloService{}, proto)
	client = hello.NewHelloServiceClient(proto)

	selector, err = goactor.NewRandomSelector(system, "hello")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		goactor.NewActor(&HelloActor{name: fmt.Sprintf("hello_%d", i)}, single, goactor.ActorWithProto(proto))
	}

	goactor.NewActor(&ClientActor{}, single, goactor.ActorWithProto(proto))

	select {
	case <-system.Context().Done():
	}
}
