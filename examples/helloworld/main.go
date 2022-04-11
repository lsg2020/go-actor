package main

import (
	"context"
	"log"
	"time"

	"github.com/lsg2020/gactor"
	"github.com/lsg2020/gactor/executer"
	"github.com/lsg2020/gactor/protocols"
)

var system *gactor.ActorSystem

type HelloWorld struct {
	actor gactor.Actor
	name  string
}

func (p *HelloWorld) OnInit(a gactor.Actor) {
	log.Println("actor start")
	p.actor = a
	system.Register(a, "hello")

	a.Sleep(time.Second)

	a.Timeout(time.Second, func() {
		a.Kill()
	})
}

func (p *HelloWorld) OnRelease(a gactor.Actor) {
	log.Println("actor release")
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	var err error
	system, err = gactor.NewActorSystem(
		gactor.WithName("hello"),
		gactor.WithInstanceId(1),
		gactor.WithEtcd("http://10.21.248.213:2379"),
		gactor.WithContext(ctx),
	)
	if err != nil {
		panic(err)
	}

	executer := &executer.SingleGoroutine{}
	executer.Start(context.Background(), 1)

	proto := protocols.NewRaw(0)

	hello := &HelloWorld{
		name: "hello1",
	}
	_ = gactor.NewActor(hello, executer, nil, []gactor.Proto{proto})

	select {
	case <-system.Context().Done():
	}
}
