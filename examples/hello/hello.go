package main

import (
	"context"
	"time"

	goactor "github.com/lsg2020/go-actor"
	"github.com/lsg2020/go-actor/executer"
)

type Actor struct {
	Id     int
	Amount int
}

func (a *Actor) OnInit(actor goactor.Actor) {
	actor.Fork(func() {
		for {
			a.Amount++
			actor.Logger().Infof("%#v test sleep", a)
			actor.Sleep(time.Second)
		}
	})

	actor.Timeout(time.Second*3, func() {
		if a.Id != 0 {
			actor.Kill()
		}
	})

	actor.Logger().Infof("actor init %#v", a)
}

func (a *Actor) OnRelease(actor goactor.Actor) {
	actor.Logger().Infof("actor release %#v", a)
}

func main() {
	single := &executer.SingleGoroutine{}
	single.Start(context.Background(), 1)

	for i := 0; i < 10; i++ {
		goactor.NewActor(&Actor{
			Id: i,
		}, single)
	}

	<-context.Background().Done()
}
