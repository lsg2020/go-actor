package game

import (
	"strconv"

	goactor "github.com/lsg2020/go-actor"
	message "github.com/lsg2020/go-actor/examples/TicTacToe/pb"
	"github.com/lsg2020/go-actor/protocols/protobuf"
	"github.com/lsg2020/go-actor/protocols/protobuf/tracing"
	"github.com/opentracing/opentracing-go"
)

type gameInfo struct {
	id    uint64
	idStr string
	name  string
	addr  *goactor.ActorAddr
}

type Manager struct {
	actor      goactor.Actor
	NextGameId uint64
	System     *goactor.ActorSystem

	gameList []*gameInfo
}

func (m *Manager) OnInit(actor goactor.Actor) {
	m.actor = actor
	m.NextGameId = 1
	actor.Logger().Info("manager start")
}

func (m *Manager) OnRelease(actor goactor.Actor) {
	actor.Logger().Info("manager release")
}

func (m *Manager) OnNewGame(ctx *goactor.DispatchMessage, req *message.ManagerNewGameRequest) (*message.ManagerNewGameResponse, error) {
	gameId := m.NextGameId
	m.NextGameId++

	gameTracingTags := func(msg *goactor.DispatchMessage) opentracing.Tags {
		if msg.Actor != nil {
			if p, ok := msg.Actor.Instance().(*Game); ok {
				tags := opentracing.Tags{}
				tags["game_id"] = p.gameId
				return tags
			}
		}
		return nil
	}
	tracer := opentracing.GlobalTracer()
	g := &Game{System: m.System}
	managerProto := protobuf.NewProtobuf(1, tracing.InterceptorCallTags(tracer, gameTracingTags), tracing.InterceptorDispatchTags(tracer, gameTracingTags), tracing.InterceptorCall(tracer), tracing.InterceptorDispatch(tracer))
	message.RegisterManagerService(nil, managerProto)

	proto := protobuf.NewProtobuf(2, tracing.InterceptorCallTags(tracer, gameTracingTags), tracing.InterceptorDispatchTags(tracer, gameTracingTags), tracing.InterceptorCall(tracer), tracing.InterceptorDispatch(tracer))
	message.RegisterPlayerService(nil, proto)

	gameProto := protobuf.NewProtobuf(3, tracing.InterceptorCallTags(tracer, gameTracingTags), tracing.InterceptorDispatchTags(tracer, gameTracingTags), tracing.InterceptorCall(tracer), tracing.InterceptorDispatch(tracer))
	message.RegisterGameService(g, gameProto)

	actor := goactor.NewActor(g, m.actor.GetExecuter(), goactor.ActorWithProto(proto), goactor.ActorWithProto(managerProto), goactor.ActorWithProto(gameProto))
	addr := m.System.Register(actor)

	gameIdStr := strconv.FormatUint(gameId, 10)
	actor.Fork(func() {
		g.Init(req.Name, gameIdStr, ProtoToAddr(req.Player), m.actor.GetAddr(m.System))
	})

	m.gameList = append(m.gameList, &gameInfo{
		id:    gameId,
		idStr: gameIdStr,
		name:  req.Name,
		addr:  addr,
	})
	return nil, nil
}

func (m *Manager) OnGetGame(ctx *goactor.DispatchMessage, req *message.ManagerGetGameRequest) (*message.ManagerGetGameResponse, error) {
	for _, game := range m.gameList {
		if game.idStr == req.GameId {
			return &message.ManagerGetGameResponse{
				Game: AddrToProto(game.addr),
			}, nil
		}
	}
	return &message.ManagerGetGameResponse{}, nil
}

func (m *Manager) OnGameList(ctx *goactor.DispatchMessage, req *message.ManagerGameListRequest) (*message.ManagerGameListResponse, error) {
	rsp := &message.ManagerGameListResponse{}
	for _, game := range m.gameList {
		rsp.Games = append(rsp.Games, &message.AvailableGame{
			GameId: game.idStr,
			Name:   game.name,
		})
	}
	return rsp, nil
}

func (m *Manager) OnFreeGame(ctx *goactor.DispatchMessage, req *message.ManagerFreeGameRequest) (*message.ManagerFreeGameResponse, error) {
	for i, game := range m.gameList {
		if game.idStr == req.GameId {
			m.gameList[i] = m.gameList[len(m.gameList)-1]
			m.gameList = m.gameList[:len(m.gameList)-1]
			break
		}
	}
	return &message.ManagerFreeGameResponse{}, nil
}
