package game

import (
	"fmt"
	"time"

	goactor "github.com/lsg2020/go-actor"
	message "github.com/lsg2020/go-actor/examples/TicTacToe/pb"
	"github.com/lsg2020/go-actor/examples/TicTacToe/tracing"
	"github.com/lsg2020/go-actor/protocols"
	"go.uber.org/zap"
)

var ExistsPlayer = make(map[uint64]bool)

func NewPlayer(system *goactor.ActorSystem, e goactor.Executer) *goactor.ActorAddr {
	p := &Player{System: system}
	managerProto := protocols.NewProtobuf(1, tracing.InterceptorCall(), tracing.InterceptorDispatch())
	message.RegisterManagerService(nil, managerProto)

	proto := protocols.NewProtobuf(2, goactor.ProtoWithInterceptorDispatch(func(msg *goactor.DispatchMessage, handler goactor.ProtoHandler, args ...interface{}) error {
		p.KeepLive()
		return handler(msg, args...)
	}), tracing.InterceptorCall(), tracing.InterceptorDispatch())
	message.RegisterPlayerService(p, proto)

	gameProto := protocols.NewProtobuf(3, tracing.InterceptorCall(), tracing.InterceptorDispatch())
	message.RegisterGameService(nil, gameProto)

	actor := goactor.NewActor(p, e, goactor.ActorWithProto(proto), goactor.ActorWithProto(managerProto), goactor.ActorWithProto(gameProto))
	addr := system.Register(actor)
	return addr
}

type Player struct {
	actor           goactor.Actor
	Name            string
	heartbeat       int64
	System          *goactor.ActorSystem
	ManagerSelector goactor.Selector
	ManagerClient   *message.ManagerServiceClient
	GameClient      *message.GameServiceClient

	gameIds   []string
	gameAddrs []*goactor.ActorAddr
}

func ProtoToAddr(addr *message.ActorAddress) *goactor.ActorAddr {
	return &goactor.ActorAddr{
		NodeInstanceId: addr.NodeInstanceId,
		Handle:         goactor.ActorHandle(addr.Handle),
	}
}
func AddrToProto(addr *goactor.ActorAddr) *message.ActorAddress {
	return &message.ActorAddress{
		NodeInstanceId: addr.NodeInstanceId,
		Handle:         uint32(addr.Handle),
	}
}

func (p *Player) OnInit(actor goactor.Actor) {
	p.actor = actor
	p.Name = "***"
	p.heartbeat = time.Now().Unix()
	p.ManagerSelector, _ = goactor.NewRandomSelector(p.System, "manager")
	p.ManagerClient = message.NewManagerServiceClient(actor.GetProto(1))
	p.GameClient = message.NewGameServiceClient(actor.GetProto(3))

	actor.Fork(func() {
		for {
			if time.Now().Add(-time.Second*10).Unix() > p.heartbeat {
				actor.Kill()
				return
			}
			actor.Sleep(time.Second)
		}
	})

	actor.Logger().Info("player start", zap.String("name", p.Name))
}

func (p *Player) OnRelease(actor goactor.Actor) {
	for _, addr := range p.gameAddrs {
		p.GameClient.Leave(p.actor.Context(), p.System, p.actor, addr, &message.GameLeaveRequest{
			Player: AddrToProto(p.actor.GetAddr(p.System)),
		}, nil)
	}

	actor.Logger().Info("player release", zap.String("name", p.Name))

	delete(ExistsPlayer, uint64(p.actor.GetAddr(p.System).Handle)) // TODO Panic 被默认捕获
}

func (p *Player) KeepLive() {
	p.heartbeat = time.Now().Unix()
}

func (p *Player) OnSetName(ctx *goactor.DispatchMessage, req *message.PlayerSetNameRequest) (*message.PlayerSetNameResponse, error) {
	p.Name = req.Name
	return &message.PlayerSetNameResponse{}, nil
}

func (p *Player) OnIndex(ctx *goactor.DispatchMessage, req *message.PlayerIndexRequest) (*message.PlayerIndexResponse, error) {
	gameList, err := p.ManagerClient.GameList(p.actor.Context(), p.System, p.actor, p.ManagerSelector.Addr(), &message.ManagerGameListRequest{}, ctx.ExtractEx(goactor.HeaderIdTracingSpan))
	if err != nil {
		return nil, err
	}
	games := make([]*message.GameInfo, 0, len(p.gameAddrs))
	for _, addr := range p.gameAddrs {
		info, err := p.GameClient.Info(p.actor.Context(), p.System, p.actor, addr, &message.GameInfoRequest{Player: AddrToProto(p.actor.GetAddr(p.System))}, ctx.ExtractEx(goactor.HeaderIdTracingSpan))
		if err != nil {
			return nil, err
		}
		games = append(games, info.Game)
	}

	return &message.PlayerIndexResponse{
		Games:      games,
		Availables: gameList.Games,
	}, nil
}

func (p *Player) OnCreateGame(ctx *goactor.DispatchMessage, req *message.PlayerCreateGameRequest) (*message.PlayerCreateGameResponse, error) {
	if len(p.gameIds) > 5 {
		return nil, fmt.Errorf("exists game")
	}
	_, err := p.ManagerClient.NewGame(p.actor.Context(), p.System, p.actor, p.ManagerSelector.Addr(), &message.ManagerNewGameRequest{
		Name:   p.Name,
		Player: AddrToProto(p.actor.GetAddr(p.System)),
	}, ctx.ExtractEx(goactor.HeaderIdTracingSpan))
	if err != nil {
		return nil, err
	}

	return &message.PlayerCreateGameResponse{}, nil
}

func (p *Player) getGameAddr(gameId string) *goactor.ActorAddr {
	for i, addr := range p.gameAddrs {
		if p.gameIds[i] == gameId {
			return addr
		}
	}
	return nil
}

func (p *Player) OnGetMoves(ctx *goactor.DispatchMessage, req *message.PlayerGetMovesRequest) (*message.PlayerGetMovesResponse, error) {
	addr := p.getGameAddr(req.GameId)
	if addr == nil {
		return nil, fmt.Errorf("game %s not exists", req.GameId)
	}

	rsp, err := p.GameClient.GetMoves(p.actor.Context(), p.System, p.actor, addr, &message.GameGetMovesRequest{
		Player: AddrToProto(p.actor.GetAddr(p.System)),
	}, ctx.ExtractEx(goactor.HeaderIdTracingSpan))
	if err != nil {
		return nil, err
	}

	return &message.PlayerGetMovesResponse{Moves: rsp.Info}, nil
}
func (p *Player) OnMakeMove(ctx *goactor.DispatchMessage, req *message.PlayerMakeMoveRequest) (*message.PlayerMakeMoveResponse, error) {
	addr := p.getGameAddr(req.GameId)
	if addr == nil {
		return nil, fmt.Errorf("game %s not exists", req.GameId)
	}

	_, err := p.GameClient.MakeMove(p.actor.Context(), p.System, p.actor, addr, &message.GameMakeMoveRequest{
		Player: AddrToProto(p.actor.GetAddr(p.System)),
		X:      req.X,
		Y:      req.Y,
	}, ctx.ExtractEx(goactor.HeaderIdTracingSpan))
	if err != nil {
		return nil, err
	}
	return &message.PlayerMakeMoveResponse{}, nil
}

func (p *Player) OnJoin(ctx *goactor.DispatchMessage, req *message.PlayerJoinRequest) (*message.PlayerJoinResponse, error) {
	gameAddr, err := p.ManagerClient.GetGame(p.actor.Context(), p.System, p.actor, p.ManagerSelector.Addr(), &message.ManagerGetGameRequest{
		GameId: req.GameId,
	}, ctx.ExtractEx(goactor.HeaderIdTracingSpan))
	if err != nil || gameAddr.Game == nil {
		return &message.PlayerJoinResponse{}, nil
	}
	p.GameClient.Join(p.actor.Context(), p.System, p.actor, ProtoToAddr(gameAddr.Game), &message.GameJoinRequest{
		Player: AddrToProto(p.actor.GetAddr(p.System)),
	}, ctx.ExtractEx(goactor.HeaderIdTracingSpan))

	return &message.PlayerJoinResponse{}, nil
}

func (p *Player) OnOnJoinGame(ctx *goactor.DispatchMessage, req *message.PlayerOnJoinGameRequest) (*message.PlayerOnJoinGameResponse, error) {
	p.actor.Logger().Info("join game", zap.String("game_id", req.GameId), zap.Any("handle", req.Addr.Handle))
	p.gameIds = append(p.gameIds, req.GameId)
	p.gameAddrs = append(p.gameAddrs, ProtoToAddr(req.Addr))
	return &message.PlayerOnJoinGameResponse{Name: p.Name}, nil
}

func (p *Player) OnOnLeaveGame(ctx *goactor.DispatchMessage, req *message.PlayerOnLeaveGameRequest) (*message.PlayerOnLeaveGameResponse, error) {
	p.actor.Logger().Info("leave game", zap.String("game_id", req.GameId))

	for i, gameId := range p.gameIds {
		if gameId == req.GameId {
			p.gameIds[i] = p.gameIds[len(p.gameIds)-1]
			p.gameIds = p.gameIds[:len(p.gameIds)-1]
			p.gameAddrs[i] = p.gameAddrs[len(p.gameAddrs)-1]
			p.gameAddrs = p.gameAddrs[:len(p.gameAddrs)-1]
			break
		}
	}
	return &message.PlayerOnLeaveGameResponse{}, nil
}
