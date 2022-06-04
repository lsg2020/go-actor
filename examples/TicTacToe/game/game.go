package game

import (
	"fmt"

	goactor "github.com/lsg2020/go-actor"
	message "github.com/lsg2020/go-actor/examples/TicTacToe/pb"
)

type Game struct {
	actor  goactor.Actor
	System *goactor.ActorSystem

	gameId string
	name   string

	ManagerClient *message.ManagerServiceClient
	PlayerClient  *message.PlayerServiceClient
	ManagerAddr   *goactor.ActorAddr

	Players      []*goactor.ActorAddr
	Watchers     []*goactor.ActorAddr
	PlayerNames  []string
	WatcherNames []string

	numMoves int32
	board    []int32
	moves    []int32
	current  int32
}

func (g *Game) Init(name string, gameId string, player *goactor.ActorAddr, manager *goactor.ActorAddr) {
	g.name = name
	g.gameId = gameId
	g.ManagerAddr = manager
	g.join(player)
}

func (g *Game) OnInit(actor goactor.Actor) {
	g.actor = actor
	g.ManagerClient = message.NewManagerServiceClient(g.actor.GetProto(1))
	g.PlayerClient = message.NewPlayerServiceClient(g.actor.GetProto(2))

	actor.Logger().Infof("game start %#v", g.gameId)
}

func (g *Game) OnRelease(actor goactor.Actor) {
	g.ManagerClient.FreeGame(g.actor.Context(), g.System, g.actor, g.ManagerAddr, &message.ManagerFreeGameRequest{
		GameId: g.gameId,
	}, nil)

	for _, player := range append(g.Players, g.Watchers...) {
		g.PlayerClient.OnLeaveGame(g.actor.Context(), g.System, g.actor, player, &message.PlayerOnLeaveGameRequest{
			GameId: g.gameId,
		}, nil)
	}
	g.Players = nil
	g.Watchers = nil

	actor.Logger().Infof("game release %#v", g.gameId)
}

func (g *Game) join(player *goactor.ActorAddr) {
	defer func() {
		g.checkState()
	}()

	for _, p := range append(g.Players, g.Watchers...) {
		if p.Handle == player.Handle {
			return
		}
	}

	rsp, err := g.PlayerClient.OnJoinGame(g.actor.Context(), g.System, g.actor, player, &message.PlayerOnJoinGameRequest{
		GameId: g.gameId,
		Addr:   AddrToProto(g.actor.GetAddr(g.System)),
	}, nil)
	if err != nil {
		return
	}

	if len(g.Players) >= 2 {
		g.Watchers = append(g.Watchers, player)
		g.WatcherNames = append(g.WatcherNames, rsp.Name)
		return
	}

	g.Players = append(g.Players, player)
	g.PlayerNames = append(g.PlayerNames, rsp.Name)

	g.actor.Logger().Infof("game join %d", player.Handle)
}

func (g *Game) leave(player *goactor.ActorAddr) {
	defer func() {
		g.checkState()
	}()

	g.actor.Logger().Infof("game leave %d", player.Handle)

	_, err := g.PlayerClient.OnLeaveGame(g.actor.Context(), g.System, g.actor, player, &message.PlayerOnLeaveGameRequest{
		GameId: g.gameId,
	}, nil)
	if err != nil {
		g.actor.Logger().Errorf("leave player error %s", err)
	}

	for i, watcher := range g.Watchers {
		if watcher.Handle == player.Handle {
			g.Watchers[i] = g.Watchers[len(g.Watchers)-1]
			g.Watchers = g.Watchers[:len(g.Watchers)-1]
			g.WatcherNames[i] = g.WatcherNames[len(g.WatcherNames)-1]
			g.WatcherNames = g.WatcherNames[:len(g.WatcherNames)-1]
			break
		}
	}
	for i, p := range g.Players {
		if p.Handle == player.Handle {
			g.Players[i] = g.Players[len(g.Players)-1]
			g.Players = g.Players[:len(g.Players)-1]
			g.PlayerNames[i] = g.PlayerNames[len(g.PlayerNames)-1]
			g.PlayerNames = g.PlayerNames[:len(g.PlayerNames)-1]
			break
		}
	}
}

func (g *Game) checkState() {
	if len(g.Players) == 0 {
		g.actor.Kill()
	}
	if len(g.Players) < 2 {
		g.resetBoard()
	}
}

func (g *Game) resetBoard() {
	g.numMoves = 0
	g.board = make([]int32, 9)
	g.current = 0
	g.moves = nil
}

func (g *Game) getPlayerIndex(player *goactor.ActorAddr) int {
	for i, addr := range g.Players {
		if addr.Handle == player.Handle {
			return i
		}
	}
	return -1
}

func (g *Game) playerIsMove(player *goactor.ActorAddr) bool {
	if !g.isStart() {
		return false
	}

	index := g.getPlayerIndex(player)
	if index == int(g.current) {
		return true
	}
	return false
}

func (g *Game) playerIsStart(player *goactor.ActorAddr) bool {
	if !g.isStart() {
		return false
	}

	index := g.getPlayerIndex(player)
	if index == int(0) {
		return true
	}
	return false
}

func (g *Game) isStart() bool {
	return len(g.Players) > 1
}

func (g *Game) getXY(key int32) (int32, int32) {
	return key / 100, key % 100
}

func (g *Game) buildXY(x int32, y int32) int32 {
	return x*100 + y
}

func (g *Game) getBoardNum(player *goactor.ActorAddr) int32 {
	if g.playerIsStart(player) {
		return 1
	}
	return 2
}

func (g *Game) getBoardIndex(x int32, y int32) int {
	if x < 0 || x > 2 {
		return -1
	}
	if y < 0 || y > 2 {
		return -1
	}
	return int(x*3 + y)
}

func (g *Game) getBoardValue(x int32, y int32) int32 {
	index := g.getBoardIndex(x, y)
	if index < 0 {
		return -1
	}
	return g.board[index]
}

func (g *Game) checkBoard(i int32, j int32, v int32) bool {
	checkPos := func(pos []int32, v int32) bool {
		for i := 0; i < len(pos); i += 6 {
			a1 := g.getBoardValue(pos[i+0], pos[i+1])
			a2 := g.getBoardValue(pos[i+2], pos[i+3])
			a3 := g.getBoardValue(pos[i+4], pos[i+5])
			if a1 == a2 && a2 == a3 && a3 == v {
				return true
			}
		}
		return false
	}
	for x := int32(0); x <= 2; x++ {
		for y := int32(0); y <= 2; y++ {
			if checkPos([]int32{
				x, y - 1, x, y, x, y + 1,
				x - 1, y, x, y, x + 1, y,
				x - 1, y - 1, x, y, x + 1, y + 1,
				x - 1, y + 1, x, y, x + 1, y - 1,
			}, v) {
				return true
			}
		}
	}
	return false
}

func (g *Game) buildInfo(player *goactor.ActorAddr) *message.GameInfo {
	info := &message.GameInfo{
		GameId:      g.gameId,
		Name:        g.name,
		State:       0,
		YourMove:    g.playerIsMove(player),
		GameStarter: g.playerIsStart(player),
		UserNames:   g.PlayerNames,
		NumMoves:    g.numMoves,
	}

	if g.isStart() {
		info.State = 1
	}

	return info
}

func (g *Game) OnJoin(ctx *goactor.DispatchMessage, req *message.GameJoinRequest) (*message.GameJoinResponse, error) {
	g.join(ProtoToAddr(req.Player))
	return &message.GameJoinResponse{}, nil
}

func (g *Game) OnLeave(ctx *goactor.DispatchMessage, req *message.GameLeaveRequest) (*message.GameLeaveResponse, error) {
	g.leave(ProtoToAddr(req.Player))
	return &message.GameLeaveResponse{}, nil
}

func (g *Game) OnInfo(ctx *goactor.DispatchMessage, req *message.GameInfoRequest) (*message.GameInfoResponse, error) {
	return &message.GameInfoResponse{
		Game: g.buildInfo(ProtoToAddr(req.Player)),
	}, nil
}

func (g *Game) OnGetMoves(ctx *goactor.DispatchMessage, req *message.GameGetMovesRequest) (*message.GameGetMovesResponse, error) {
	info := &message.GameMoves{
		Summary: g.buildInfo(ProtoToAddr(req.Player)),
	}
	for _, move := range g.moves {
		x, y := g.getXY(move)
		info.Moves = append(info.Moves, &message.MoveStep{X: x, Y: y})
	}

	return &message.GameGetMovesResponse{Info: info}, nil
}

func (g *Game) OnMakeMove(ctx *goactor.DispatchMessage, req *message.GameMakeMoveRequest) (*message.GameMakeMoveResponse, error) {
	player := ProtoToAddr(req.Player)
	if !g.playerIsMove(player) {
		return nil, fmt.Errorf("not move")
	}
	if g.getBoardIndex(req.X, req.Y) < 0 {
		return nil, fmt.Errorf("not move")
	}
	if g.getBoardValue(req.X, req.Y) != 0 {
		return nil, fmt.Errorf("not move")
	}

	g.numMoves++
	g.current = (g.current + 1) % int32(len(g.Players))
	g.moves = append(g.moves, g.buildXY(req.X, req.Y))
	g.board[req.X*3+req.Y] = g.getBoardNum(player)

	if g.checkBoard(req.X, req.Y, g.getBoardNum(player)) {
		g.resetBoard()
	}

	return &message.GameMakeMoveResponse{}, nil
}
