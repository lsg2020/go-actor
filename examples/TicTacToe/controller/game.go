package controller

import (
	"context"
	"encoding/base32"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	goactor "github.com/lsg2020/go-actor"
	"github.com/lsg2020/go-actor/examples/TicTacToe/game"
	message "github.com/lsg2020/go-actor/examples/TicTacToe/pb"
	"github.com/lsg2020/go-actor/examples/TicTacToe/tracing"
	"github.com/lsg2020/go-actor/executer"
	"github.com/lsg2020/go-actor/protocols"
)

var system *goactor.ActorSystem
var client goactor.Actor
var playerClient *message.PlayerServiceClient
var single *executer.SingleGoroutine

type clientActor struct {
}

func (c *clientActor) OnInit(actor goactor.Actor) {
}

func (c *clientActor) OnRelease(actor goactor.Actor) {
}

func NewGameController(s *goactor.ActorSystem) http.Handler {
	system = s

	single = &executer.SingleGoroutine{}
	single.Start(context.Background(), 1)

	// TODO
	p := &clientActor{}
	proto := protocols.NewProtobuf(2, tracing.InterceptorCall(), tracing.InterceptorDispatch())
	message.RegisterPlayerService(nil, proto)
	client = goactor.NewActor(p, single, goactor.ActorWithProto(proto))
	playerClient = message.NewPlayerServiceClient(proto)

	r := mux.NewRouter()
	r.HandleFunc("/Index/", gameIndex)
	r.HandleFunc("/SetUser/{name}", gameSetUser)
	r.HandleFunc("/CreateGame/", gameCreateGame)
	r.HandleFunc("/GetMoves/{gameId}", gameGetMoves)
	r.Path("/MakeMove/{gameId}/").Queries("x", "{x}").Queries("y", "{y}").HandlerFunc(gameMakeMove)
	r.HandleFunc("/Join/{gameId}", gameJoin)

	return r
}

func getPlayerAddr(w http.ResponseWriter, r *http.Request) *goactor.ActorAddr {
	setCookie := func(str string) {
		cookie := http.Cookie{}
		cookie.Name = "playerAddr"
		cookie.Value = str
		cookie.Path = "/"
		//cookie.MaxAge =  3600
		cookie.Expires = time.Now().Add(time.Second * 10)
		cookie.Secure = true
		cookie.HttpOnly = true
		cookie.SameSite = http.SameSiteStrictMode
		http.SetCookie(w, &cookie)
	}
	newPlayer := func() *goactor.ActorAddr {
		addr := game.NewPlayer(system, single)
		game.ExistsPlayer[uint64(addr.Handle)] = true

		buf, _ := json.Marshal(addr)
		setCookie(base32.StdEncoding.EncodeToString(buf))
		return addr
	}
	player, err := r.Cookie("playerAddr")
	if err != nil {
		return newPlayer()
	}

	str, _ := base32.StdEncoding.DecodeString(player.Value)
	addr := &goactor.ActorAddr{}
	err = json.Unmarshal(str, addr)

	if err != nil || !game.ExistsPlayer[uint64(addr.Handle)] {
		return newPlayer()
	}
	setCookie(player.Value)
	return addr
}

func responseJson(w http.ResponseWriter, err error, data proto.Message) {
	if err != nil {
		w.WriteHeader(201)
		w.Write([]byte(err.Error()))
		return
	}

	buf, _ := (&jsonpb.Marshaler{
		EmitDefaults: true,
	}).MarshalToString(data)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(buf))
	return
}

func exec(ctx context.Context, f func()) {
	finish := make(chan struct{}, 1)
	client.Fork(func() {
		f()
		finish <- struct{}{}
	})

	select {
	case <-finish:
	case <-ctx.Done():
	}
}

func gameIndex(w http.ResponseWriter, r *http.Request) {
	exec(r.Context(), func() {
		player := getPlayerAddr(w, r)

		rsp, err := playerClient.Index(r.Context(), system, client, player, &message.PlayerIndexRequest{}, nil)
		if err != nil {
			responseJson(w, err, nil)
			return
		}

		responseJson(w, nil, rsp)
	})
}

func gameSetUser(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	exec(r.Context(), func() {
		player := getPlayerAddr(w, r)

		_, err := playerClient.SetName(r.Context(), system, client, player, &message.PlayerSetNameRequest{Name: name}, nil)
		if err != nil {
			responseJson(w, err, nil)
			return
		}

		responseJson(w, nil, nil)
	})
}

func gameCreateGame(w http.ResponseWriter, r *http.Request) {
	exec(r.Context(), func() {
		player := getPlayerAddr(w, r)

		_, err := playerClient.CreateGame(r.Context(), system, client, player, &message.PlayerCreateGameRequest{}, nil)
		if err != nil {
			responseJson(w, err, nil)
			return
		}

		responseJson(w, nil, nil)
	})
}

func gameGetMoves(w http.ResponseWriter, r *http.Request) {
	exec(r.Context(), func() {
		player := getPlayerAddr(w, r)

		rsp, err := playerClient.GetMoves(r.Context(), system, client, player, &message.PlayerGetMovesRequest{GameId: mux.Vars(r)["gameId"]}, nil)
		if err != nil {
			responseJson(w, err, nil)
			return
		}

		responseJson(w, nil, rsp.Moves)
	})
}

func gameMakeMove(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	_ = vars
	x, _ := strconv.ParseInt(r.FormValue("x"), 10, 32)
	y, _ := strconv.ParseInt(r.FormValue("y"), 10, 32)

	exec(r.Context(), func() {
		player := getPlayerAddr(w, r)

		rsp, err := playerClient.MakeMove(r.Context(), system, client, player, &message.PlayerMakeMoveRequest{
			GameId: mux.Vars(r)["gameId"],
			X:      int32(x),
			Y:      int32(y),
		}, nil)
		if err != nil {
			responseJson(w, err, nil)
			return
		}

		responseJson(w, nil, rsp)
	})
}

func gameJoin(w http.ResponseWriter, r *http.Request) {
	exec(r.Context(), func() {
		player := getPlayerAddr(w, r)

		rsp, err := playerClient.Join(r.Context(), system, client, player, &message.PlayerJoinRequest{
			GameId: mux.Vars(r)["gameId"],
		}, nil)
		if err != nil {
			responseJson(w, err, nil)
			return
		}

		responseJson(w, nil, rsp)
	})
}
