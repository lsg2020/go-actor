package controller

import (
	"net/http"

	"github.com/gorilla/mux"
	message "github.com/lsg2020/go-actor/examples/TicTacToe/pb"
)

func NewHomeController() http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/Join/{gameId}", homeJoin)
	return r
}

func homeJoin(w http.ResponseWriter, r *http.Request) {
	exec(r.Context(), func() {
		player := getPlayerAddr(w, r)

		_, err := playerClient.Join(r.Context(), system, client, player, &message.PlayerJoinRequest{
			GameId: mux.Vars(r)["gameId"],
		}, nil)
		if err != nil {
			responseJson(w, err, nil)
			return
		}

		http.Redirect(w, r, "/", 302)
	})
}
