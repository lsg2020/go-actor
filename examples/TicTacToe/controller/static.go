package controller

import (
	"embed"
	"fmt"
	"io/ioutil"
	"net/http"
)

type StaticFileRouter struct {
	FS          *embed.FS
	Path        string
	StripPrefix string
}

func (rt StaticFileRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	staticAssest := func(path string) {
		r, err := rt.FS.Open(path)
		if err != nil {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			fmt.Fprint(w, fmt.Sprintf("static file %s not exists", path))
			return
		}
		defer r.Close()
		contents, err := ioutil.ReadAll(r)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, string(contents))
	}

	if rt.Path != "" {
		staticAssest(rt.Path)
		return
	}
	if rt.StripPrefix != "" {
		staticAssest(r.URL.Path[len(rt.StripPrefix):])
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, fmt.Sprintf("static file %s not exists", r.URL.Path))
	return
}
