// Package httpapi implements a simple API for operating on string values.
package httpapi

import (
	"bytes"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"

	"github.com/peterbourgon/caspaxos"
)

func read(x []byte) []byte {
	return x
}

func cas(current, next []byte) caspaxos.ChangeFunc {
	return func(x []byte) []byte {
		if bytes.Compare(current, x) == 0 {
			return next
		}
		return x
	}
}

// ProposerServer wraps a caspaxos.Proposer and provides a basic HTTP API.
// Note that this is an artificially restricted API. The protocol itself
// is much more expressive, this is mostly meant as a tech demo.
//
//     GET /{key}
//         Returns the current value for key as a string.
//         Returns 404 Not Found if the key doesn't exist.
//
//     POST /{key}?current=CURRENT&next=NEXT
//         Performs a compare-and-swap on the key's value.
//         Returns 412 Precondition Failed on CAS error.
//
//     DELETE /{key}?current=CURRENT
//         Deletes the key.
//         Returns 404 Not Found if the key doesn't exist.
//
type ProposerServer struct {
	http.Handler
	proposer caspaxos.Proposer
	logger   log.Logger
}

// NewProposerServer returns an ProposerServer wrapping the provided proposer.
// The ProposerServer is an http.Handler and can ServeHTTP.
func NewProposerServer(proposer caspaxos.Proposer, logger log.Logger) ProposerServer {
	ps := ProposerServer{
		proposer: proposer,
		logger:   logger,
	}
	{
		r := mux.NewRouter().StrictSlash(true)
		r.Methods("GET").Path("/{key}").HandlerFunc(ps.handleGet)
		r.Methods("POST").Path("/{key}").HandlerFunc(ps.handlePost)
		r.Methods("DELETE").Path("/{key}").HandlerFunc(ps.handleDelete)
		r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Proposer encountered unrecognized request path: "+r.URL.String(), http.StatusNotFound)
		})
		ps.Handler = r
	}
	return ps
}

func (ps ProposerServer) handleGet(w http.ResponseWriter, r *http.Request) {
	iw := &interceptingWriter{w, http.StatusOK}
	defer func(begin time.Time) {
		level.Info(ps.logger).Log(
			"handler", "handleGet",
			"method", r.Method,
			"url", r.URL.String(),
			"took", time.Since(begin),
			"status", iw.code,
		)
	}(time.Now())
	w = iw

	key := mux.Vars(r)["key"]
	if key == "" {
		http.Error(w, "no key specified", http.StatusBadRequest)
		return
	}

	val, err := ps.proposer.Propose(r.Context(), key, read)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if val == nil {
		http.Error(w, fmt.Sprintf("key %q doesn't exist", key), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%s\n", val)
}

func (ps ProposerServer) handlePost(w http.ResponseWriter, r *http.Request) {
	iw := &interceptingWriter{w, http.StatusOK}
	defer func(begin time.Time) {
		level.Info(ps.logger).Log(
			"handler", "handlePost",
			"method", r.Method,
			"url", r.URL.String(),
			"took", time.Since(begin),
			"status", iw.code,
		)
	}(time.Now())
	w = iw

	vars := mux.Vars(r)
	key := vars["key"]
	if key == "" {
		http.Error(w, "no key specified", http.StatusBadRequest)
		return
	}

	var currentBytes []byte
	if current := r.URL.Query().Get("current"); current != "" {
		currentBytes = []byte(current)
	}

	var nextBytes []byte
	next := r.URL.Query().Get("next")
	if next == "" {
		http.Error(w, "no next value specified", http.StatusBadRequest)
		return
	}
	nextBytes = []byte(next)

	val, err := ps.proposer.Propose(r.Context(), key, cas(currentBytes, nextBytes))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if bytes.Compare(val, nextBytes) != 0 { // CAS failure
		http.Error(w, fmt.Sprintf("wanted to set %q, but got back %q", string(nextBytes), string(val)), http.StatusPreconditionFailed)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "CAS(%q, %q, %q) success: new value %q\n", key, string(currentBytes), string(nextBytes), string(val))
}

func (ps ProposerServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
