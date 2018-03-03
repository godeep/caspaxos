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

	returnedValue, err := ps.proposer.Propose(r.Context(), key, read)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if returnedValue == nil {
		http.Error(w, fmt.Sprintf("key %s doesn't exist", key), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%s\n", prettyPrint(returnedValue))
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

	var current []byte
	if s := r.URL.Query().Get("current"); s != "" {
		current = []byte(s)
	}

	var next []byte
	nextStr := r.URL.Query().Get("next")
	if nextStr == "" {
		http.Error(w, "no next value specified", http.StatusBadRequest)
		return
	}
	next = []byte(nextStr)

	returnedValue, err := ps.proposer.Propose(r.Context(), key, cas(current, next))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if bytes.Compare(returnedValue, next) != 0 { // CAS failure
		http.Error(w, fmt.Sprintf("CAS(%s, %s, %s) failed: returned value %s", key, prettyPrint(current), prettyPrint(next), prettyPrint(returnedValue)), http.StatusPreconditionFailed)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "CAS(%s, %s, %s) success: new value %s\n", key, prettyPrint(current), prettyPrint(next), prettyPrint(returnedValue))
}

func (ps ProposerServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

type prettyPrint []byte

func (pp prettyPrint) String() string {
	if pp == nil {
		return "Ø"
	}
	return string(pp)
}
