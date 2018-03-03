// Package httpapi implements a simple API for operating on string values.
package httpapi

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"

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
		return current
	}
}

// ProposerServer wraps a caspaxos.Proposer and provides a basic HTTP API.
// Note that this is an artificially restricted API. The protocol itself
// is much more expressive, this is mostly meant as a tech demo.
//
//     GET /:key
//         Returns the current value for key as a string.
//         Returns 404 Not Found if the key doesn't exist.
//
//     POST /:key/:current/:new
//         Performs a compare-and-swap on the key's value.
//         Returns 412 Precondition Failed on CAS error.
//
//     DELETE /:key
//         Deletes the key.
//         Returns 404 Not Found if the key doesn't exist.
//
type ProposerServer struct {
	Proposer caspaxos.Proposer
}

func (ps ProposerServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		ps.handleGet(w, r)
	case "POST":
		ps.handlePost(w, r)
	case "DELETE":
		ps.handleDelete(w, r)
	default:
		http.Error(w, fmt.Sprintf("invalid method %s", r.Method), http.StatusBadRequest)
	}
}

func (ps ProposerServer) handleGet(w http.ResponseWriter, r *http.Request) {
	key := strings.Trim(r.URL.Path, "/")
	if key == "" {
		http.Error(w, "no key specified", http.StatusBadRequest)
		return
	}

	val, err := ps.Proposer.Propose(r.Context(), key, read)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if val == nil {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%s", val)
}

func (ps ProposerServer) handlePost(w http.ResponseWriter, r *http.Request) {
	tokens := strings.SplitN(strings.Trim(r.URL.Path, "/"), "/", 3)
	if len(tokens) < 2 {
		http.Error(w, "invalid path: need /:key/:current/:new", http.StatusBadRequest)
		return
	}

	key := tokens[0]
	if key == "" {
		http.Error(w, "no key specified", http.StatusBadRequest)
		return
	}

	var currentBytes []byte
	if current := tokens[1]; current != "" {
		currentBytes = []byte(current)
	}

	var nextBytes []byte
	if next := tokens[2]; next != "" {
		nextBytes = []byte(next)
	}

	val, err := ps.Proposer.Propose(r.Context(), key, cas(currentBytes, nextBytes))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if bytes.Compare(val, nextBytes) != 0 { // CAS failure
		http.Error(w, http.StatusText(http.StatusPreconditionFailed), http.StatusPreconditionFailed)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%s", val)
}

func (ps ProposerServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
