package httpapi

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/peterbourgon/caspaxos"
	"github.com/pkg/errors"
)

// AcceptorServer wraps a caspaxos.Acceptor and provides a basic HTTP API.
//
//     POST /prepare/:key
//         Prepare request for the given key.
//         Expects and returns "X-Caspaxos-Ballot: Counter/ID" header.
//         Returns 412 Precondition Failed on protocol error.
//
//     POST /accept/:key/:value
//         Accept request for the given key and value.
//         Expects "X-Caspaxos-Ballot: Counter/ID" header.
//         Returns 406 Not Acceptable on protocol error.
//
type AcceptorServer struct {
	Acceptor caspaxos.Acceptor
}

func (as AcceptorServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	println("### AcceptorServer ServeHTTP", r.Method, r.URL.String())
	router := mux.NewRouter().StrictSlash(true).Methods("POST").Subrouter()
	router.HandleFunc("/prepare/{key}", as.handlePrepare)
	router.HandleFunc("/accept/{key}/{value}", as.handleAccept)
	router.HandleFunc("/accept/{key}", as.handleAccept) // no value is OK
	router.ServeHTTP(w, r)
}

func (as AcceptorServer) handlePrepare(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]
	if key == "" {
		http.Error(w, "no key specified", http.StatusBadRequest)
		return
	}

	b, err := header2ballot(r.Header)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	val, current, err := as.Acceptor.Prepare(r.Context(), key, b)
	ballot2header(current, w.Header())
	if err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%s", val)
}

func (as AcceptorServer) handleAccept(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	if key == "" {
		http.Error(w, "no key specified", http.StatusBadRequest)
		return
	}

	var valueBytes []byte
	if value := vars["value"]; value != "" {
		valueBytes = []byte(value)
	}

	b, err := header2ballot(r.Header)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err = as.Acceptor.Accept(r.Context(), key, b, valueBytes); err != nil {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "OK")
}

// AcceptorClient implements caspaxos.Acceptor by making HTTP requests to a
// remote acceptor.
type AcceptorClient struct {
	// URL of the remote acceptor HTTP API. Required.
	URL *url.URL

	// HTTPClient used to make remote HTTP requests. Optional.
	// By default, http.DefaultClient is used.
	HTTPClient interface {
		Do(*http.Request) (*http.Response, error)
	}
}

// Address implements caspaxos.Acceptor, returning the wrapped URL.
func (ac AcceptorClient) Address() string {
	return ac.URL.String()
}

// Prepare implements caspaxos.Acceptor by making an HTTP request to the remote
// acceptor API.
func (ac AcceptorClient) Prepare(ctx context.Context, key string, b caspaxos.Ballot) (value []byte, current caspaxos.Ballot, err error) {
	client := ac.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	u := *ac.URL
	u.Path = fmt.Sprintf("/prepare/%s", url.PathEscape(key))
	println("### AcceptorClient Prepare POST to", u.String())
	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, caspaxos.Ballot{}, errors.Wrap(err, "constructing HTTP request")
	}

	ballot2header(b, req.Header)
	println("### AcceptorClient Prepare ballot2header", b.String(), "->", req.Header.Get(ballotHeaderKey))
	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if err != nil {
		return nil, caspaxos.Ballot{}, errors.Wrap(err, "executing HTTP request")
	}

	current, err = header2ballot(resp.Header)
	if err != nil {
		return nil, caspaxos.Ballot{}, errors.Wrap(err, "extracting response ballot")
	}

	if resp.StatusCode != http.StatusOK {
		buf, _ := ioutil.ReadAll(resp.Body)
		return nil, current, fmt.Errorf("%s (%s)", resp.Status, strings.TrimSpace(string(buf)))
	}

	value, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, current, errors.Wrap(err, "consuming response value")
	}

	return value, current, nil
}

// Accept implements caspaxos.Acceptor by making an HTTP request to the remote
// acceptor API.
func (ac AcceptorClient) Accept(ctx context.Context, key string, b caspaxos.Ballot, value []byte) error {
	client := ac.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	u := *ac.URL
	u.Path = fmt.Sprintf("/accept/%s/%s", url.PathEscape(key), url.PathEscape(string(value)))
	println("### AcceptorClient Accept POST", u.String())
	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		println("### AcceptorClient Accept POST", u.String(), "NewRequest error", err.Error())
		return errors.Wrap(err, "constructing HTTP request")
	}

	ballot2header(b, req.Header)
	println("### AcceptorClient Accept", req.Method, req.URL.String(), ballotHeaderKey, req.Header.Get(ballotHeaderKey))
	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if err != nil {
		println("### AcceptorClient Accept", req.Method, req.URL.String(), "Do error", err.Error())
		return errors.Wrap(err, "executing HTTP request")
	}

	if resp.StatusCode != http.StatusOK {
		println("### AcceptorClient Accept", req.Method, req.URL.String(), "Status error", resp.Status)
		return errors.New(resp.Status)
	}

	return nil
}

const ballotHeaderKey = "X-Caspaxos-Ballot"

func header2ballot(h http.Header) (caspaxos.Ballot, error) {
	ballot := h.Get(ballotHeaderKey)
	if ballot == "" {
		return caspaxos.Ballot{}, fmt.Errorf("%s not provided", ballotHeaderKey)
	}
	tokens := strings.SplitN(ballot, "/", 2)
	if len(tokens) != 2 {
		return caspaxos.Ballot{}, fmt.Errorf("%s has invalid format", ballotHeaderKey)
	}
	counter, err := strconv.ParseUint(tokens[0], 10, 64)
	if err != nil {
		return caspaxos.Ballot{}, fmt.Errorf("%s has invalid Counter value %q", ballotHeaderKey, tokens[0])
	}
	id, err := strconv.ParseUint(tokens[1], 10, 64)
	if err != nil {
		return caspaxos.Ballot{}, fmt.Errorf("%s has invalid ID value %q", ballotHeaderKey, tokens[1])
	}
	return caspaxos.Ballot{Counter: counter, ID: id}, nil
}

func ballot2header(b caspaxos.Ballot, h http.Header) {
	h.Set(ballotHeaderKey, fmt.Sprintf("%d/%d", b.Counter, b.ID))
}
