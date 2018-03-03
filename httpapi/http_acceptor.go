package httpapi

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

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
	tokens := strings.SplitN(strings.Trim(r.URL.Path, "/"), "/", 2)
	if len(tokens) != 2 {
		http.Error(w, "invalid path: expect /{prepare,accept}/:key[/:value]", http.StatusBadRequest)
		return
	}

	println("### AcceptorServer ServeHTTP path", tokens[0])
	switch strings.ToLower(tokens[0]) {
	case "prepare":
		as.handlePrepare(w, r)
	case "accept":
		as.handleAccept(w, r)
	default:
		http.Error(w, "invalid path: expect /{prepare,accept}/:key[/:value]", http.StatusBadRequest)
	}
}

func (as AcceptorServer) handlePrepare(w http.ResponseWriter, r *http.Request) {
	tokens := strings.SplitN(r.URL.Path, "/", 2)
	if len(tokens) != 2 {
		http.Error(w, "invalid path: need /prepare/:key", http.StatusBadRequest)
		return
	}

	key := tokens[1]
	if key == "" {
		http.Error(w, "no key specified", http.StatusBadRequest)
		return
	}

	b, err := header2ballot(r.Header)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	println("### AcceptorServer handlePrepare Prepare key", key, "b", b.String())
	val, current, err := as.Acceptor.Prepare(r.Context(), key, b)
	ballot2header(current, w.Header())
	if err != nil {
		println("### AcceptorServer handlePrepare Prepare key", key, "b", b.String(), "err", err.Error())
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	println("### AcceptorServer handlePrepare Prepare OK")
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%s", val)
}

func (as AcceptorServer) handleAccept(w http.ResponseWriter, r *http.Request) {
	tokens := strings.SplitN(strings.Trim(r.URL.Path, "/"), "/", 3)
	if len(tokens) < 2 {
		println("### AcceptorServer handleAccept error-out at path check, URL", r.URL.String())
		http.Error(w, "invalid path: need /accept/:key/:value", http.StatusBadRequest)
		return
	}

	key := tokens[1]
	if key == "" {
		println("### AcceptorServer handleAccept error-out at key check, URL", r.URL.String())
		http.Error(w, "no key specified", http.StatusBadRequest)
		return
	}

	var valueBytes []byte
	if len(tokens) > 2 && tokens[2] != "" {
		valueBytes = []byte(tokens[2])
	}

	b, err := header2ballot(r.Header)
	if err != nil {
		println("### AcceptorServer handleAccept error-out at header check")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	println("### AcceptorServer handleAccept Accept key", key, "b", b.String(), "value", string(valueBytes))
	if err = as.Acceptor.Accept(r.Context(), key, b, valueBytes); err != nil {
		println("### AcceptorServer handleAccept Accept key", key, "b", b.String(), "value", string(valueBytes), "err", err.Error())
		http.Error(w, err.Error(), http.StatusNotAcceptable)
		return
	}

	println("### AcceptorServer handleAccept Accept OK")
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
	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return errors.Wrap(err, "constructing HTTP request")
	}

	ballot2header(b, req.Header)
	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "executing HTTP request")
	}

	if resp.StatusCode != http.StatusOK {
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
