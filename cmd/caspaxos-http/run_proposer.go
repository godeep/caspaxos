package main

import (
	"context"
	"flag"
	"fmt"
	"hash/crc32"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/peterbourgon/caspaxos"
	"github.com/peterbourgon/caspaxos/cluster"
	"github.com/peterbourgon/caspaxos/httpapi"
)

func runProposer(args []string) error {
	flagset := flag.NewFlagSet("proposer", flag.ExitOnError)
	var (
		debug                = flagset.Bool("debug", false, "log debug information")
		apiAddr              = flagset.String("api", defaultAPIAddr, "listen address for HTTP API")
		clusterBindAddr      = flagset.String("cluster", defaultClusterAddr, "listen address for cluster comms")
		clusterAdvertiseAddr = flagset.String("cluster.advertise-addr", "", "optional, explicit address to advertise in cluster")
		clusterPeers         = stringslice{}
	)
	flagset.Var(&clusterPeers, "peer", "cluster peer host:port (repeatable)")
	flagset.Usage = usageFor(flagset, "caspaxos-http proposer [flags]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	// Build a logger.
	var logger log.Logger
	{
		logger = log.NewLogfmtLogger(os.Stderr)
		lv := level.AllowInfo()
		if *debug {
			lv = level.AllowDebug()
		}
		logger = level.NewFilter(logger, lv)
	}

	// Parse API addresses.
	var apiNetwork string
	var apiHost string
	var apiPort int
	{
		var err error
		apiNetwork, _, apiHost, apiPort, err = parseAddr(*apiAddr, defaultAPIPort)
		if err != nil {
			return err
		}
	}

	// Parse cluster comms addresses.
	var clusterBindHost string
	var clusterBindPort int
	var clusterAdvertiseHost string
	var clusterAdvertisePort int
	{
		var err error
		_, _, clusterBindHost, clusterBindPort, err = parseAddr(*clusterBindAddr, defaultClusterPort)
		if err != nil {
			return err
		}
		level.Info(logger).Log("cluster_bind", fmt.Sprintf("%s:%d", clusterBindHost, clusterBindPort))

		if *clusterAdvertiseAddr != "" {
			_, _, clusterAdvertiseHost, clusterAdvertisePort, err = parseAddr(*clusterAdvertiseAddr, defaultClusterPort)
			if err != nil {
				return err
			}
			level.Info(logger).Log("cluster_advertise", fmt.Sprintf("%s:%d", clusterAdvertiseHost, clusterAdvertisePort))
		}

		advertiseIP, err := cluster.CalculateAdvertiseIP(clusterBindHost, clusterAdvertiseHost, net.DefaultResolver, logger)
		if err != nil {
			level.Error(logger).Log("err", "couldn't deduce an advertise IP: "+err.Error())
			return err
		}

		if hasNonlocal(clusterPeers) && isUnroutable(advertiseIP.String()) {
			level.Warn(logger).Log("err", "this node advertises itself on an unroutable IP", "ip", advertiseIP.String())
			level.Warn(logger).Log("err", "this node will be unreachable in the cluster")
			level.Warn(logger).Log("err", "provide -cluster.advertise-addr as a routable IP address or hostname")
		}
		level.Info(logger).Log("user_bind_host", clusterBindHost, "user_advertise_host", clusterAdvertiseHost, "calculated_advertise_ip", advertiseIP)
		clusterAdvertiseHost = advertiseIP.String()
		if clusterAdvertisePort == 0 {
			clusterAdvertisePort = clusterBindPort
		}
	}

	// Construct a peer.
	var peer *cluster.Peer
	{
		var err error
		peer, err = cluster.NewPeer(
			clusterBindHost, clusterBindPort,
			clusterAdvertiseHost, clusterAdvertisePort,
			clusterPeers,
			cluster.NodeTypeProposer, apiPort,
			log.With(logger, "component", "cluster"),
		)
		if err != nil {
			return err
		}
		defer func() {
			if err := peer.Leave(time.Second); err != nil {
				level.Warn(logger).Log("op", "peer.Leave", "err", err)
			}
		}()
	}

	// Lol.
	{
		level.Info(logger).Log("msg", "waiting 1s for acceptors to appear")
		time.Sleep(1 * time.Second)
		level.Info(logger).Log("acceptors", fmt.Sprintf("%v", peer.Current(cluster.NodeTypeAcceptor)))
	}

	// Use the peer to get an initial set of acceptors.
	var initialAcceptors []caspaxos.Acceptor
	{
		for _, hostport := range peer.Current(cluster.NodeTypeAcceptor) {
			initialAcceptors = append(initialAcceptors, httpapi.AcceptorClient{
				URL: &url.URL{Scheme: "http", Host: hostport},
			})
		}
		level.Debug(logger).Log("initial_acceptors", len(initialAcceptors))
	}

	// Generate a unique ID for this proposer.
	var id uint64
	{
		h := crc32.NewIEEE()
		fmt.Fprint(h, clusterAdvertiseHost)
		fmt.Fprint(h, clusterAdvertisePort)
		id = uint64(h.Sum32())
		level.Debug(logger).Log("proposer_id", id)
	}

	// Construct the proposer.
	var proposer caspaxos.Proposer
	{
		proposer = caspaxos.NewLocalProposer(
			id,
			log.With(logger, "component", "proposer"),
			initialAcceptors...,
		)
		// TODO(pb): wire up configuration changes
	}

	// Set up the API listener and server.
	var apiListener net.Listener
	{
		var err error
		apiListener, err = net.Listen(apiNetwork, net.JoinHostPort(apiHost, strconv.Itoa(apiPort)))
		if err != nil {
			return err
		}
		defer func() {
			if err := apiListener.Close(); err != nil {
				level.Warn(logger).Log("op", "apiListener.Close", "err", err)
			}
		}()
	}

	var g run.Group
	{
		// Serve the HTTP API.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		server := &http.Server{
			Handler: httpapi.NewProposerServer(proposer, log.With(logger, "component", "api")),
		}
		level.Info(logger).Log("component", "api", "addr", apiListener.Addr().String())
		g.Add(func() error {
			return server.Serve(apiListener)
		}, func(error) {
			server.Shutdown(ctx)
		})
	}
	{
		// Listen for ctrl-C.
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			c := make(chan os.Signal, 1)
			signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
			select {
			case sig := <-c:
				return fmt.Errorf("received signal %s", sig)
			case <-ctx.Done():
				return ctx.Err()
			}
		}, func(error) {
			cancel()
		})
	}
	return g.Run()
}
