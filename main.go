// Command response-keeper keeps OpenAI Responses history in front of a
// stateless upstream so clients can continue with previous_response_id and a
// delta input, over HTTP or WebSocket.
package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type config struct {
	upstream              *url.URL
	listen                string
	ttl                   time.Duration
	maxStoreBytes         int64
	maxRequestBytes       int64
	requireNoDefaultRoute bool
	// ignoreClientStoreFalse keeps turns even when the client sent store:false
	// (e.g. Codex always does), so they can be continued on a new socket.
	ignoreClientStoreFalse bool
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		if err := healthcheck(os.Getenv("LISTEN_ADDR")); err != nil {
			log.Print(err)
			os.Exit(1)
		}
		return
	}
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	if cfg.requireNoDefaultRoute {
		if err := waitForNoDefaultRoute(os.DirFS("/proc/net"), 5*time.Second); err != nil {
			log.Fatal(err)
		}
	}
	k := newKeeper(cfg)
	srv := &http.Server{Addr: cfg.listen, Handler: k, ReadHeaderTimeout: 10 * time.Second}
	listener, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on %s upstream=%s ttl=%s max_store_bytes=%d", listener.Addr(), cfg.upstream, cfg.ttl, cfg.maxStoreBytes)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func loadConfig() (config, error) {
	raw := os.Getenv("UPSTREAM_URL")
	if raw == "" {
		return config{}, errors.New("UPSTREAM_URL is required")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return config{}, errors.New("UPSTREAM_URL must be an http(s) URL without userinfo, query, or fragment")
	}
	cfg := config{upstream: u, listen: os.Getenv("LISTEN_ADDR")}
	if cfg.listen == "" {
		cfg.listen = ":8080"
	}
	if cfg.ttl, err = envDuration("STORE_TTL", 10*time.Minute); err != nil {
		return config{}, err
	}
	if cfg.maxStoreBytes, err = envPositiveInt("STORE_MAX_BYTES", 128<<20); err != nil {
		return config{}, err
	}
	if cfg.maxRequestBytes, err = envPositiveInt("MAX_REQUEST_BYTES", 32<<20); err != nil {
		return config{}, err
	}
	if v := os.Getenv("REQUIRE_NO_DEFAULT_ROUTE"); v != "" {
		if cfg.requireNoDefaultRoute, err = strconv.ParseBool(v); err != nil {
			return config{}, errors.New("REQUIRE_NO_DEFAULT_ROUTE must be true or false")
		}
	}
	if v := os.Getenv("IGNORE_CLIENT_STORE_FALSE"); v != "" {
		if cfg.ignoreClientStoreFalse, err = strconv.ParseBool(v); err != nil {
			return config{}, errors.New("IGNORE_CLIENT_STORE_FALSE must be true or false")
		}
	}
	return cfg, nil
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", name)
	}
	return d, nil
}

func envPositiveInt(name string, fallback int64) (int64, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return n, nil
}

func healthcheck(listen string) error {
	if listen == "" {
		listen = ":8080"
	}
	_, port, err := net.SplitHostPort(listen)
	if err != nil || port == "" {
		return fmt.Errorf("invalid LISTEN_ADDR %q", listen)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	client := &http.Client{Timeout: 2 * time.Second, Transport: transport}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck returned %s", resp.Status)
	}
	return nil
}

// waitForNoDefaultRoute refuses to serve until the network namespace has no
// default route, so a misconfigured deployment cannot reach the internet.
func waitForNoDefaultRoute(fsys fs.FS, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		v4, err4 := fs.ReadFile(fsys, "route")
		v6, err6 := fs.ReadFile(fsys, "ipv6_route")
		if err4 == nil && err6 == nil && !hasDefaultRoute(v4, v6) {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("default-route isolation check failed")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func hasDefaultRoute(v4, v6 []byte) bool {
	for _, line := range strings.Split(string(v4), "\n") {
		f := strings.Fields(line)
		if len(f) >= 8 && f[1] == "00000000" && f[7] == "00000000" && f[3] != "0000" {
			return true
		}
	}
	for _, line := range strings.Split(string(v6), "\n") {
		f := strings.Fields(line)
		if len(f) >= 10 && f[0] == strings.Repeat("0", 32) && f[1] == "00" && f[9] != "lo" {
			return true
		}
	}
	return false
}
