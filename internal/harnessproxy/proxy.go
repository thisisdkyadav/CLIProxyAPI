package harnessproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	log "github.com/sirupsen/logrus"
)

// Server manages the optional per-harness listeners.
type Server struct {
	mu      sync.Mutex
	servers []*http.Server
}

// Start launches one reverse proxy for every configured harness profile.
func Start(cfg *config.Config) (*Server, error) {
	if cfg == nil || len(cfg.Harnesses) == 0 {
		return &Server{}, nil
	}
	mainHost := cfg.Host
	if strings.TrimSpace(mainHost) == "" || mainHost == "0.0.0.0" || mainHost == "::" {
		mainHost = "127.0.0.1"
	}
	target, err := url.Parse(fmt.Sprintf("http://%s:%d", mainHost, cfg.Port))
	if err != nil {
		return nil, fmt.Errorf("parse main proxy address: %w", err)
	}

	result := &Server{}
	for _, profile := range cfg.Harnesses {
		if profile.Port <= 0 || profile.Port == cfg.Port {
			return nil, fmt.Errorf("harness %q has invalid or conflicting port %d", profile.Name, profile.Port)
		}
		host := profile.Host
		if strings.TrimSpace(host) == "" {
			host = "127.0.0.1"
		}
		handler := newHandler(target, profile)
		srv := &http.Server{Addr: fmt.Sprintf("%s:%d", host, profile.Port), Handler: handler}
		result.mu.Lock()
		result.servers = append(result.servers, srv)
		result.mu.Unlock()
		go func(s *http.Server, name string) {
			log.Infof("harness listener %q started on %s", name, s.Addr)
			if errServe := s.ListenAndServe(); errServe != nil && errServe != http.ErrServerClosed {
				log.Errorf("harness listener %q stopped: %v", name, errServe)
			}
		}(srv, profile.Name)
	}
	return result, nil
}

func newHandler(target *url.URL, profile config.HarnessConfig) http.Handler {
	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director
	proxy.Director = func(r *http.Request) {
		originalDirector(r)
		r.Header.Set("X-CLIProxy-Harness", profile.Name)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if profile.DelayMs > 0 {
			time.Sleep(time.Duration(profile.DelayMs) * time.Millisecond)
		}
		if len(profile.Models) > 0 && r.Method == http.MethodGet && r.URL.Path == "/v1/models" {
			writeModels(w, r, profile.Models)
			return
		}
		proxy.ServeHTTP(w, r)
	})
}

func writeModels(w http.ResponseWriter, r *http.Request, models []string) {
	// Preserve the OpenAI list shape while keeping model visibility scoped to
	// this harness. The actual provider routing still happens on the main API.
	type model struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}
	items := make([]model, 0, len(models))
	for _, id := range models {
		if strings.TrimSpace(id) == "" {
			continue
		}
		items = append(items, model{ID: id, Object: "model", Created: time.Now().Unix(), OwnedBy: "cliproxyapi"})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": items})
	_ = r
}

// Stop shuts down all harness listeners.
func (s *Server) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	servers := append([]*http.Server(nil), s.servers...)
	s.mu.Unlock()
	for _, srv := range servers {
		if err := srv.Shutdown(ctx); err != nil && err != io.EOF {
			return err
		}
	}
	return nil
}

// ParseDelay is shared by management clients validating delay values.
func ParseDelay(value string) (int, error) {
	ms, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || ms < 0 {
		return 0, fmt.Errorf("delay must be a non-negative integer in milliseconds")
	}
	return ms, nil
}
