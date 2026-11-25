package main

import (
	"bufio"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

//go:embed web/index.html
var webFS embed.FS

type config struct {
	baseURL string
	model   string
	keyFile string
	apiKey  string
	addr    string
	mcpName string
	city    string
	collect time.Duration
	report  time.Duration
}

type server struct {
	cfg     config
	store   *ChatStore
	agent   *Agent
	mcp     *MCPClient
	mcpInfo map[string]any
	manager *SchedulerManager
	chatMu  sync.Mutex
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "mcp-server" {
		runWeatherMCPServer()
		return
	}
	if err := loadDotEnv(".env"); err != nil {
		log.Printf("config: %v", err)
	}

	cfg := loadConfig()
	if cfg.apiKey == "" {
		log.Printf("deepseek: ключ не задан в %s", cfg.keyFile)
	}

	store, err := NewChatStore("data")
	if err != nil {
		log.Fatalf("store: %v", err)
	}

	tools := buildTools()
	exe, err := os.Executable()
	if err != nil {
		log.Fatalf("executable: %v", err)
	}
	mcpCtx, mcpCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	mcpClient, mcpErr := NewMCPCommandClient(mcpCtx, cfg.mcpName, exe, "mcp-server")
	mcpCancel()
	if mcpErr != nil {
		log.Printf("mcp: %v", mcpErr)
	} else {
		addMCPTools(tools, mcpClient, cfg.mcpName)
		log.Printf("mcp: подключено, инструментов: %d", len(mcpClient.Tools()))
	}
	client := NewDeepSeekClient(cfg.baseURL, cfg.apiKey, cfg.model)
	manager := NewSchedulerManager(mcpClient, client, store)
	addSchedulerTools(tools, manager, "")
	agent := NewAgent(client, tools, 6)
	mcpInfo := mcpStatus(mcpClient, cfg.mcpName, mcpErr)

	s := &server{cfg: cfg, store: store, agent: agent, mcp: mcpClient, mcpInfo: mcpInfo, manager: manager}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.index)
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /api/state", s.state)
	mux.HandleFunc("POST /api/chat", s.chat)
	mux.HandleFunc("POST /api/chat/reset", s.reset)
	mux.HandleFunc("POST /api/reset-all", s.resetAll)
	mux.HandleFunc("POST /api/scheduler/collect", s.schedulerCollect)
	mux.HandleFunc("POST /api/scheduler/report", s.schedulerReport)
	mux.HandleFunc("GET /api/scheduler/status", s.schedulerStatus)

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           logRequests(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("сервер запущен: http://localhost%s (модель: %s)", cfg.addr, cfg.model)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("останавливаюсь...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	if s.mcp != nil {
		_ = s.mcp.Close()
	}
}

func addSchedulerTools(tools map[string]Tool, manager *SchedulerManager, defaultCity string) {
	tools["create_weather_scheduler"] = Tool{
		Schema: ToolSchema{Type: "function", Function: ToolFunction{
			Name:        "create_weather_scheduler",
			Description: "Создаёт и запускает планировщик сбора погоды для города. collect_every и report_every — Go duration, например 20s и 60s.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"city":          map[string]any{"type": "string", "description": "Город, например Казань"},
					"collect_every": map[string]any{"type": "string", "description": "Интервал сбора, например 20s"},
					"report_every":  map[string]any{"type": "string", "description": "Интервал отчёта, например 60s"},
				},
				"required": []string{"city"},
			},
		}},
		Run: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var args struct {
				City         string `json:"city"`
				CollectEvery string `json:"collect_every"`
				ReportEvery  string `json:"report_every"`
			}
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &args); err != nil {
					return nil, err
				}
			}
			if strings.TrimSpace(args.City) == "" {
				args.City = defaultCity
			}
			collect := parseDuration(args.CollectEvery, 20*time.Second)
			report := parseDuration(args.ReportEvery, 60*time.Second)
			scheduler, err := manager.Create(args.City, collect, report)
			if err != nil {
				return nil, err
			}
			return scheduler.Status(), nil
		},
	}
	tools["list_weather_schedulers"] = Tool{
		Schema: ToolSchema{Type: "function", Function: ToolFunction{
			Name:        "list_weather_schedulers",
			Description: "Возвращает список запущенных планировщиков погоды.",
			Parameters:  map[string]any{"type": "object", "properties": map[string]any{}},
		}},
		Run: func(ctx context.Context, _ json.RawMessage) (any, error) {
			return manager.List(), nil
		},
	}
	tools["stop_weather_scheduler"] = Tool{
		Schema: ToolSchema{Type: "function", Function: ToolFunction{
			Name:        "stop_weather_scheduler",
			Description: "Останавливает планировщик погоды для города.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"city": map[string]any{"type": "string", "description": "Город"},
				},
				"required": []string{"city"},
			},
		}},
		Run: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var args struct {
				City string `json:"city"`
			}
			if err := json.Unmarshal(raw, &args); err != nil {
				return nil, err
			}
			if err := manager.Stop(args.City); err != nil {
				return nil, err
			}
			return map[string]any{"status": "stopped", "city": args.City}, nil
		},
	}
	tools["collect_weather_now"] = Tool{
		Schema: ToolSchema{Type: "function", Function: ToolFunction{
			Name:        "collect_weather_now",
			Description: "Немедленно выполняет сбор погоды для города.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"city": map[string]any{"type": "string", "description": "Город; пусто — первый планировщик"},
				},
			},
		}},
		Run: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var args struct {
				City string `json:"city"`
			}
			if len(raw) > 0 {
				_ = json.Unmarshal(raw, &args)
			}
			scheduler := manager.Get(args.City)
			if scheduler == nil {
				return nil, fmt.Errorf("планировщик не найден")
			}
			return scheduler.CollectNow(ctx)
		},
	}
	tools["make_weather_report"] = Tool{
		Schema: ToolSchema{Type: "function", Function: ToolFunction{
			Name:        "make_weather_report",
			Description: "Формирует и сохраняет агрегированный отчёт по накопленным наблюдениям погоды.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"city": map[string]any{"type": "string", "description": "Город; пусто — первый планировщик"},
				},
			},
		}},
		Run: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var args struct {
				City string `json:"city"`
			}
			if len(raw) > 0 {
				_ = json.Unmarshal(raw, &args)
			}
			scheduler := manager.Get(args.City)
			if scheduler == nil {
				return nil, fmt.Errorf("планировщик не найден")
			}
			return scheduler.ReportNow(ctx)
		},
	}
}

func (s *server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data, err := webFS.ReadFile("web/index.html")
	if err != nil {
		http.Error(w, "index.html не найден", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(data)
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"model":      s.cfg.model,
		"configured": s.cfg.apiKey != "",
	})
}

func (s *server) state(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	writeJSON(w, http.StatusOK, s.statePayload(ctx))
}

func (s *server) statePayload(ctx context.Context) map[string]any {
	chat := s.store.Get()
	return map[string]any{
		"turns":        chat.Turns,
		"internal":     chat.Internal,
		"tools":        s.agent.Tools(),
		"mcp":          s.mcpInfo,
		"schedulers":   s.manager.List(),
		"scheduler":    s.manager.FirstStatus(),
		"observations": s.manager.Observations(),
		"reports":      s.manager.Reports(20),
		"model":        s.cfg.model,
		"configured":   s.cfg.apiKey != "",
	}
}

func (s *server) schedulerStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.manager.List())
}

func (s *server) schedulerCollect(w http.ResponseWriter, r *http.Request) {
	var body struct {
		City string `json:"city"`
	}
	_ = decodeJSON(w, r, &body)
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	scheduler := s.manager.Get(body.City)
	if scheduler == nil {
		writeError(w, http.StatusNotFound, "планировщик не найден")
		return
	}
	observation, err := scheduler.CollectNow(ctx)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"observation": observation, "state": s.statePayload(ctx)})
}

func (s *server) schedulerReport(w http.ResponseWriter, r *http.Request) {
	var body struct {
		City string `json:"city"`
	}
	_ = decodeJSON(w, r, &body)
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	scheduler := s.manager.Get(body.City)
	if scheduler == nil {
		writeError(w, http.StatusNotFound, "планировщик не найден")
		return
	}
	report, err := scheduler.ReportNow(ctx)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"report": report, "state": s.statePayload(ctx)})
}

func (s *server) chat(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Message string `json:"message"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	message := strings.TrimSpace(body.Message)
	if message == "" {
		writeError(w, http.StatusBadRequest, "сообщение пустое")
		return
	}
	if len([]rune(message)) > 4000 {
		writeError(w, http.StatusBadRequest, "сообщение слишком длинное (максимум 4000 символов)")
		return
	}

	s.chatMu.Lock()
	defer s.chatMu.Unlock()

	state := s.store.Get()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()

	result, err := s.agent.Run(ctx, state.History(), message)

	turn := Turn{
		ID:        newID(),
		User:      message,
		Reply:     result.Reply,
		Steps:     result.Steps,
		Usage:     result.Usage,
		LatencyMs: result.LatencyMs,
		Calls:     result.Calls,
		CreatedAt: time.Now(),
	}
	if err != nil {
		turn.Error = err.Error()
	}

	state.Turns = append(state.Turns, turn)
	if saveErr := s.store.Save(state); saveErr != nil {
		writeError(w, http.StatusInternalServerError, saveErr.Error())
		return
	}

	writeJSON(w, http.StatusOK, turn)
}

func (s *server) resetAll(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := s.manager.ResetAll(ctx); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.store.Reset(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.statePayload(ctx))
}

func (s *server) reset(w http.ResponseWriter, r *http.Request) {
	s.chatMu.Lock()
	defer s.chatMu.Unlock()

	if err := s.store.Reset(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func loadConfig() config {
	baseURL := strings.TrimRight(envOr("DEEPSEEK_BASE_URL", "https://api.deepseek.com"), "/")
	model := envOr("DEEPSEEK_MODEL", "deepseek-flash")
	keyFile := envOr("DEEPSEEK_API_KEY_FILE", "api_key.txt")
	port := envOr("PORT", "8088")
	mcpName := envOr("MCP_SERVER", "weather")
	city := envOr("CITY", "Москва")
	collect := parseDuration(envOr("COLLECT_EVERY", "20s"), 20*time.Second)
	report := parseDuration(envOr("REPORT_EVERY", "60s"), 60*time.Second)

	return config{
		baseURL: baseURL,
		model:   model,
		keyFile: keyFile,
		apiKey:  readSecret(keyFile, "sk-your-key-here"),
		addr:    ":" + port,
		mcpName: mcpName,
		city:    city,
		collect: collect,
		report:  report,
	}
}

func parseDuration(value string, fallback time.Duration) time.Duration {
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return fallback
	}
	return duration
}

func readSecret(path, placeholder string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	value := unquote(strings.TrimSpace(strings.TrimPrefix(string(data), "\ufeff")))
	if value == placeholder {
		return ""
	}
	return value
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		return fmt.Errorf("некорректный JSON: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}

func loadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("файл %s не найден, использую переменные окружения", path)
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(strings.TrimPrefix(scanner.Text(), "\ufeff"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(strings.TrimPrefix(key, "export "))
		value = unquote(strings.TrimSpace(value))
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, value)
		}
	}
	return scanner.Err()
}

func unquote(v string) string {
	if len(v) >= 2 {
		if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
			return v[1 : len(v)-1]
		}
	}
	return v
}
