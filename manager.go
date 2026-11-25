package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type SchedulerManager struct {
	mu    sync.Mutex
	mcp   *MCPClient
	llm   LLMClient
	chat  *ChatStore
	items map[string]*Scheduler
	order []string
}

func NewSchedulerManager(mcp *MCPClient, llm LLMClient, chat *ChatStore) *SchedulerManager {
	return &SchedulerManager{
		mcp:   mcp,
		llm:   llm,
		chat:  chat,
		items: map[string]*Scheduler{},
	}
}

func (m *SchedulerManager) Create(city string, collect, report time.Duration) (*Scheduler, error) {
	city = strings.TrimSpace(city)
	if city == "" {
		return nil, fmt.Errorf("city обязателен")
	}
	if m.mcp == nil {
		return nil, fmt.Errorf("MCP не подключён")
	}
	if collect < 5*time.Second {
		collect = 5 * time.Second
	}
	if report < 10*time.Second {
		report = 10 * time.Second
	}
	m.mu.Lock()
	old := m.items[city]
	if old != nil {
		old.Stop()
	}
	scheduler := NewScheduler(m.mcp, m.llm, m.chat, city, collect, report)
	m.items[city] = scheduler
	found := false
	for _, item := range m.order {
		if item == city {
			found = true
			break
		}
	}
	if !found {
		m.order = append(m.order, city)
	}
	m.mu.Unlock()
	scheduler.Start(context.Background())
	return scheduler, nil
}

func (m *SchedulerManager) Get(city string) *Scheduler {
	m.mu.Lock()
	defer m.mu.Unlock()
	if city == "" && len(m.order) > 0 {
		return m.items[m.order[0]]
	}
	return m.items[city]
}

func (m *SchedulerManager) Stop(city string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	scheduler := m.items[city]
	if scheduler == nil {
		return fmt.Errorf("планировщик %q не найден", city)
	}
	scheduler.Stop()
	delete(m.items, city)
	next := m.order[:0]
	for _, item := range m.order {
		if item != city {
			next = append(next, item)
		}
	}
	m.order = next
	return nil
}

func (m *SchedulerManager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, scheduler := range m.items {
		scheduler.Stop()
	}
	m.items = map[string]*Scheduler{}
	m.order = nil
}

func (m *SchedulerManager) List() []SchedulerStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SchedulerStatus, 0, len(m.order))
	for _, city := range m.order {
		if scheduler := m.items[city]; scheduler != nil {
			out = append(out, scheduler.Status())
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].City < out[j].City })
	return out
}

func (m *SchedulerManager) Observations() []Observation {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Observation{}
	for _, scheduler := range m.items {
		items, _ := scheduler.Observations(context.Background())
		out = append(out, items...)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].CollectedAt < out[j].CollectedAt })
	return out
}

func (m *SchedulerManager) Reports(limit int) []WeatherReport {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []WeatherReport{}
	for _, scheduler := range m.items {
		items, _ := scheduler.Reports(context.Background(), 0)
		out = append(out, items...)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

func (m *SchedulerManager) FirstStatus() SchedulerStatus {
	items := m.List()
	if len(items) == 0 {
		return SchedulerStatus{}
	}
	return items[0]
}

func (m *SchedulerManager) ResetAll(ctx context.Context) error {
	m.StopAll()
	if m.mcp == nil {
		return nil
	}
	_, err := m.mcp.Call(ctx, "clear_weather_data", map[string]any{})
	return err
}
