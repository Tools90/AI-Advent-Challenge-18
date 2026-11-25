package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type Scheduler struct {
	mu              sync.RWMutex
	mcp             *MCPClient
	llm             LLMClient
	chat            *ChatStore
	city            string
	collectEvery    time.Duration
	reportEvery     time.Duration
	startedAt       time.Time
	lastCollect     time.Time
	lastReport      time.Time
	nextCollect     time.Time
	nextReport      time.Time
	runs            int
	reportCount     int
	lastError       string
	lastObservation *Observation
	observations    []Observation
	reports         []WeatherReport
	stop            chan struct{}
	started         bool
}

type SchedulerStatus struct {
	Running         bool         `json:"running"`
	City            string       `json:"city"`
	CollectEvery    string       `json:"collectEvery"`
	ReportEvery     string       `json:"reportEvery"`
	Runs            int          `json:"runs"`
	Reports         int          `json:"reports"`
	LastCollect     string       `json:"lastCollect"`
	LastReport      string       `json:"lastReport"`
	NextCollect     string       `json:"nextCollect"`
	NextReport      string       `json:"nextReport"`
	LastError       string       `json:"lastError"`
	LastObservation *Observation `json:"lastObservation,omitempty"`
}

func NewScheduler(mcp *MCPClient, llm LLMClient, chat *ChatStore, city string, collectEvery, reportEvery time.Duration) *Scheduler {
	now := time.Now()
	return &Scheduler{
		mcp:          mcp,
		llm:          llm,
		chat:         chat,
		city:         city,
		collectEvery: collectEvery,
		reportEvery:  reportEvery,
		startedAt:    now,
		nextCollect:  now,
		nextReport:   now.Add(reportEvery),
		stop:         make(chan struct{}),
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.startedAt = time.Now()
	s.mu.Unlock()

	s.bootstrap(ctx)
	collectTicker := time.NewTicker(s.collectEvery)
	reportTicker := time.NewTicker(s.reportEvery)
	go func() {
		defer collectTicker.Stop()
		defer reportTicker.Stop()
		s.runCollect(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stop:
				return
			case <-collectTicker.C:
				s.runCollect(ctx)
			case <-reportTicker.C:
				s.runReport(ctx)
			}
		}
	}()
}

func (s *Scheduler) bootstrap(ctx context.Context) {
	if s.mcp == nil {
		return
	}
	raw, err := s.mcp.Call(ctx, "read_observations", map[string]any{"city": s.city})
	if err == nil {
		var observations []Observation
		if decodeAny(raw, &observations) == nil {
			s.mu.Lock()
			s.observations = observations
			if len(observations) > 0 {
				if earliest, err := time.Parse(time.RFC3339, observations[0].CollectedAt); err == nil && earliest.Before(s.startedAt) {
					s.startedAt = earliest
				}
			}
			s.mu.Unlock()
		}
	}
	raw, err = s.mcp.Call(ctx, "read_reports", map[string]any{"limit": 0})
	if err == nil {
		var reports []WeatherReport
		if decodeAny(raw, &reports) == nil {
			s.mu.Lock()
			s.reports = reports
			s.reportCount = len(reports)
			s.mu.Unlock()
		}
	}
}

func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return
	}
	close(s.stop)
	s.started = false
}

func (s *Scheduler) Status() SchedulerStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	status := SchedulerStatus{
		Running:      s.started,
		City:         s.city,
		CollectEvery: s.collectEvery.String(),
		ReportEvery:  s.reportEvery.String(),
		Runs:         s.runs,
		Reports:      s.reportCount,
		LastError:    s.lastError,
	}
	if !s.lastCollect.IsZero() {
		status.LastCollect = s.lastCollect.Format(time.RFC3339)
	}
	if !s.lastReport.IsZero() {
		status.LastReport = s.lastReport.Format(time.RFC3339)
	}
	if !s.nextCollect.IsZero() {
		status.NextCollect = s.nextCollect.Format(time.RFC3339)
	}
	if !s.nextReport.IsZero() {
		status.NextReport = s.nextReport.Format(time.RFC3339)
	}
	if s.lastObservation != nil {
		obs := *s.lastObservation
		status.LastObservation = &obs
	}
	return status
}

func (s *Scheduler) Observations(ctx context.Context) ([]Observation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]Observation{}, s.observations...), nil
}

func (s *Scheduler) Reports(ctx context.Context, limit int) ([]WeatherReport, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 || len(s.reports) <= limit {
		return append([]WeatherReport{}, s.reports...), nil
	}
	return append([]WeatherReport{}, s.reports[len(s.reports)-limit:]...), nil
}

func (s *Scheduler) CollectNow(ctx context.Context) (Observation, error) {
	return s.collect(ctx)
}

func (s *Scheduler) ReportNow(ctx context.Context) (WeatherReport, error) {
	return s.report(ctx)
}

func (s *Scheduler) runCollect(ctx context.Context) {
	s.mu.Lock()
	s.nextCollect = time.Now().Add(s.collectEvery)
	s.mu.Unlock()
	if _, err := s.collect(ctx); err != nil {
		s.mu.Lock()
		s.lastError = err.Error()
		s.mu.Unlock()
	}
}

func (s *Scheduler) collect(ctx context.Context) (Observation, error) {
	if s.mcp == nil {
		return Observation{}, fmt.Errorf("MCP не подключён")
	}
	raw, err := s.mcp.Call(ctx, "collect_weather", map[string]any{"city": s.city})
	if err != nil {
		return Observation{}, err
	}
	var weather WeatherResult
	if err := decodeAny(raw, &weather); err != nil {
		return Observation{}, err
	}
	observation := Observation{
		City:         weather.City,
		Country:      weather.Country,
		TemperatureC: weather.TemperatureC,
		Humidity:     weather.Humidity,
		WindKmh:      weather.WindKmh,
		Condition:    weather.Condition,
		ObservedAt:   weather.ObservedAt,
		CollectedAt:  time.Now().Format(time.RFC3339),
	}
	args, _ := toMap(observation)
	raw, err = s.mcp.Call(ctx, "save_observation", args)
	if err != nil {
		return Observation{}, err
	}
	if err := decodeAny(raw, &observation); err != nil {
		return Observation{}, err
	}
	now := time.Now()
	s.mu.Lock()
	s.runs++
	s.lastCollect = now
	s.lastObservation = &observation
	s.observations = append(s.observations, observation)
	s.lastError = ""
	s.mu.Unlock()
	return observation, nil
}

func (s *Scheduler) runReport(ctx context.Context) {
	s.mu.Lock()
	s.nextReport = time.Now().Add(s.reportEvery)
	from := s.startedAt.Format(time.RFC3339)
	s.mu.Unlock()
	if _, err := s.reportFrom(ctx, from); err != nil {
		s.mu.Lock()
		s.lastError = err.Error()
		s.mu.Unlock()
	}
}

func (s *Scheduler) report(ctx context.Context) (WeatherReport, error) {
	s.mu.RLock()
	from := s.startedAt.Format(time.RFC3339)
	s.mu.RUnlock()
	return s.reportFrom(ctx, from)
}

func (s *Scheduler) reportFrom(ctx context.Context, from string) (WeatherReport, error) {
	if s.mcp == nil {
		return WeatherReport{}, fmt.Errorf("MCP не подключён")
	}
	to := time.Now().Format(time.RFC3339)
	raw, err := s.mcp.Call(ctx, "read_observations", map[string]any{"city": s.city, "from": from, "to": to})
	if err != nil {
		return WeatherReport{}, err
	}
	var observations []Observation
	if err := decodeAny(raw, &observations); err != nil {
		return WeatherReport{}, err
	}
	report := buildWeatherReport(s.city, from, to, observations)
	report.Summary = s.summarize(ctx, report)
	args, _ := toMap(report)
	raw, err = s.mcp.Call(ctx, "save_report", args)
	if err != nil {
		return WeatherReport{}, err
	}
	if err := decodeAny(raw, &report); err != nil {
		return WeatherReport{}, err
	}
	s.mu.Lock()
	s.reportCount++
	s.lastReport = time.Now()
	s.reports = append(s.reports, report)
	s.lastError = ""
	s.mu.Unlock()
	return report, nil
}

func buildWeatherReport(city, from, to string, observations []Observation) WeatherReport {
	report := WeatherReport{
		City:        city,
		PeriodStart: from,
		PeriodEnd:   to,
		RunCount:    len(observations),
		Conditions:  map[string]int{},
		CreatedAt:   time.Now().Format(time.RFC3339),
	}
	if len(observations) == 0 {
		return report
	}
	report.TempMin = observations[0].TemperatureC
	report.TempMax = observations[0].TemperatureC
	var tempSum, humiditySum, windSum float64
	for _, observation := range observations {
		if observation.TemperatureC < report.TempMin {
			report.TempMin = observation.TemperatureC
		}
		if observation.TemperatureC > report.TempMax {
			report.TempMax = observation.TemperatureC
		}
		if observation.WindKmh > report.WindMax {
			report.WindMax = observation.WindKmh
		}
		tempSum += observation.TemperatureC
		humiditySum += float64(observation.Humidity)
		windSum += observation.WindKmh
		report.Conditions[observation.Condition]++
	}
	count := float64(len(observations))
	report.TempAvg = tempSum / count
	report.HumidityAvg = humiditySum / count
	report.WindAvg = windSum / count
	return report
}

func (s *Scheduler) summarize(ctx context.Context, report WeatherReport) string {
	if report.RunCount == 0 {
		return "За период не было ни одного наблюдения."
	}
	conditions := make([]string, 0, len(report.Conditions))
	for condition, count := range report.Conditions {
		conditions = append(conditions, fmt.Sprintf("%s — %d", condition, count))
	}
	sort.Strings(conditions)
	prompt := fmt.Sprintf(
		"Напиши краткую оперативную сводку погоды по-русски, 3-4 предложения. Используй только факты ниже, не выдумывай. Город: %s. Период: %s — %s. Запусков: %d. Температура: min %.1f, max %.1f, avg %.1f °C. Влажность avg %.0f%%. Ветер avg %.1f, max %.1f км/ч. Условия: %s.",
		report.City, report.PeriodStart, report.PeriodEnd, report.RunCount,
		report.TempMin, report.TempMax, report.TempAvg, report.HumidityAvg,
		report.WindAvg, report.WindMax, strings.Join(conditions, "; "),
	)
	if s.llm == nil {
		return fallbackSummary(report)
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	response, err := s.llm.Chat(ctx, []LLMMessage{{Role: "user", Content: prompt}}, nil)
	summary := ""
	if err != nil {
	} else {
		summary = strings.TrimSpace(response.Message.Content)
	}
	if summary == "" {
		summary = fallbackSummary(report)
	}
	return summary
}

func fallbackSummary(report WeatherReport) string {
	return fmt.Sprintf(
		"За период выполнено %d наблюдений. Температура: от %.1f до %.1f °C, средняя %.1f °C. Средняя влажность %.0f%%, средний ветер %.1f км/ч.",
		report.RunCount, report.TempMin, report.TempMax, report.TempAvg, report.HumidityAvg, report.WindAvg,
	)
}

func decodeAny(value any, dst any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dst)
}

func toMap(value any) (map[string]any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}
