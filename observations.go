package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Observation struct {
	ID           string  `json:"id"`
	City         string  `json:"city"`
	Country      string  `json:"country"`
	TemperatureC float64 `json:"temperatureC"`
	Humidity     int     `json:"humidity"`
	WindKmh      float64 `json:"windKmh"`
	Condition    string  `json:"condition"`
	ObservedAt   string  `json:"observedAt"`
	CollectedAt  string  `json:"collectedAt"`
}

type WeatherReport struct {
	ID          string         `json:"id"`
	City        string         `json:"city"`
	PeriodStart string         `json:"periodStart"`
	PeriodEnd   string         `json:"periodEnd"`
	RunCount    int            `json:"runCount"`
	TempMin     float64        `json:"tempMin"`
	TempMax     float64        `json:"tempMax"`
	TempAvg     float64        `json:"tempAvg"`
	HumidityAvg float64        `json:"humidityAvg"`
	WindAvg     float64        `json:"windAvg"`
	WindMax     float64        `json:"windMax"`
	Conditions  map[string]int `json:"conditions"`
	Summary     string         `json:"summary"`
	CreatedAt   string         `json:"createdAt"`
}

type ObservationStore struct {
	mu      sync.Mutex
	dir     string
	obsPath string
	repPath string
	obs     []Observation
	reports []WeatherReport
}

func NewObservationStore(dir string) (*ObservationStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	store := &ObservationStore{
		dir:     dir,
		obsPath: filepath.Join(dir, "observations.json"),
		repPath: filepath.Join(dir, "reports.json"),
	}
	if err := loadJSON(store.obsPath, &store.obs); err != nil {
		return nil, err
	}
	if err := loadJSON(store.repPath, &store.reports); err != nil {
		return nil, err
	}
	if store.obs == nil {
		store.obs = []Observation{}
	}
	if store.reports == nil {
		store.reports = []WeatherReport{}
	}
	return store, nil
}

func loadJSON(path string, dst any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	data = trimBOM(data)
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, dst)
}

func (s *ObservationStore) ClearAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.obs = []Observation{}
	s.reports = []WeatherReport{}
	_ = os.Remove(s.obsPath)
	_ = os.Remove(s.repPath)
	return nil
}

func (s *ObservationStore) SaveObservation(observation Observation) (Observation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if observation.ID == "" {
		observation.ID = "obs-" + newID()
	}
	if observation.CollectedAt == "" {
		observation.CollectedAt = time.Now().Format(time.RFC3339)
	}
	s.obs = append(s.obs, observation)
	if err := saveJSON(s.obsPath, s.obs); err != nil {
		return Observation{}, err
	}
	return observation, nil
}

func (s *ObservationStore) ReadObservations(city, from, to string) ([]Observation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Observation{}
	for _, observation := range s.obs {
		if city != "" && observation.City != city {
			continue
		}
		if from != "" && observation.CollectedAt < from {
			continue
		}
		if to != "" && observation.CollectedAt > to {
			continue
		}
		out = append(out, observation)
	}
	return out, nil
}

func (s *ObservationStore) SaveReport(report WeatherReport) (WeatherReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if report.ID == "" {
		report.ID = "rep-" + newID()
	}
	if report.CreatedAt == "" {
		report.CreatedAt = time.Now().Format(time.RFC3339)
	}
	s.reports = append(s.reports, report)
	if err := saveJSON(s.repPath, s.reports); err != nil {
		return WeatherReport{}, err
	}
	return report, nil
}

func (s *ObservationStore) ReadReports(limit int) ([]WeatherReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 || len(s.reports) <= limit {
		return append([]WeatherReport{}, s.reports...), nil
	}
	return append([]WeatherReport{}, s.reports[len(s.reports)-limit:]...), nil
}

func saveJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	_ = os.Remove(path)
	return os.Rename(tmp, path)
}

func trimBOM(data []byte) []byte {
	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		return data[3:]
	}
	return data
}
