package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type WeatherArgs struct {
	City string `json:"city" jsonschema:"Название города, например Москва или Moscow"`
}

type WeatherResult struct {
	City         string  `json:"city"`
	Country      string  `json:"country"`
	TemperatureC float64 `json:"temperatureC"`
	Humidity     int     `json:"humidity"`
	WindKmh      float64 `json:"windKmh"`
	Condition    string  `json:"condition"`
	ObservedAt   string  `json:"observedAt"`
}

var weatherHTTP = &http.Client{Timeout: 20 * time.Second}

func runWeatherMCPServer() {
	store, err := NewObservationStore(filepath.Join("data", "weather"))
	if err != nil {
		log.Printf("weather store: %v", err)
		return
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "weather", Version: "1.0.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "collect_weather",
		Description: "Возвращает текущую погоду в городе через Open-Meteo",
	}, collectWeatherTool)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "save_observation",
		Description: "Сохраняет наблюдение погоды в накопительное хранилище",
	}, saveObservationTool(store))
	mcp.AddTool(server, &mcp.Tool{
		Name:        "read_observations",
		Description: "Читает накопленные наблюдения за период",
	}, readObservationsTool(store))
	mcp.AddTool(server, &mcp.Tool{
		Name:        "save_report",
		Description: "Сохраняет агрегированный отчёт",
	}, saveReportTool(store))
	mcp.AddTool(server, &mcp.Tool{
		Name:        "clear_weather_data",
		Description: "Удаляет все накопленные наблюдения и отчёты",
	}, clearWeatherDataTool(store))
	mcp.AddTool(server, &mcp.Tool{
		Name:        "read_reports",
		Description: "Возвращает последние сохранённые отчёты",
	}, readReportsTool(store))
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		log.Printf("weather MCP: %v", err)
	}
}

func collectWeatherTool(ctx context.Context, _ *mcp.CallToolRequest, args WeatherArgs) (*mcp.CallToolResult, any, error) {
	city := strings.TrimSpace(args.City)
	if city == "" {
		return nil, nil, fmt.Errorf("city обязателен")
	}
	location, err := geocodeCity(ctx, city)
	if err != nil {
		return nil, nil, err
	}
	result, err := currentWeather(ctx, location)
	if err != nil {
		return nil, nil, err
	}
	return nil, result, nil
}

type geoLocation struct {
	Name      string  `json:"name"`
	Country   string  `json:"country"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type geoResponse struct {
	Results []geoLocation `json:"results"`
}

func geocodeCity(ctx context.Context, city string) (geoLocation, error) {
	endpoint := "https://geocoding-api.open-meteo.com/v1/search?name=" + url.QueryEscape(city) + "&count=1&language=ru&format=json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return geoLocation{}, err
	}
	resp, err := weatherHTTP.Do(req)
	if err != nil {
		return geoLocation{}, fmt.Errorf("Open-Meteo geocoding: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return geoLocation{}, fmt.Errorf("Open-Meteo geocoding: HTTP %d", resp.StatusCode)
	}
	var data geoResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return geoLocation{}, fmt.Errorf("Open-Meteo geocoding JSON: %w", err)
	}
	if len(data.Results) == 0 {
		return geoLocation{}, fmt.Errorf("город %q не найден", city)
	}
	return data.Results[0], nil
}

type forecastResponse struct {
	Current struct {
		Time        string  `json:"time"`
		Temperature float64 `json:"temperature_2m"`
		Humidity    int     `json:"relative_humidity_2m"`
		WindSpeed   float64 `json:"wind_speed_10m"`
		WeatherCode int     `json:"weather_code"`
	} `json:"current"`
}

func currentWeather(ctx context.Context, location geoLocation) (WeatherResult, error) {
	endpoint := fmt.Sprintf(
		"https://api.open-meteo.com/v1/forecast?latitude=%.4f&longitude=%.4f&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code&timezone=auto",
		location.Latitude, location.Longitude,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return WeatherResult{}, err
	}
	resp, err := weatherHTTP.Do(req)
	if err != nil {
		return WeatherResult{}, fmt.Errorf("Open-Meteo forecast: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return WeatherResult{}, fmt.Errorf("Open-Meteo forecast: HTTP %d", resp.StatusCode)
	}
	var data forecastResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return WeatherResult{}, fmt.Errorf("Open-Meteo forecast JSON: %w", err)
	}
	return WeatherResult{
		City:         location.Name,
		Country:      location.Country,
		TemperatureC: data.Current.Temperature,
		Humidity:     data.Current.Humidity,
		WindKmh:      data.Current.WindSpeed,
		Condition:    weatherCondition(data.Current.WeatherCode),
		ObservedAt:   data.Current.Time,
	}, nil
}

type observationRange struct {
	City string `json:"city,omitempty" jsonschema:"Город; пусто — все города"`
	From string `json:"from,omitempty" jsonschema:"Начало периода в RFC3339"`
	To   string `json:"to,omitempty" jsonschema:"Конец периода в RFC3339"`
}

type reportLimit struct {
	Limit int `json:"limit,omitempty" jsonschema:"Сколько последних отчётов вернуть; 0 — все"`
}

func clearWeatherDataTool(store *ObservationStore) func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, map[string]any, error) {
	return func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, map[string]any, error) {
		if err := store.ClearAll(); err != nil {
			return nil, nil, err
		}
		return nil, map[string]any{"status": "cleared"}, nil
	}
}

func saveObservationTool(store *ObservationStore) func(context.Context, *mcp.CallToolRequest, Observation) (*mcp.CallToolResult, Observation, error) {
	return func(ctx context.Context, _ *mcp.CallToolRequest, observation Observation) (*mcp.CallToolResult, Observation, error) {
		saved, err := store.SaveObservation(observation)
		if err != nil {
			return nil, Observation{}, err
		}
		return nil, saved, nil
	}
}

func readObservationsTool(store *ObservationStore) func(context.Context, *mcp.CallToolRequest, observationRange) (*mcp.CallToolResult, []Observation, error) {
	return func(ctx context.Context, _ *mcp.CallToolRequest, args observationRange) (*mcp.CallToolResult, []Observation, error) {
		items, err := store.ReadObservations(args.City, args.From, args.To)
		if err != nil {
			return nil, nil, err
		}
		return nil, items, nil
	}
}

func saveReportTool(store *ObservationStore) func(context.Context, *mcp.CallToolRequest, WeatherReport) (*mcp.CallToolResult, WeatherReport, error) {
	return func(ctx context.Context, _ *mcp.CallToolRequest, report WeatherReport) (*mcp.CallToolResult, WeatherReport, error) {
		saved, err := store.SaveReport(report)
		if err != nil {
			return nil, WeatherReport{}, err
		}
		return nil, saved, nil
	}
}

func readReportsTool(store *ObservationStore) func(context.Context, *mcp.CallToolRequest, reportLimit) (*mcp.CallToolResult, []WeatherReport, error) {
	return func(ctx context.Context, _ *mcp.CallToolRequest, args reportLimit) (*mcp.CallToolResult, []WeatherReport, error) {
		items, err := store.ReadReports(args.Limit)
		if err != nil {
			return nil, nil, err
		}
		return nil, items, nil
	}
}

func weatherCondition(code int) string {
	switch {
	case code == 0:
		return "ясно"
	case code == 1:
		return "преимущественно ясно"
	case code == 2:
		return "переменная облачность"
	case code == 3:
		return "облачно"
	case code == 45 || code == 48:
		return "туман"
	case code >= 51 && code <= 57:
		return "морось"
	case code >= 61 && code <= 67:
		return "дождь"
	case code >= 71 && code <= 77:
		return "снег"
	case code >= 80 && code <= 82:
		return "ливень"
	case code >= 85 && code <= 86:
		return "снегопад"
	case code >= 95:
		return "гроза"
	default:
		return fmt.Sprintf("код погоды %d", code)
	}
}
