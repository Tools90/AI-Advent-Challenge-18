package main

import (
	"strings"
	"testing"
)

func TestBuildWeatherReportAggregatesAll(t *testing.T) {
	observations := []Observation{
		{City: "Москва", TemperatureC: -2, Humidity: 80, WindKmh: 10, Condition: "снег"},
		{City: "Москва", TemperatureC: 1, Humidity: 90, WindKmh: 20, Condition: "снег"},
		{City: "Москва", TemperatureC: 3, Humidity: 70, WindKmh: 15, Condition: "облачно"},
	}
	report := buildWeatherReport("Москва", "2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z", observations)
	if report.RunCount != 3 {
		t.Fatalf("runCount = %d", report.RunCount)
	}
	if report.TempMin != -2 || report.TempMax != 3 || report.TempAvg != 2.0/3.0 {
		t.Fatalf("temp = %+v", report)
	}
	if report.HumidityAvg != 80 || report.WindAvg != 15 || report.WindMax != 20 {
		t.Fatalf("stats = %+v", report)
	}
	if report.Conditions["снег"] != 2 || report.Conditions["облачно"] != 1 {
		t.Fatalf("conditions = %+v", report.Conditions)
	}
}

func TestFallbackSummaryUsesRunCount(t *testing.T) {
	summary := fallbackSummary(WeatherReport{RunCount: 4, TempMin: -1, TempMax: 2, TempAvg: 0.5, HumidityAvg: 80, WindAvg: 12})
	if !strings.Contains(summary, "4") || !strings.Contains(summary, "наблюдений") {
		t.Fatalf("summary = %s", summary)
	}
}
