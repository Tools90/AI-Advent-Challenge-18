package main

import "testing"

func TestWeatherCondition(t *testing.T) {
	cases := map[int]string{
		0:  "ясно",
		2:  "переменная облачность",
		61: "дождь",
		71: "снег",
		95: "гроза",
	}
	for code, want := range cases {
		if got := weatherCondition(code); got != want {
			t.Errorf("weatherCondition(%d) = %q, want %q", code, got, want)
		}
	}
}
