/*
Copyright 2026 The Fluid Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cron

import (
	"testing"
	"time"
)

func mustParse(t *testing.T, expr string) *Schedule {
	t.Helper()
	s, err := Parse(expr)
	if err != nil {
		t.Fatalf("Parse(%q) returned unexpected error: %v", expr, err)
	}
	return s
}

func TestParseInvalid(t *testing.T) {
	invalid := []string{
		"",
		"   ",
		"* * * *",
		"* * * * * *",
		"60 * * * *",
		"* 24 * * *",
		"* * 32 * *",
		"* * * 13 *",
		"* * * * 8",
		"*/a * * * *",
		"*/0 * * * *",
		"5-3 * * * *",
		"1,,2 * * * *",
		"abc * * * *",
		"? * * * *",
	}
	for _, expr := range invalid {
		if _, err := Parse(expr); err == nil {
			t.Errorf("Parse(%q) expected error, got nil", expr)
		}
	}
}

func TestNextBasic(t *testing.T) {
	loc := time.UTC
	now := time.Date(2026, 11, 9, 10, 30, 25, 0, loc)

	cases := []struct {
		expr string
		want time.Time
	}{
		{"* * * * *", time.Date(2026, 11, 9, 10, 31, 0, 0, loc)},
		{"0 * * * *", time.Date(2026, 11, 9, 11, 0, 0, 0, loc)},
		{"0 0 * * *", time.Date(2026, 11, 10, 0, 0, 0, 0, loc)},
		{"*/15 * * * *", time.Date(2026, 11, 9, 10, 45, 0, 0, loc)},
		{"0 0 1 * *", time.Date(2026, 12, 1, 0, 0, 0, 0, loc)},
		{"0 9 ? * MON-FRI", time.Date(2026, 11, 10, 9, 0, 0, 0, loc)}, // 2026-11-09 is Monday; next weekday 9am is Tue 10th
		{"30 10 * * *", time.Date(2026, 11, 10, 10, 30, 0, 0, loc)},  // same minute already passed
		{"0 0 1 1 *", time.Date(2027, 1, 1, 0, 0, 0, 0, loc)},
	}

	for _, c := range cases {
		s := mustParse(t, c.expr)
		got, err := s.Next(now)
		if err != nil {
			t.Fatalf("Next for %q returned error: %v", c.expr, err)
		}
		if !got.Equal(c.want) {
			t.Errorf("Next(%q) = %v, want %v", c.expr, got, c.want)
		}
	}
}

func TestNextNamesAndSundayAlias(t *testing.T) {
	loc := time.UTC
	now := time.Date(2026, 11, 9, 10, 30, 0, 0, loc) // Monday

	// named month + named weekday: every Saturday in December, the first one after Nov 9 is Dec 5 2026.
	s := mustParse(t, "0 0 * DEC SAT")
	got, err := s.Next(now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.Equal(time.Date(2026, 12, 5, 0, 0, 0, 0, loc)) || got.Weekday() != time.Saturday {
		t.Errorf("expected Saturday 2026-12-05, got %v (%v)", got, got.Weekday())
	}

	// 7 should be treated as Sunday
	sun := mustParse(t, "0 0 * * 7")
	got, err = sun.Next(now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Weekday() != time.Sunday {
		t.Errorf("expected Sunday, got %v", got.Weekday())
	}
}

func TestDayOfMonthOrDayOfWeekSemantics(t *testing.T) {
	loc := time.UTC
	// "0 0 15 * MON" matches 15th of month OR every Monday.
	s := mustParse(t, "0 0 15 * MON")
	from := time.Date(2026, 11, 9, 10, 30, 0, 0, loc) // Monday Nov 9
	got, err := s.Next(from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Next match is Monday Nov 16 (weekday rule), since the 15th (Sunday) is before that?
	// Nov 15 2026 is a Sunday and still after Nov 9, so the 15th matches via dom rule first.
	if !got.Equal(time.Date(2026, 11, 15, 0, 0, 0, 0, loc)) {
		t.Errorf("got %v, want 2026-11-15 00:00 (day-of-month match)", got)
	}
}

func TestTruncatesSeconds(t *testing.T) {
	s := mustParse(t, "* * * * *")
	got, err := s.Next(time.Date(2026, 11, 9, 10, 30, 59, 999, time.UTC))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Second() != 0 || got.Nanosecond() != 0 {
		t.Errorf("expected whole minute, got %v", got)
	}
}
