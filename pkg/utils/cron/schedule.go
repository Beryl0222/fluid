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

// Package cron provides a self-contained parser for the standard 5-field cron
// format (minute hour day-of-month month day-of-week). It exists so that fluid can
// validate schedules and compute scheduled times without depending on an external
// cron library, which keeps schedule decisions inside the controller instead of
// leaving them to the workload only.
package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// fieldCount is the number of space separated fields of a standard cron expression.
	fieldCount = 5
	// bitset size, covers all field ranges.
	bitSize = 64
)

// Schedule is a parsed cron expression. Each field is stored as a bitset in which
// bit n is set when value n matches.
type Schedule struct {
	expression string
	minute     uint64
	hour       uint64
	dom        uint64 // 1-31
	month      uint64 // 1-12
	dow        uint64 // 0-6, Sunday = 0

	// domStar/dowStar track whether the corresponding field is unrestricted ("*"),
	// which decides the OR rule between day-of-month and day-of-week.
	domStar bool
	dowStar bool
}

var (
	monthNames = map[string]uint{
		"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
		"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
	}
	dowNames = map[string]uint{
		"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
	}
)

// Parse parses a standard 5-field cron expression and returns an error describing
// exactly which field is invalid when the expression is not supported.
func Parse(expression string) (*Schedule, error) {
	fields := strings.Fields(strings.TrimSpace(expression))
	if len(fields) == 0 {
		return nil, fmt.Errorf("empty cron schedule, expect 5 fields: minute hour day-of-month month day-of-week")
	}
	if len(fields) != fieldCount {
		return nil, fmt.Errorf("invalid cron schedule %q: expected exactly 5 fields (minute hour day-of-month month day-of-week), got %d",
			expression, len(fields))
	}

	s := &Schedule{expression: expression}
	var err error
	fieldNames := [fieldCount]string{"minute", "hour", "day-of-month", "month", "day-of-week"}
	ranges := [fieldCount][2]uint{
		{0, 59},
		{0, 23},
		{1, 31},
		{1, 12},
		{0, 6},
	}

	if s.minute, err = parseField(fields[0], ranges[0], fieldNames[0], nil); err != nil {
		return nil, err
	}
	if s.hour, err = parseField(fields[1], ranges[1], fieldNames[1], nil); err != nil {
		return nil, err
	}
	if s.dom, s.domStar, err = parseDayOfMonth(fields[2]); err != nil {
		return nil, err
	}
	if s.month, err = parseField(fields[3], ranges[3], fieldNames[3], monthNames); err != nil {
		return nil, err
	}
	if s.dow, s.dowStar, err = parseDayOfWeek(fields[4]); err != nil {
		return nil, err
	}

	return s, nil
}

// Expression returns the original cron expression.
func (s *Schedule) Expression() string {
	return s.expression
}

// Next returns the earliest time strictly after t that matches the schedule.
// The returned time keeps the location of t and is truncated to whole minutes.
// An error is only returned if no match can be found within 5 years, which
// indicates an over-constrained schedule.
func (s *Schedule) Next(t time.Time) (time.Time, error) {
	// start at the next whole minute
	next := t.Add(time.Minute - time.Duration(t.Second())*time.Second - time.Duration(t.Nanosecond())*time.Nanosecond)
	// guard against pathological expressions such as "0 0 31 2 *" (Feb 31st never exists)
	limit := next.AddDate(5, 0, 0)

	for next.Before(limit) {
		if s.month&(1<<uint(next.Month())) == 0 {
			// jump to the first day of the next month
			next = time.Date(next.Year(), next.Month()+1, 1, 0, 0, 0, 0, next.Location())
			continue
		}

		if !s.dayMatches(next) {
			next = time.Date(next.Year(), next.Month(), next.Day()+1, 0, 0, 0, 0, next.Location())
			continue
		}

		if s.hour&(1<<uint(next.Hour())) == 0 {
			next = time.Date(next.Year(), next.Month(), next.Day(), next.Hour()+1, 0, 0, 0, next.Location())
			continue
		}

		if s.minute&(1<<uint(next.Minute())) != 0 {
			return next, nil
		}
		next = next.Add(time.Minute)
	}

	return time.Time{}, fmt.Errorf("no scheduled time found within 5 years for cron schedule %q", s.expression)
}

// dayMatches implements the traditional cron OR semantics: when either the
// day-of-month or day-of-week field is unrestricted, the non-restricted field
// dictates; when both are restricted, a day matches if either field matches.
func (s *Schedule) dayMatches(t time.Time) bool {
	domMatch := s.dom&(1<<uint(t.Day())) != 0
	dowMatch := s.dow&(1<<uint(t.Weekday())) != 0

	if s.domStar || s.dowStar {
		return domMatch && dowMatch
	}
	return domMatch || dowMatch
}

func parseDayOfMonth(field string) (bits uint64, star bool, err error) {
	if field == "*" || field == "?" {
		return (1 << bitSize) - 1, true, nil
	}
	bits, err = parseField(field, [2]uint{1, 31}, "day-of-month", nil)
	return bits, false, err
}

func parseDayOfWeek(field string) (bits uint64, star bool, err error) {
	if field == "*" || field == "?" {
		return (1 << bitSize) - 1, true, nil
	}
	// dow accepts 0-6 (Sunday = 0) and 7 as an alias of Sunday.
	bits, err = parseField(field, [2]uint{0, 7}, "day-of-week", dowNames)
	if err != nil {
		return 0, false, err
	}
	if bits&(1<<7) != 0 {
		bits |= 1 << 0
		bits &^= 1 << 7
	}
	return bits, false, nil
}

// parseField parses one cron field into a bitset over [min, max].
func parseField(field string, r [2]uint, fieldName string, names map[string]uint) (uint64, error) {
	var bits uint64
	for _, part := range strings.Split(field, ",") {
		if part == "" {
			return 0, fmt.Errorf("invalid %s field %q: empty value", fieldName, field)
		}

		// step syntax: range/step
		rangePart := part
		var step uint64 = 1
		if idx := strings.Index(part, "/"); idx >= 0 {
			stepStr := part[idx+1:]
			step64, err := strconv.ParseUint(stepStr, 10, 64)
			if err != nil || step64 == 0 {
				return 0, fmt.Errorf("invalid %s field %q: step %q is not a positive integer", fieldName, field, stepStr)
			}
			step = step64
			rangePart = part[:idx]
			if rangePart == "" {
				return 0, fmt.Errorf("invalid %s field %q: missing range before step", fieldName, field)
			}
		}

		var start, end uint
		switch {
		case rangePart == "*":
			start, end = r[0], r[1]
		case strings.Contains(rangePart, "-"):
			values := strings.Split(rangePart, "-")
			if len(values) != 2 {
				return 0, fmt.Errorf("invalid %s field %q: malformed range %q", fieldName, field, rangePart)
			}
			var err error
			if start, err = parseValue(values[0], r, fieldName, field, names); err != nil {
				return 0, err
			}
			if end, err = parseValue(values[1], r, fieldName, field, names); err != nil {
				return 0, err
			}
		default:
			v, err := parseValue(rangePart, r, fieldName, field, names)
			if err != nil {
				return 0, err
			}
			if strings.Contains(part, "/") {
				// "n/step" means from n up to the upper bound.
				start, end = v, r[1]
			} else {
				start, end = v, v
			}
		}

		if start < r[0] || end > r[1] || start > end {
			return 0, fmt.Errorf("invalid %s field %q: value out of range [%d, %d]", fieldName, field, r[0], r[1])
		}

		for v := start; v <= end; v += uint(step) {
			if v < bitSize {
				bits |= 1 << v
			}
		}
	}

	if bits == 0 {
		return 0, fmt.Errorf("invalid %s field %q: no values matched", fieldName, field)
	}
	return bits, nil
}

func parseValue(token string, r [2]uint, fieldName, field string, names map[string]uint) (uint, error) {
	if v, err := strconv.ParseUint(token, 10, 64); err == nil {
		if uint(v) < r[0] || uint(v) > r[1] {
			return 0, fmt.Errorf("invalid %s field %q: value %s is out of range [%d, %d]", fieldName, field, token, r[0], r[1])
		}
		return uint(v), nil
	}

	if names != nil {
		if v, ok := names[strings.ToLower(token)]; ok {
			return v, nil
		}
	}
	return 0, fmt.Errorf("invalid %s field %q: %q is not a valid value", fieldName, field, token)
}
