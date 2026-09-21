package config

import (
	"encoding/json"
	"errors"
	"time"
)

// Duration is a time.Duration that unmarshals from either a string
// like "200ms"/"1.5s" OR a JSON number interpreted as nanoseconds.
// This keeps old numeric configs working while allowing readable
// duration strings; any other form is rejected.
type Duration time.Duration

// Std converts to time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		parsed, perr := time.ParseDuration(s)
		if perr != nil {
			return perr
		}
		*d = Duration(parsed)
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err == nil {
		if n < 0 {
			return errors.New("duration must be >= 0")
		}
		*d = Duration(n)
		return nil
	}
	return errors.New("duration must be a number (nanoseconds) or a string like \"200ms\"")
}

// MarshalJSON emits the human-readable string form.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}
