package apis

import (
	"fmt"
	"strings"

	"github.com/SUDOKU-ASCII/sudoku/pkg/obfs/sudoku"
)

const (
	// DefaultKey is a deterministic shared key for the appearance table.
	// Override it when the client and server should use a private table.
	DefaultKey = "2d8f2c94-3fd0-4a57-8d18-61f33aee2c44"

	// DefaultASCII is the default Sudoku byte layout preference.
	DefaultASCII = "prefer_entropy"
)

// Config describes only the Sudoku appearance layer.
type Config struct {
	// Key seeds the Sudoku table. Both sides must use the same key.
	Key string

	// ASCII selects the byte layout: "prefer_entropy", "prefer_ascii", or a
	// directional value such as "up_ascii_down_entropy".
	ASCII string

	// CustomTables contains optional X/P/V byte-layout patterns.
	// With no handshake in this raw codec API, both sides must use the same TableIndex.
	CustomTables []string

	// TableIndex selects which configured table to use. The default table is index 0.
	TableIndex int

	// EnablePureDownlink keeps server-to-client traffic in classic Sudoku mode.
	// The default is false, which uses packed downlink.
	EnablePureDownlink bool

	// PaddingMin and PaddingMax are optional padding probabilities in percent.
	PaddingMin int
	PaddingMax int
}

// DefaultConfig returns the default raw Sudoku appearance config.
func DefaultConfig() *Config {
	return &Config{
		Key:   DefaultKey,
		ASCII: DefaultASCII,
	}
}

// Normalize returns a copy of cfg with defaults and canonical mode names applied.
func Normalize(cfg *Config) (Config, error) {
	out := Config{}
	if cfg != nil {
		out = *cfg
		out.CustomTables = append([]string(nil), cfg.CustomTables...)
	}
	if strings.TrimSpace(out.Key) == "" {
		out.Key = DefaultKey
	}
	if strings.TrimSpace(out.ASCII) == "" {
		out.ASCII = DefaultASCII
	}
	ascii, err := sudoku.NormalizeASCIIMode(out.ASCII)
	if err != nil {
		return Config{}, err
	}
	out.ASCII = ascii
	return out, nil
}

// Validate checks the normalized config and custom table patterns.
func (c *Config) Validate() error {
	_, err := BuildTables(c)
	return err
}

// BuildTables builds every configured table candidate.
func BuildTables(cfg *Config) ([]*sudoku.Table, error) {
	c, err := Normalize(cfg)
	if err != nil {
		return nil, err
	}
	if err := validatePadding(c); err != nil {
		return nil, err
	}
	if c.TableIndex < 0 {
		return nil, fmt.Errorf("TableIndex must be >= 0, got %d", c.TableIndex)
	}
	ts, err := sudoku.NewTableSet(c.Key, c.ASCII, c.CustomTables)
	if err != nil {
		return nil, err
	}
	if ts == nil || len(ts.Tables) == 0 {
		return nil, fmt.Errorf("no sudoku tables configured")
	}
	if c.TableIndex >= len(ts.Tables) {
		return nil, fmt.Errorf("TableIndex %d out of range for %d table(s)", c.TableIndex, len(ts.Tables))
	}
	return ts.Tables, nil
}

// BuildTables builds every configured table candidate.
func (c *Config) BuildTables() ([]*sudoku.Table, error) {
	return BuildTables(c)
}

func selectedTable(cfg *Config) (Config, *sudoku.Table, error) {
	c, err := Normalize(cfg)
	if err != nil {
		return Config{}, nil, err
	}
	tables, err := BuildTables(&c)
	if err != nil {
		return Config{}, nil, err
	}
	return c, tables[c.TableIndex], nil
}

func validatePadding(c Config) error {
	if c.PaddingMin < 0 || c.PaddingMin > 100 {
		return fmt.Errorf("PaddingMin must be between 0 and 100, got %d", c.PaddingMin)
	}
	if c.PaddingMax < 0 || c.PaddingMax > 100 {
		return fmt.Errorf("PaddingMax must be between 0 and 100, got %d", c.PaddingMax)
	}
	if c.PaddingMax < c.PaddingMin {
		return fmt.Errorf("PaddingMax (%d) must be >= PaddingMin (%d)", c.PaddingMax, c.PaddingMin)
	}
	return nil
}
