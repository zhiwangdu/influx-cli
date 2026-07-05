package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/format"
	"gopkg.in/yaml.v3"
)

type File struct {
	Profiles map[string]Profile `yaml:"profiles"`
	Defaults Defaults           `yaml:"defaults"`
}

type Defaults struct {
	Profile string `yaml:"profile"`
	Render  string `yaml:"render"`
	Timeout string `yaml:"timeout"`
}

type Profile struct {
	Adapter         string `yaml:"adapter"`
	URL             string `yaml:"url"`
	Username        string `yaml:"username"`
	Password        string `yaml:"password"`
	Token           string `yaml:"token"`
	Database        string `yaml:"database"`
	RetentionPolicy string `yaml:"retention_policy"`
	Precision       string `yaml:"precision"`
}

type Overrides struct {
	Profile         string
	Adapter         string
	URL             string
	Username        string
	Password        string
	Token           string
	Database        string
	RetentionPolicy string
	Precision       string
	Render          string
	Timeout         string
}

type Effective struct {
	Profile         string
	Adapter         string
	URL             string
	Username        string
	Password        string
	Token           string
	Database        string
	RetentionPolicy string
	Precision       string
	Render          string
	Timeout         time.Duration
	ConfigPath      string
	ConfigFound     bool
}

type EnvGetter func(string) string

func DefaultPath() string {
	configDir, err := os.UserConfigDir()
	if err != nil || configDir == "" {
		home, homeErr := os.UserHomeDir()
		if homeErr != nil || home == "" {
			return "config.yaml"
		}
		configDir = filepath.Join(home, ".config")
	}
	return filepath.Join(configDir, "influx-cli", "config.yaml")
}

func Resolve(path string, overrides Overrides, getenv EnvGetter) (Effective, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	if path == "" {
		path = DefaultPath()
	}

	file, found, err := loadFile(path)
	if err != nil {
		return Effective{}, err
	}

	profileName := firstNonEmpty(overrides.Profile, getenv("INFLUX_CLI_PROFILE"), file.Defaults.Profile)
	profile, hasProfile := file.Profiles[profileName]
	if profileName == "" {
		profileName, profile, hasProfile = selectDefaultProfile(file.Profiles)
	}
	if !hasProfile && profileName != "" && found {
		return Effective{}, fmt.Errorf("profile %q not found in %s", profileName, path)
	}

	effective := Effective{
		Profile:         profileName,
		Adapter:         firstNonEmpty(profile.Adapter, "influxdb"),
		URL:             firstNonEmpty(profile.URL, "http://127.0.0.1:8086"),
		Username:        profile.Username,
		Password:        profile.Password,
		Token:           profile.Token,
		Database:        profile.Database,
		RetentionPolicy: profile.RetentionPolicy,
		Precision:       firstNonEmpty(profile.Precision, "rfc3339"),
		Render:          firstNonEmpty(file.Defaults.Render, "table"),
		ConfigPath:      path,
		ConfigFound:     found,
	}

	applyEnv(&effective, getenv)
	applyOverrides(&effective, overrides)

	timeoutRaw := firstNonEmpty(overrides.Timeout, getenv("INFLUX_CLI_TIMEOUT"), file.Defaults.Timeout, "10s")
	timeout, err := time.ParseDuration(timeoutRaw)
	if err != nil {
		return Effective{}, fmt.Errorf("parse timeout %q: %w", timeoutRaw, err)
	}
	if timeout <= 0 {
		return Effective{}, errors.New("timeout must be greater than zero")
	}
	effective.Timeout = timeout

	if err := effective.Validate(); err != nil {
		return Effective{}, err
	}
	return effective, nil
}

func (e Effective) Validate() error {
	if strings.TrimSpace(e.Adapter) == "" {
		return errors.New("adapter is required")
	}
	parsed, err := url.Parse(e.URL)
	if err != nil {
		return fmt.Errorf("parse URL: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("URL must include scheme and host")
	}
	if e.Render == "" {
		return errors.New("render format is required")
	}
	if _, err := format.Normalize(e.Render); err != nil {
		return err
	}
	return nil
}

func (e Effective) RedactedLines() []string {
	return []string{
		"profile: " + printable(e.Profile),
		"adapter: " + e.Adapter,
		"url: " + e.URL,
		"username: " + printable(e.Username),
		"password: " + redact(e.Password),
		"token: " + redact(e.Token),
		"database: " + printable(e.Database),
		"retention_policy: " + printable(e.RetentionPolicy),
		"precision: " + printable(e.Precision),
		"render: " + printable(e.Render),
		"timeout: " + e.Timeout.String(),
		"config_path: " + e.ConfigPath,
		"config_found: " + fmt.Sprint(e.ConfigFound),
	}
}

func loadFile(path string) (File, bool, error) {
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return File{Profiles: map[string]Profile{}}, false, nil
	}
	if err != nil {
		return File{}, false, fmt.Errorf("read config %s: %w", path, err)
	}
	var file File
	if err := yaml.Unmarshal(body, &file); err != nil {
		return File{}, false, fmt.Errorf("parse config %s: %w", path, err)
	}
	if file.Profiles == nil {
		file.Profiles = map[string]Profile{}
	}
	return file, true, nil
}

func selectDefaultProfile(profiles map[string]Profile) (string, Profile, bool) {
	if len(profiles) == 0 {
		return "", Profile{}, false
	}
	if profile, ok := profiles["local"]; ok {
		return "local", profile, true
	}
	if len(profiles) == 1 {
		for name, profile := range profiles {
			return name, profile, true
		}
	}
	names := make([]string, 0, len(profiles))
	for name := range profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	name := names[0]
	return name, profiles[name], true
}

func applyEnv(e *Effective, getenv EnvGetter) {
	applyString(&e.Adapter, getenv("INFLUX_CLI_ADAPTER"))
	applyString(&e.URL, getenv("INFLUX_CLI_URL"))
	applyString(&e.Username, getenv("INFLUX_CLI_USERNAME"))
	applyString(&e.Password, getenv("INFLUX_CLI_PASSWORD"))
	applyString(&e.Token, getenv("INFLUX_CLI_TOKEN"))
	applyString(&e.Database, getenv("INFLUX_CLI_DB"))
	applyString(&e.RetentionPolicy, getenv("INFLUX_CLI_RP"))
	applyString(&e.Precision, getenv("INFLUX_CLI_PRECISION"))
	applyString(&e.Render, getenv("INFLUX_CLI_RENDER"))
}

func applyOverrides(e *Effective, overrides Overrides) {
	applyString(&e.Adapter, overrides.Adapter)
	applyString(&e.URL, overrides.URL)
	applyString(&e.Username, overrides.Username)
	applyString(&e.Password, overrides.Password)
	applyString(&e.Token, overrides.Token)
	applyString(&e.Database, overrides.Database)
	applyString(&e.RetentionPolicy, overrides.RetentionPolicy)
	applyString(&e.Precision, overrides.Precision)
	applyString(&e.Render, overrides.Render)
}

func applyString(target *string, value string) {
	if value != "" {
		*target = value
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func printable(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func redact(value string) string {
	if value == "" {
		return "-"
	}
	return "<redacted>"
}
