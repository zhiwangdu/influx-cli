package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
	Host            string `yaml:"host"`
	Port            *int   `yaml:"port"`
	SSL             *bool  `yaml:"ssl"`
	UnsafeSSL       *bool  `yaml:"unsafeSsl"`
	Username        string `yaml:"username"`
	Password        string `yaml:"password"`
	Token           string `yaml:"token"`
	Database        string `yaml:"database"`
	RetentionPolicy string `yaml:"retention_policy"`
	Precision       string `yaml:"precision"`
}

type IntOverride struct {
	Value int
	Set   bool
}

type BoolOverride struct {
	Value bool
	Set   bool
}

type Overrides struct {
	Profile         string
	Adapter         string
	Host            string
	Port            IntOverride
	SSL             BoolOverride
	UnsafeSSL       BoolOverride
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
	Host            string
	Port            int
	SSL             bool
	UnsafeSSL       bool
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
	if strings.TrimSpace(getenv("INFLUX_CLI_URL")) != "" {
		return Effective{}, errors.New("INFLUX_CLI_URL has been removed; use INFLUX_CLI_HOST, INFLUX_CLI_PORT, and INFLUX_CLI_SSL instead")
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
		Host:            firstNonEmpty(profile.Host, "localhost"),
		Port:            intValue(profile.Port, 8086),
		SSL:             boolValue(profile.SSL, false),
		UnsafeSSL:       boolValue(profile.UnsafeSSL, false),
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

	if err := applyEnv(&effective, getenv); err != nil {
		return Effective{}, err
	}
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
	if _, err := NormalizeHost(e.Host); err != nil {
		return err
	}
	if e.Port < 1 || e.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535, got %d", e.Port)
	}
	if e.UnsafeSSL && !e.SSL {
		return errors.New("unsafeSsl requires ssl; set --ssl when using --unsafeSsl")
	}
	if e.Render == "" {
		return errors.New("render format is required")
	}
	if _, err := format.Normalize(e.Render); err != nil {
		return err
	}
	return nil
}

func NormalizeHost(host string) (string, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		return "", errors.New("host is required")
	}
	if strings.Contains(host, "://") || strings.ContainsAny(host, "/?#") {
		return "", errors.New("host must not include a scheme, path, query, or fragment")
	}
	if strings.HasPrefix(host, "[") || strings.HasSuffix(host, "]") {
		if !strings.HasPrefix(host, "[") || !strings.HasSuffix(host, "]") {
			return "", errors.New("host must not include a port; use --port")
		}
		inner := strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
		if net.ParseIP(inner) == nil {
			return "", fmt.Errorf("invalid bracketed IPv6 host %q", host)
		}
		return inner, nil
	}
	if splitHost, splitPort, err := net.SplitHostPort(host); err == nil && splitHost != "" && splitPort != "" {
		return "", errors.New("host must not include a port; use --port")
	}
	if strings.Contains(host, ":") && net.ParseIP(host) == nil {
		return "", fmt.Errorf("invalid host %q; use --port for the port", host)
	}
	return host, nil
}

func (e Effective) RedactedLines() []string {
	return []string{
		"profile: " + printable(e.Profile),
		"adapter: " + e.Adapter,
		"host: " + printable(e.Host),
		"port: " + fmt.Sprint(e.Port),
		"ssl: " + fmt.Sprint(e.SSL),
		"unsafeSsl: " + fmt.Sprint(e.UnsafeSSL),
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
	if err := rejectLegacyURLField(body, path); err != nil {
		return File{}, false, err
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

func rejectLegacyURLField(body []byte, path string) error {
	var document yaml.Node
	if err := yaml.Unmarshal(body, &document); err != nil {
		return nil
	}
	if len(document.Content) == 0 || document.Content[0].Kind != yaml.MappingNode {
		return nil
	}
	root := document.Content[0]
	for i := 0; i+1 < len(root.Content); i += 2 {
		key := root.Content[i]
		value := root.Content[i+1]
		if key.Value != "profiles" || value.Kind != yaml.MappingNode {
			continue
		}
		for j := 0; j+1 < len(value.Content); j += 2 {
			profileName := value.Content[j].Value
			profileValue := value.Content[j+1]
			if profileValue.Kind != yaml.MappingNode {
				continue
			}
			for k := 0; k+1 < len(profileValue.Content); k += 2 {
				if profileValue.Content[k].Value == "url" {
					return fmt.Errorf("config %s profile %q uses removed field \"url\"; use host, port, ssl, and unsafeSsl instead", path, profileName)
				}
			}
		}
	}
	return nil
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

func applyEnv(e *Effective, getenv EnvGetter) error {
	applyString(&e.Adapter, getenv("INFLUX_CLI_ADAPTER"))
	applyString(&e.Host, getenv("INFLUX_CLI_HOST"))
	if err := applyEnvInt(&e.Port, "INFLUX_CLI_PORT", getenv); err != nil {
		return err
	}
	if err := applyEnvBool(&e.SSL, "INFLUX_CLI_SSL", getenv); err != nil {
		return err
	}
	if err := applyEnvBool(&e.UnsafeSSL, "INFLUX_CLI_UNSAFE_SSL", getenv); err != nil {
		return err
	}
	applyString(&e.Username, getenv("INFLUX_CLI_USERNAME"))
	applyString(&e.Password, getenv("INFLUX_CLI_PASSWORD"))
	applyString(&e.Token, getenv("INFLUX_CLI_TOKEN"))
	applyString(&e.Database, getenv("INFLUX_CLI_DB"))
	applyString(&e.RetentionPolicy, getenv("INFLUX_CLI_RP"))
	applyString(&e.Precision, getenv("INFLUX_CLI_PRECISION"))
	applyString(&e.Render, getenv("INFLUX_CLI_RENDER"))
	return nil
}

func applyOverrides(e *Effective, overrides Overrides) {
	applyString(&e.Adapter, overrides.Adapter)
	applyString(&e.Host, overrides.Host)
	if overrides.Port.Set {
		e.Port = overrides.Port.Value
	}
	if overrides.SSL.Set {
		e.SSL = overrides.SSL.Value
	}
	if overrides.UnsafeSSL.Set {
		e.UnsafeSSL = overrides.UnsafeSSL.Value
	}
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

func applyEnvInt(target *int, name string, getenv EnvGetter) error {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("parse %s %q: %w", name, value, err)
	}
	*target = parsed
	return nil
}

func applyEnvBool(target *bool, name string, getenv EnvGetter) error {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return nil
	}
	parsed, err := parseBool(value)
	if err != nil {
		return fmt.Errorf("parse %s %q: %w", name, value, err)
	}
	*target = parsed
	return nil
}

func parseBool(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "t", "true", "y", "yes", "on":
		return true, nil
	case "0", "f", "false", "n", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("expected true or false")
	}
}

func intValue(value *int, fallback int) int {
	if value == nil {
		return fallback
	}
	return *value
}

func boolValue(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
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
