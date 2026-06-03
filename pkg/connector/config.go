package connector

import (
	_ "embed"
	"strings"
	"text/template"
	"time"
	"unicode"

	up "go.mau.fi/util/configupgrade"
	"gopkg.in/yaml.v3"

	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
)

const (
	defaultConversationSyncLimit = 50
	defaultRequestTimeoutSeconds = 30
	defaultUserAgent             = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
	maxConfigDurationSeconds     = int64(1<<63-1) / int64(time.Second)
	maxConfigDuration            = time.Duration(maxConfigDurationSeconds) * time.Second
	maxDisplayNameRunes          = 100
	displayNameTruncation        = "..."
)

//go:embed example-config.yaml
var ExampleConfig string

type Config struct {
	DisplaynameTemplate   string `yaml:"displayname_template"`
	ConversationSyncLimit int    `yaml:"conversation_sync_limit"`
	RequestTimeoutSeconds int    `yaml:"request_timeout_seconds"`
	UserAgent             string `yaml:"user_agent"`

	displaynameTemplate *template.Template `yaml:"-"`
}

type rawConfig Config

func (c *Config) UnmarshalYAML(node *yaml.Node) error {
	err := node.Decode((*rawConfig)(c))
	if err != nil {
		return err
	}
	return c.PostProcess()
}

func (c *Config) PostProcess() error {
	c.ConversationSyncLimit = c.ConversationSyncBatchLimit()
	if c.RequestTimeoutSeconds <= 0 {
		c.RequestTimeoutSeconds = defaultRequestTimeoutSeconds
	}
	c.UserAgent = cleanConfiguredUserAgent(c.UserAgent)
	tmpl := strings.TrimSpace(c.DisplaynameTemplate)
	if tmpl == "" {
		tmpl = "{{ if .DisplayName }}{{ .DisplayName }}{{ else }}{{ .Username }}{{ end }}"
	}
	parsed, err := template.New("displayname").Parse(tmpl)
	if err != nil {
		return err
	}
	c.DisplaynameTemplate = tmpl
	c.displaynameTemplate = parsed
	return nil
}

func upgradeConfig(helper up.Helper) {
	helper.Copy(up.Str, "displayname_template")
	helper.Copy(up.Int, "conversation_sync_limit")
	helper.Copy(up.Int, "request_timeout_seconds")
	helper.Copy(up.Str, "user_agent")
}

func (c Config) BrowserUserAgent() string {
	if err := c.PostProcess(); err != nil {
		return defaultUserAgent
	}
	return c.UserAgent
}

func cleanConfiguredUserAgent(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return defaultUserAgent
	}
	return value
}

func (c *Config) ConversationSyncBatchLimit() int {
	if c == nil || c.ConversationSyncLimit <= 0 {
		return defaultConversationSyncLimit
	}
	if c.ConversationSyncLimit > tumblr.MaxRequestLimit {
		return tumblr.MaxRequestLimit
	}
	return c.ConversationSyncLimit
}

func (c *Config) RequestTimeout() time.Duration {
	if c == nil {
		return defaultRequestTimeoutSeconds * time.Second
	}
	return boundedConfigDuration(c.RequestTimeoutSeconds, defaultRequestTimeoutSeconds*time.Second)
}

func boundedConfigDuration(seconds int, fallback time.Duration) time.Duration {
	if seconds <= 0 {
		return fallback
	}
	if int64(seconds) > maxConfigDurationSeconds {
		return maxConfigDuration
	}
	return time.Duration(seconds) * time.Second
}

type DisplaynameParams struct {
	Username    string
	DisplayName string
}

func (c *Config) FormatDisplayname(username, displayName string) string {
	username = cleanDisplayName(username)
	displayName = cleanDisplayName(displayName)
	if c.displaynameTemplate == nil {
		if err := c.PostProcess(); err != nil {
			return fallbackDisplayname(username, displayName)
		}
	}
	var buf strings.Builder
	err := c.displaynameTemplate.Execute(&buf, DisplaynameParams{
		Username:    username,
		DisplayName: displayName,
	})
	if err != nil {
		return fallbackDisplayname(username, displayName)
	}
	formatted := cleanDisplayName(buf.String())
	if formatted == "" {
		return fallbackDisplayname(username, displayName)
	}
	return formatted
}

func fallbackDisplayname(username, displayName string) string {
	displayName = cleanDisplayName(displayName)
	if displayName != "" {
		return displayName
	}
	return cleanDisplayName(username)
}

func cleanDisplayName(value string) string {
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	})
	normalized := strings.Join(fields, " ")
	if normalized == "" {
		return ""
	}
	runes := []rune(normalized)
	if len(runes) <= maxDisplayNameRunes {
		return normalized
	}
	return string(runes[:maxDisplayNameRunes]) + displayNameTruncation
}

func (tc *TumblrConnector) GetConfig() (string, any, up.Upgrader) {
	return ExampleConfig, &tc.Config, &up.StructUpgrader{
		SimpleUpgrader: up.SimpleUpgrader(upgradeConfig),
		Base:           ExampleConfig,
	}
}
