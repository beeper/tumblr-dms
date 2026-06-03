package tumblr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/netip"
	"net/textproto"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	DefaultWebBaseURL       = "https://www.tumblr.com"
	DefaultAPIBaseURL       = "https://www.tumblr.com/api"
	MaxMessageTextRunes     = 4096
	MaxRequestLimit         = 100
	defaultUserAgent        = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
	DefaultMaxDownloadBytes = 5 * 1024 * 1024
	DefaultMaxUploadBytes   = DefaultMaxDownloadBytes
	maxBootstrapPageBytes   = 4 * 1024 * 1024
	maxAPIResponseBytes     = 8 * 1024 * 1024
	maxIdentifierRunes      = 512

	blogFields             = "?avatar,name,?title,url,?blog_view_url,?can_message,?description,?is_adult,?uuid,?is_private_channel,?posts,?is_group_channel,?primary,?admin,?drafts,?followers,?queue,?has_flagged_posts,?messages,?ask,?can_submit,?mention_key,?timezone_offset,?analytics_url,?is_premium_partner,?is_blogless_advertiser,?is_tumblrpay_onboarded,?theme,?tumblrmart_orders"
	conversationBlogFields = "?avatar,name,?seconds_since_last_activity,url,?blog_view_url,?uuid,?theme,?description_npf,?is_adult,?primary"
	suggestionBlogFields   = "avatar,title,name,theme,url,blogViewUrl,isAdult,uuid"
)

var (
	apiURLRe    = regexp.MustCompile(`"apiUrl"\s*:\s*"([^"]+)"`)
	apiTokenRe  = regexp.MustCompile(`"API_TOKEN"\s*:\s*"([^"]+)"`)
	csrfTokenRe = regexp.MustCompile(`"csrfToken"\s*:\s*"([^"]*)"`)
	loggedOutRe = regexp.MustCompile(`"isLoggedIn"\s*:\s*false`)
)

var apiTransientRetryDelays = []time.Duration{
	500 * time.Millisecond,
	1250 * time.Millisecond,
}

type Options struct {
	WebBaseURL   string
	APIBaseURL   string
	UserAgent    string
	CookieHeader string
	APIToken     string
	CSRFToken    string
	APIVersion   string
	HTTPClient   *http.Client
}

type Client struct {
	mu           sync.RWMutex
	webBaseURL   string
	apiBaseURL   string
	userAgent    string
	cookieHeader string
	apiToken     string
	csrfToken    string
	apiVersion   string
	httpClient   *http.Client
}

type ImageUpload struct {
	Data        []byte
	FileName    string
	ContentType string
}

func NewClient(opts Options) *Client {
	webBaseURL := normalizeConfiguredBaseURL(opts.WebBaseURL, DefaultWebBaseURL)
	apiBaseURL := normalizeConfiguredBaseURL(opts.APIBaseURL, DefaultAPIBaseURL)
	userAgent := normalizeUserAgent(opts.UserAgent)
	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{
		webBaseURL:   webBaseURL,
		apiBaseURL:   apiBaseURL,
		userAgent:    userAgent,
		cookieHeader: normalizeClientCookieHeader(opts.CookieHeader),
		apiToken:     normalizeBearerToken(opts.APIToken),
		csrfToken:    normalizeOptionalHeaderCredential(opts.CSRFToken),
		apiVersion:   normalizeOptionalHeaderCredential(opts.APIVersion),
		httpClient:   httpClient,
	}
}

func normalizeUserAgent(input string) string {
	input = strings.TrimSpace(input)
	if input == "" || containsHTTPHeaderControl(input) {
		return defaultUserAgent
	}
	return input
}

func normalizeConfiguredBaseURL(rawURL, fallback string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return fallback
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fallback
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fallback
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	parsed.Opaque = ""
	normalized := strings.TrimRight(parsed.String(), "/")
	if normalized == "" {
		return fallback
	}
	return normalized
}

func CookieHeaderFromMap(cookies map[string]string) string {
	header := normalizeCookieHeader(cookies["cookie_header"])
	existingNames := cookieHeaderNames(header)
	keys := make([]string, 0, len(cookies))
	for key := range cookies {
		if key == "cookie_header" || key == "api_token" || key == "csrf_token" || key == "api_version" {
			continue
		}
		if _, ok := existingNames[key]; ok {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	if header != "" {
		parts = append(parts, header)
	}
	for _, key := range keys {
		value := strings.TrimSpace(cookies[key])
		if !validCookieMapPair(key, value) {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s", key, value))
	}
	return strings.Join(parts, "; ")
}

func cookieHeaderNames(header string) map[string]struct{} {
	names := make(map[string]struct{})
	for _, part := range strings.Split(header, ";") {
		name, _, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		if name != "" {
			names[name] = struct{}{}
		}
	}
	return names
}

func normalizeCookieHeader(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return ""
	}
	if value := HeaderValueFromText(input, "cookie"); value != "" {
		if containsHTTPHeaderControl(value) {
			return ""
		}
		return value
	}
	if containsHTTPHeaderControl(input) {
		return ""
	}
	return input
}

func normalizeClientCookieHeader(input string) string {
	header := normalizeCookieHeader(input)
	if !CookieHeaderHasPair(header) {
		return ""
	}
	return header
}

func CookieHeaderHasPair(header string) bool {
	header = normalizeCookieHeader(header)
	if header == "" {
		return false
	}
	for _, part := range strings.Split(header, ";") {
		part = strings.TrimSpace(part)
		if idx := strings.Index(part, "="); idx > 0 && idx < len(part)-1 {
			return true
		}
	}
	return false
}

func validCookieMapPair(key, value string) bool {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" {
		return false
	}
	if strings.ContainsAny(key, "=;") || strings.IndexFunc(key, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) >= 0 {
		return false
	}
	return !strings.Contains(value, ";") && !containsHTTPHeaderControl(value)
}

func containsHTTPHeaderControl(value string) bool {
	return strings.IndexFunc(value, unicode.IsControl) >= 0
}

func normalizeBearerToken(input string) string {
	input = normalizeOptionalHeaderCredential(input)
	if input == "" {
		return ""
	}
	if len(input) < len("bearer") || !strings.EqualFold(input[:len("bearer")], "bearer") {
		return input
	}
	remainder := strings.TrimSpace(input[len("bearer"):])
	if remainder == "" || remainder == input[len("bearer"):] {
		return input
	}
	return normalizeOptionalHeaderCredential(remainder)
}

func normalizeOptionalHeaderCredential(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || containsHTTPHeaderControl(value) {
		return ""
	}
	return value
}

func normalizeHeaderLine(line string) string {
	line = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), `\`))
	for _, prefix := range []string{"-H", "--header"} {
		if strings.HasPrefix(line, prefix+" ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, prefix))
			break
		}
	}
	line = strings.TrimPrefix(line, "$")
	return strings.Trim(strings.TrimSpace(line), `"'`)
}

func headerValue(line, headerName string) (string, bool) {
	name, value, ok := strings.Cut(line, ":")
	if !ok || !strings.EqualFold(strings.TrimSpace(name), headerName) {
		return "", false
	}
	value = strings.TrimSpace(strings.TrimSuffix(value, `\`))
	return value, value != ""
}

func HeaderValueFromText(input, headerName string) string {
	input = strings.TrimSpace(input)
	headerName = strings.TrimSpace(headerName)
	if input == "" || headerName == "" {
		return ""
	}
	for _, line := range strings.Split(input, "\n") {
		line = normalizeHeaderLine(line)
		if value, ok := headerValue(line, headerName); ok {
			return value
		}
	}
	if value, ok := headerValue(input, headerName); ok {
		return value
	}
	return inlineHeaderValue(input, headerName)
}

func inlineHeaderValue(input, headerName string) string {
	needle := strings.ToLower(headerName) + ":"
	lowerInput := strings.ToLower(input)
	searchStart := 0
	for {
		idx := strings.Index(lowerInput[searchStart:], needle)
		if idx < 0 {
			return ""
		}
		idx += searchStart
		if idx == 0 || isInlineHeaderBoundary(input[idx-1]) {
			value := input[idx+len(needle):]
			if end := strings.IndexAny(value, "'\"\r\n"); end >= 0 {
				value = value[:end]
			}
			return strings.TrimSpace(strings.TrimSuffix(value, `\`))
		}
		searchStart = idx + len(needle)
	}
}

func isInlineHeaderBoundary(previous byte) bool {
	return previous == ' ' || previous == '\t' || previous == '\r' || previous == '\n' || previous == '\'' || previous == '"'
}

func NormalizeBlogName(input string) string {
	normalized := strings.TrimSpace(input)
	normalized = strings.TrimPrefix(normalized, "@")
	if normalized == "" {
		return ""
	}
	if parsed, ok := parseBlogURL(normalized); ok {
		if parsed.User != nil {
			return ""
		}
		host := strings.TrimPrefix(strings.ToLower(parsed.Hostname()), "www.")
		if host == "tumblr.com" {
			if hasEscapedPathSeparator(parsed) {
				return ""
			}
			return cleanBlogName(blogNameFromTumblrPath(parsed.Path))
		}
		if strings.HasSuffix(host, ".tumblr.com") {
			return cleanBlogName(strings.TrimSuffix(host, ".tumblr.com"))
		}
		return ""
	}
	if hasInvalidExplicitBlogURLScheme(normalized) {
		return ""
	}
	return cleanBlogName(normalized)
}

func hasEscapedPathSeparator(parsed *url.URL) bool {
	escapedPath := strings.ToLower(parsed.EscapedPath())
	return strings.Contains(escapedPath, "%2f") || strings.Contains(escapedPath, "%5c")
}

func hasInvalidExplicitBlogURLScheme(input string) bool {
	parsed, err := url.Parse(input)
	if err != nil || parsed.Scheme == "" {
		return false
	}
	return (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == ""
}

func parseBlogURL(input string) (*url.URL, bool) {
	parsed, err := url.Parse(input)
	if err == nil && parsed.Host != "" {
		if parsed.Scheme == "http" || parsed.Scheme == "https" || parsed.Scheme == "" {
			return parsed, true
		}
		return nil, false
	}
	firstPart := input
	if slash := strings.IndexByte(firstPart, '/'); slash >= 0 {
		firstPart = firstPart[:slash]
	}
	hostPart := strings.TrimPrefix(strings.ToLower(firstPart), "www.")
	if strings.Contains(hostPart, ":") {
		if host, _, err := net.SplitHostPort(hostPart); err == nil {
			hostPart = strings.TrimPrefix(strings.ToLower(host), "www.")
		}
	}
	if hostPart == "tumblr.com" || strings.HasSuffix(hostPart, ".tumblr.com") {
		parsed, err = url.Parse("https://" + input)
		return parsed, err == nil && parsed.Host != ""
	}
	return nil, false
}

func blogNameFromTumblrPath(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return ""
	}
	switch strings.ToLower(parts[0]) {
	case "blog":
		if len(parts) < 2 {
			return ""
		}
		if strings.EqualFold(parts[1], "view") {
			if len(parts) >= 3 {
				return parts[2]
			}
			return ""
		}
		return parts[1]
	case "dashboard":
		if len(parts) >= 3 && strings.EqualFold(parts[1], "blog") {
			return parts[2]
		}
		return ""
	}
	if isReservedTumblrPathSegment(parts[0]) {
		return ""
	}
	return parts[0]
}

func isReservedTumblrPathSegment(segment string) bool {
	switch strings.ToLower(segment) {
	case "activity",
		"dashboard",
		"explore",
		"inbox",
		"likes",
		"login",
		"messaging",
		"new",
		"register",
		"search",
		"settings",
		"tagged":
		return true
	default:
		return false
	}
}

func cleanBlogName(input string) string {
	normalized := strings.TrimSpace(input)
	if normalized == "" {
		return ""
	}
	if strings.ContainsAny(normalized, "/?#\\") {
		return ""
	}
	normalized = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(normalized)), "www.")
	normalized = strings.TrimSuffix(normalized, ".tumblr.com")
	if strings.Contains(normalized, ".") {
		return ""
	}
	if strings.Contains(normalized, ":") {
		return ""
	}
	if containsSpaceOrControl(normalized) {
		return ""
	}
	return normalized
}

func containsSpaceOrControl(value string) bool {
	return strings.IndexFunc(value, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) >= 0
}

func (c *Client) CookieHeader() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cookieHeader
}

func (c *Client) APIToken() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.apiToken
}

func (c *Client) CSRFToken() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.csrfToken
}

func (c *Client) APIVersion() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.apiVersion
}

func (c *Client) needsBootstrap(mutating bool) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.apiToken == "" || (mutating && c.csrfToken == "")
}

func (c *Client) hasCSRFToken() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.csrfToken != ""
}

func (c *Client) Bootstrap(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.webBaseURL+"/messaging", nil)
	if err != nil {
		return err
	}
	c.setBrowserHeaders(req)
	resp, err := c.bootstrapHTTPClient().Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return &BootstrapError{Message: "Tumblr messaging page request failed"}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &BootstrapError{
			Message: fmt.Sprintf("failed to load Tumblr messaging page: HTTP %d", resp.StatusCode),
			Auth:    resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden,
		}
	}
	if isLoginPath(resp.Request.URL.Path) {
		return &BootstrapError{Message: "Tumblr session is not logged in", Auth: true}
	}
	body, err := readLimitedBody(resp.Body, maxBootstrapPageBytes, "Tumblr messaging page")
	if err != nil {
		return err
	}
	if loggedOutRe.Match(body) {
		return &BootstrapError{Message: "Tumblr session is not logged in", Auth: true}
	}
	return c.bootstrapFromHTML(string(body))
}

func isLoginPath(rawPath string) bool {
	trimmed := strings.TrimRight(rawPath, "/")
	return trimmed == "/login" || strings.HasPrefix(trimmed, "/login/")
}

func (c *Client) bootstrapFromHTML(html string) error {
	apiURL := unescapeBootstrapValue(firstSubmatch(apiURLRe, html))
	rawAPIToken := unescapeBootstrapValue(firstSubmatch(apiTokenRe, html))
	apiToken := normalizeBearerToken(rawAPIToken)
	csrfToken := normalizeOptionalHeaderCredential(unescapeBootstrapValue(firstSubmatch(csrfTokenRe, html)))
	apiBaseURL := ""
	if apiURL != "" {
		normalized, err := normalizeBootstrapAPIURL(apiURL, c.webBaseURL)
		if err != nil {
			return err
		}
		apiBaseURL = normalized
	}
	if apiToken == "" {
		if strings.TrimSpace(rawAPIToken) != "" {
			return &BootstrapError{Message: "Tumblr messaging page included an invalid API token"}
		}
		return &BootstrapError{Message: "Tumblr messaging page did not include an API token"}
	}
	c.mu.Lock()
	if apiBaseURL != "" {
		c.apiBaseURL = apiBaseURL
	}
	c.apiToken = apiToken
	c.csrfToken = csrfToken
	c.mu.Unlock()
	return nil
}

func normalizeBootstrapAPIURL(rawAPIURL, webBaseURL string) (string, error) {
	webURL, err := url.Parse(strings.TrimRight(webBaseURL, "/"))
	if err != nil || webURL.Scheme == "" || webURL.Host == "" {
		return "", &BootstrapError{Message: "Tumblr messaging page included an invalid API URL"}
	}
	parsed, err := url.Parse(strings.TrimSpace(rawAPIURL))
	if err != nil {
		return "", &BootstrapError{Message: "Tumblr messaging page included an invalid API URL"}
	}
	parsed = webURL.ResolveReference(parsed)
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return "", &BootstrapError{Message: "Tumblr messaging page included an invalid API URL"}
	}
	if isSameOrigin(parsed, webURL) || isTrustedTumblrAPIOrigin(parsed) {
		return strings.TrimRight(parsed.String(), "/"), nil
	}
	return "", &BootstrapError{Message: "Tumblr messaging page included an unsafe API URL"}
}

func isSameOrigin(parsed, webURL *url.URL) bool {
	return parsed.Scheme == webURL.Scheme && strings.EqualFold(parsed.Host, webURL.Host)
}

func isTrustedTumblrAPIOrigin(parsed *url.URL) bool {
	if parsed.Scheme != "https" {
		return false
	}
	if port := parsed.Port(); port != "" && port != "443" {
		return false
	}
	switch strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".") {
	case "tumblr.com", "www.tumblr.com", "api.tumblr.com":
		return true
	default:
		return false
	}
}

func (c *Client) bootstrapHTTPClient() *http.Client {
	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	clone := *httpClient
	existingCheckRedirect := clone.CheckRedirect
	webBaseURL := c.webBaseURL
	clone.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if req == nil || req.URL == nil {
			return fmt.Errorf("tumblr web redirect URL is invalid")
		}
		if !isAllowedBootstrapRedirectURL(req.URL, webBaseURL) {
			return fmt.Errorf("tumblr web redirect URL is not allowed")
		}
		if existingCheckRedirect != nil {
			return existingCheckRedirect(req, via)
		}
		if len(via) >= 10 {
			return fmt.Errorf("stopped after 10 redirects")
		}
		return nil
	}
	return &clone
}

func isAllowedBootstrapRedirectURL(redirectURL *url.URL, webBaseURL string) bool {
	if redirectURL == nil || redirectURL.Scheme == "" || redirectURL.Host == "" || redirectURL.User != nil {
		return false
	}
	webURL, err := url.Parse(strings.TrimRight(webBaseURL, "/"))
	if err == nil && webURL.Scheme != "" && webURL.Host != "" && isSameOrigin(redirectURL, webURL) {
		return true
	}
	return isTrustedTumblrWebOrigin(redirectURL)
}

func isTrustedTumblrWebOrigin(parsed *url.URL) bool {
	if parsed.Scheme != "https" {
		return false
	}
	if port := parsed.Port(); port != "" && port != "443" {
		return false
	}
	switch strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".") {
	case "tumblr.com", "www.tumblr.com":
		return true
	default:
		return false
	}
}

func firstSubmatch(re *regexp.Regexp, input string) string {
	match := re.FindStringSubmatch(input)
	if len(match) < 2 {
		return ""
	}
	return match[1]
}

func unescapeBootstrapValue(value string) string {
	if value == "" {
		return ""
	}
	if unquoted, err := strconv.Unquote(`"` + value + `"`); err == nil {
		return unquoted
	}
	value = strings.ReplaceAll(value, `\/`, `/`)
	value = strings.ReplaceAll(value, `\u0026`, "&")
	return value
}

func (c *Client) CurrentUser(ctx context.Context) (*UserInfoResponse, error) {
	var response UserInfoResponse
	err := c.do(ctx, http.MethodGet, "/v2/user/info", url.Values{
		"fields[blogs]": []string{blogFields},
	}, nil, &response)
	if err != nil {
		return nil, err
	}
	if response.User != nil && len(response.Blogs) == 0 {
		response.Blogs = response.User.Blogs
	}
	return &response, nil
}

func (c *Client) GetBlogInfo(ctx context.Context, blogName string) (*BlogInfoResponse, error) {
	normalized, err := requireIdentifierValue(NormalizeBlogName(blogName), "blog name")
	if err != nil {
		return nil, err
	}
	var response BlogInfoResponse
	err = c.do(ctx, http.MethodGet, "/v2/blog/"+url.PathEscape(normalized)+"/info", url.Values{
		"fields[blogs]": []string{blogFields},
	}, nil, &response)
	return &response, err
}

func (c *Client) ListConversations(ctx context.Context, selectedBlogUUID string, limit int) (*ConversationListResponse, error) {
	return c.ListConversationsBefore(ctx, selectedBlogUUID, limit, "")
}

func (c *Client) ListConversationsBefore(ctx context.Context, selectedBlogUUID string, limit int, before string) (*ConversationListResponse, error) {
	selectedBlogUUID, err := requireIdentifierValue(selectedBlogUUID, "selected blog UUID")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(before) != "" {
		before, err = requireIdentifierValue(before, "pagination cursor")
		if err != nil {
			return nil, err
		}
	}
	query := url.Values{
		"participant":   []string{selectedBlogUUID},
		"fields[blogs]": []string{conversationBlogFields},
	}
	if limit := cappedRequestLimit(limit); limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}
	if before != "" {
		query.Set("before", before)
	}
	var response ConversationListResponse
	err = c.do(ctx, http.MethodGet, "/v2/conversations", query, nil, &response)
	return &response, err
}

func (c *Client) GetParticipantSuggestions(ctx context.Context, forBlogName string, limit int) (*ParticipantSuggestionsResponse, error) {
	return c.SearchParticipantSuggestions(ctx, forBlogName, "", limit)
}

func (c *Client) SearchParticipantSuggestions(ctx context.Context, forBlogName, searchQuery string, limit int) (*ParticipantSuggestionsResponse, error) {
	participant, err := requireIdentifierValue(NormalizeBlogName(forBlogName), "participant blog name")
	if err != nil {
		return nil, err
	}
	query := url.Values{
		"participant":   []string{participant},
		"fields[blogs]": []string{suggestionBlogFields},
	}
	if searchQuery = strings.TrimSpace(searchQuery); searchQuery != "" {
		query.Set("q", searchQuery)
	}
	if limit := cappedRequestLimit(limit); limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}
	var response ParticipantSuggestionsResponse
	err = c.do(ctx, http.MethodGet, "/v2/conversations/participant_suggestions", query, nil, &response)
	return &response, err
}

func (c *Client) GetConversation(ctx context.Context, selectedBlogName, conversationID string, limit int) (*ConversationMessagesResponse, error) {
	return c.GetConversationBefore(ctx, selectedBlogName, conversationID, limit, "")
}

func (c *Client) GetConversationBefore(ctx context.Context, selectedBlogName, conversationID string, limit int, before string) (*ConversationMessagesResponse, error) {
	return c.getConversationMessages(ctx, selectedBlogName, conversationID, limit, before)
}

func (c *Client) getConversationMessages(ctx context.Context, selectedBlogName, conversationID string, limit int, before string) (*ConversationMessagesResponse, error) {
	selectedBlogName, err := requireIdentifierValue(selectedBlogName, "selected blog name")
	if err != nil {
		return nil, err
	}
	conversationID, err = requireIdentifierValue(conversationID, "conversation ID")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(before) != "" {
		before, err = requireIdentifierValue(before, "pagination cursor")
		if err != nil {
			return nil, err
		}
	}
	query := url.Values{
		"participant":     []string{selectedBlogName},
		"conversation_id": []string{conversationID},
		"fields[blogs]":   []string{conversationBlogFields},
	}
	query.Set("preserve_last_read_ts", "true")
	if limit := cappedRequestLimit(limit); limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}
	if before != "" {
		query.Set("before", before)
	}
	var response ConversationMessagesResponse
	err = c.do(ctx, http.MethodGet, "/v2/conversations/messages", query, nil, &response)
	return &response, err
}

func (c *Client) GetConversationByParticipants(ctx context.Context, selectedBlogName, otherParticipantName string, limit int) (*ConversationMessagesResponse, error) {
	selectedBlogName, err := requireIdentifierValue(NormalizeBlogName(selectedBlogName), "selected blog name")
	if err != nil {
		return nil, err
	}
	otherParticipantName, err = requireIdentifierValue(NormalizeBlogName(otherParticipantName), "other participant blog name")
	if err != nil {
		return nil, err
	}
	query := url.Values{
		"participant":     []string{selectedBlogName},
		"participants[0]": []string{selectedBlogName},
		"participants[1]": []string{otherParticipantName},
		"fields[blogs]":   []string{conversationBlogFields},
	}
	query.Set("preserve_last_read_ts", "true")
	if limit := cappedRequestLimit(limit); limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}
	var response ConversationMessagesResponse
	err = c.do(ctx, http.MethodGet, "/v2/conversations/messages", query, nil, &response)
	return &response, err
}

func (c *Client) SendText(ctx context.Context, selectedBlogName, conversationID, text string) (*SendMessageResponse, error) {
	selectedBlogName, err := requireIdentifierValue(selectedBlogName, "selected blog name")
	if err != nil {
		return nil, err
	}
	conversationID, err = requireIdentifierValue(conversationID, "conversation ID")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("message text is empty")
	}
	if utf8.RuneCountInString(text) > MaxMessageTextRunes {
		return nil, fmt.Errorf("message text is too long")
	}
	var response SendMessageResponse
	err = c.do(ctx, http.MethodPost, "/v2/conversations/messages", url.Values{
		"fields[blogs]": []string{conversationBlogFields},
	}, SendMessageRequest{
		ConversationID: conversationID,
		Type:           MessageTypeText,
		Participant:    selectedBlogName,
		Message:        text,
	}, &response)
	if err == nil && response.Conversation != nil {
		responseConversationID, validationErr := requireIdentifierValue(response.Conversation.ID, "send response conversation ID")
		if validationErr != nil {
			err = validationErr
		} else if responseConversationID != conversationID {
			err = fmt.Errorf("send response conversation ID did not match requested conversation ID")
		}
	}
	return &response, err
}

func (c *Client) SendTextToParticipants(ctx context.Context, senderParticipantID string, participantIDs []string, text string) (*SendMessageResponse, error) {
	senderParticipantID, err := requireIdentifierValue(senderParticipantID, "sender participant ID")
	if err != nil {
		return nil, err
	}
	participantIDs, err = requireParticipantIDs(participantIDs)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("message text is empty")
	}
	if utf8.RuneCountInString(text) > MaxMessageTextRunes {
		return nil, fmt.Errorf("message text is too long")
	}
	var response SendMessageResponse
	err = c.do(ctx, http.MethodPost, "/v2/conversations/messages", url.Values{
		"fields[blogs]": []string{conversationBlogFields},
	}, SendMessageRequest{
		Participants: participantIDs,
		Type:         MessageTypeText,
		Participant:  senderParticipantID,
		Message:      text,
	}, &response)
	if err == nil {
		if response.Conversation == nil {
			err = fmt.Errorf("send response conversation metadata is missing")
		} else if _, validationErr := requireIdentifierValue(response.Conversation.ID, "send response conversation ID"); validationErr != nil {
			err = validationErr
		}
	}
	return &response, err
}

func (c *Client) SendImage(ctx context.Context, selectedBlogName, conversationID string, image ImageUpload) (*SendMessageResponse, error) {
	selectedBlogName, err := requireIdentifierValue(selectedBlogName, "selected blog name")
	if err != nil {
		return nil, err
	}
	conversationID, err = requireIdentifierValue(conversationID, "conversation ID")
	if err != nil {
		return nil, err
	}
	image, err = requireImageUpload(image)
	if err != nil {
		return nil, err
	}
	var response SendMessageResponse
	err = c.do(ctx, http.MethodPost, "/v2/conversations/messages", url.Values{
		"fields[blogs]": []string{conversationBlogFields},
	}, multipartFormData{
		Fields: map[string]string{
			"conversation_id": conversationID,
			"type":            MessageTypeImage,
			"participant":     selectedBlogName,
		},
		FileField:       "data",
		FileName:        image.FileName,
		FileContentType: image.ContentType,
		FileData:        image.Data,
	}, &response)
	if err == nil && response.Conversation != nil {
		responseConversationID, validationErr := requireIdentifierValue(response.Conversation.ID, "send response conversation ID")
		if validationErr != nil {
			err = validationErr
		} else if responseConversationID != conversationID {
			err = fmt.Errorf("send response conversation ID did not match requested conversation ID")
		}
	}
	return &response, err
}

func (c *Client) SendImageToParticipants(ctx context.Context, senderParticipantID string, participantIDs []string, image ImageUpload) (*SendMessageResponse, error) {
	senderParticipantID, err := requireIdentifierValue(senderParticipantID, "sender participant ID")
	if err != nil {
		return nil, err
	}
	participantIDs, err = requireParticipantIDs(participantIDs)
	if err != nil {
		return nil, err
	}
	image, err = requireImageUpload(image)
	if err != nil {
		return nil, err
	}
	var response SendMessageResponse
	err = c.do(ctx, http.MethodPost, "/v2/conversations/messages", url.Values{
		"fields[blogs]": []string{conversationBlogFields},
	}, multipartFormData{
		Fields: map[string]string{
			"participants": strings.Join(participantIDs, ","),
			"type":         MessageTypeImage,
			"participant":  senderParticipantID,
		},
		FileField:       "data",
		FileName:        image.FileName,
		FileContentType: image.ContentType,
		FileData:        image.Data,
	}, &response)
	if err == nil {
		if response.Conversation == nil {
			err = fmt.Errorf("send response conversation metadata is missing")
		} else if _, validationErr := requireIdentifierValue(response.Conversation.ID, "send response conversation ID"); validationErr != nil {
			err = validationErr
		}
	}
	return &response, err
}

func (c *Client) SendSticker(ctx context.Context, selectedBlogName, conversationID, stickerID string) (*SendMessageResponse, error) {
	selectedBlogName, err := requireIdentifierValue(selectedBlogName, "selected blog name")
	if err != nil {
		return nil, err
	}
	conversationID, err = requireIdentifierValue(conversationID, "conversation ID")
	if err != nil {
		return nil, err
	}
	stickerID, err = requireIdentifierValue(stickerID, "sticker ID")
	if err != nil {
		return nil, err
	}
	var response SendMessageResponse
	err = c.do(ctx, http.MethodPost, "/v2/conversations/messages", url.Values{
		"fields[blogs]": []string{conversationBlogFields},
	}, SendMessageRequest{
		ConversationID: conversationID,
		Type:           MessageTypeSticker,
		Participant:    selectedBlogName,
		StickerID:      stickerID,
	}, &response)
	if err == nil && response.Conversation != nil {
		responseConversationID, validationErr := requireIdentifierValue(response.Conversation.ID, "send response conversation ID")
		if validationErr != nil {
			err = validationErr
		} else if responseConversationID != conversationID {
			err = fmt.Errorf("send response conversation ID did not match requested conversation ID")
		}
	}
	return &response, err
}

func (c *Client) SendStickerToParticipants(ctx context.Context, senderParticipantID string, participantIDs []string, stickerID string) (*SendMessageResponse, error) {
	senderParticipantID, err := requireIdentifierValue(senderParticipantID, "sender participant ID")
	if err != nil {
		return nil, err
	}
	participantIDs, err = requireParticipantIDs(participantIDs)
	if err != nil {
		return nil, err
	}
	stickerID, err = requireIdentifierValue(stickerID, "sticker ID")
	if err != nil {
		return nil, err
	}
	var response SendMessageResponse
	err = c.do(ctx, http.MethodPost, "/v2/conversations/messages", url.Values{
		"fields[blogs]": []string{conversationBlogFields},
	}, SendMessageRequest{
		Participants: participantIDs,
		Type:         MessageTypeSticker,
		Participant:  senderParticipantID,
		StickerID:    stickerID,
	}, &response)
	if err == nil {
		if response.Conversation == nil {
			err = fmt.Errorf("send response conversation metadata is missing")
		} else if _, validationErr := requireIdentifierValue(response.Conversation.ID, "send response conversation ID"); validationErr != nil {
			err = validationErr
		}
	}
	return &response, err
}

func (c *Client) SendPostRef(ctx context.Context, senderParticipantID, conversationID string, post PostShare) (*SendMessageResponse, error) {
	senderParticipantID, err := requireIdentifierValue(senderParticipantID, "sender participant ID")
	if err != nil {
		return nil, err
	}
	conversationID, err = requireIdentifierValue(conversationID, "conversation ID")
	if err != nil {
		return nil, err
	}
	post, err = requirePostShare(post)
	if err != nil {
		return nil, err
	}
	var response SendMessageResponse
	err = c.do(ctx, http.MethodPost, "/v2/conversations/messages", url.Values{
		"fields[blogs]": []string{conversationBlogFields},
	}, SendMessageRequest{
		ConversationID: conversationID,
		Type:           MessageTypePostRef,
		Participant:    senderParticipantID,
		Post:           post,
	}, &response)
	if err == nil && response.Conversation != nil {
		responseConversationID, validationErr := requireIdentifierValue(response.Conversation.ID, "send response conversation ID")
		if validationErr != nil {
			err = validationErr
		} else if responseConversationID != conversationID {
			err = fmt.Errorf("send response conversation ID did not match requested conversation ID")
		}
	}
	return &response, err
}

func (c *Client) SendPostRefToParticipants(ctx context.Context, senderParticipantID string, participantIDs []string, post PostShare) (*SendMessageResponse, error) {
	senderParticipantID, err := requireIdentifierValue(senderParticipantID, "sender participant ID")
	if err != nil {
		return nil, err
	}
	participantIDs, err = requireParticipantIDs(participantIDs)
	if err != nil {
		return nil, err
	}
	post, err = requirePostShare(post)
	if err != nil {
		return nil, err
	}
	var response SendMessageResponse
	err = c.do(ctx, http.MethodPost, "/v2/conversations/messages", url.Values{
		"fields[blogs]": []string{conversationBlogFields},
	}, SendMessageRequest{
		Participants: participantIDs,
		Type:         MessageTypePostRef,
		Participant:  senderParticipantID,
		Post:         post,
	}, &response)
	if err == nil {
		if response.Conversation == nil {
			err = fmt.Errorf("send response conversation metadata is missing")
		} else if _, validationErr := requireIdentifierValue(response.Conversation.ID, "send response conversation ID"); validationErr != nil {
			err = validationErr
		}
	}
	return &response, err
}

func (c *Client) MarkConversationAsRead(ctx context.Context, selectedBlogName, conversationID string) error {
	selectedBlogName, err := requireIdentifierValue(selectedBlogName, "selected blog name")
	if err != nil {
		return err
	}
	conversationID, err = requireIdentifierValue(conversationID, "conversation ID")
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, "/v2/conversations/mark_as_read", nil, map[string]string{
		"conversation_id": conversationID,
		"participant":     selectedBlogName,
	}, nil)
}

func (c *Client) DeleteConversation(ctx context.Context, selectedBlogName, conversationID string) error {
	selectedBlogName, err := requireIdentifierValue(selectedBlogName, "selected blog name")
	if err != nil {
		return err
	}
	conversationID, err = requireIdentifierValue(conversationID, "conversation ID")
	if err != nil {
		return err
	}
	query := url.Values{
		"conversation_id": []string{conversationID},
		"participant":     []string{selectedBlogName},
	}
	return c.do(ctx, http.MethodDelete, "/v2/conversations/messages", query, nil, nil)
}

func requireNonEmpty(value, fieldName string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s is empty", fieldName)
	}
	return value, nil
}

func cappedRequestLimit(limit int) int {
	if limit <= 0 {
		return 0
	}
	if limit > MaxRequestLimit {
		return MaxRequestLimit
	}
	return limit
}

func requireParticipantIDs(input []string) ([]string, error) {
	if len(input) < 2 {
		return nil, fmt.Errorf("participant IDs must include at least two participants")
	}
	output := make([]string, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	for _, participantID := range input {
		participantID, err := requireIdentifierValue(participantID, "participant ID")
		if err != nil {
			return nil, err
		}
		if _, ok := seen[participantID]; ok {
			continue
		}
		seen[participantID] = struct{}{}
		output = append(output, participantID)
	}
	if len(output) < 2 {
		return nil, fmt.Errorf("participant IDs must include at least two distinct participants")
	}
	return output, nil
}

func requirePostShare(input PostShare) (PostShare, error) {
	postID, err := requireIdentifierValue(input.ID, "post ID")
	if err != nil {
		return PostShare{}, err
	}
	blogID, err := requireIdentifierValue(input.Blog, "post blog UUID")
	if err != nil {
		return PostShare{}, err
	}
	return PostShare{
		ID:   postID,
		Blog: blogID,
		Type: strings.TrimSpace(input.Type),
	}, nil
}

func requireImageUpload(input ImageUpload) (ImageUpload, error) {
	if len(input.Data) == 0 {
		return ImageUpload{}, fmt.Errorf("image data is empty")
	}
	if int64(len(input.Data)) > DefaultMaxUploadBytes {
		return ImageUpload{}, fmt.Errorf("image data is too large")
	}
	contentType, err := normalizeImageUploadContentType(input.ContentType, input.Data)
	if err != nil {
		return ImageUpload{}, err
	}
	return ImageUpload{
		Data:        append([]byte(nil), input.Data...),
		FileName:    cleanUploadFileName(input.FileName, contentType),
		ContentType: contentType,
	}, nil
}

func normalizeImageUploadContentType(contentType string, data []byte) (string, error) {
	contentType = strings.TrimSpace(contentType)
	if contentType != "" {
		parsed, _, err := mime.ParseMediaType(contentType)
		if err != nil {
			return "", fmt.Errorf("image content type is invalid")
		}
		contentType = parsed
	} else {
		contentType = http.DetectContentType(data)
	}
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	if !strings.HasPrefix(contentType, "image/") {
		return "", fmt.Errorf("image content type is not supported")
	}
	return contentType, nil
}

func cleanUploadFileName(fileName, contentType string) string {
	fileName = strings.TrimSpace(strings.ReplaceAll(fileName, "\\", "/"))
	if slash := strings.LastIndex(fileName, "/"); slash >= 0 {
		fileName = fileName[slash+1:]
	}
	fileName = strings.Map(func(r rune) rune {
		switch {
		case r == '/' || r == '\\':
			return '_'
		case unicode.IsControl(r):
			return -1
		default:
			return r
		}
	}, fileName)
	if strings.TrimSpace(fileName) != "" {
		return fileName
	}
	extensions, _ := mime.ExtensionsByType(contentType)
	if len(extensions) > 0 {
		return "tumblr-image" + extensions[0]
	}
	return "tumblr-image"
}

func requireIdentifierValue(value, fieldName string) (string, error) {
	value, err := requireNonEmpty(value, fieldName)
	if err != nil {
		return "", err
	}
	if containsSpaceOrControl(value) {
		return "", fmt.Errorf("%s is invalid", fieldName)
	}
	if utf8.RuneCountInString(value) > maxIdentifierRunes {
		return "", fmt.Errorf("%s is invalid", fieldName)
	}
	return value, nil
}

func (c *Client) Download(ctx context.Context, rawURL string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxDownloadBytes
	}
	downloadURL, err := normalizeDownloadURL(rawURL)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "image/webp,image/png,image/jpeg,image/*;q=0.8,*/*;q=0.5")
	resp, err := c.downloadHTTPClient().Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return nil, fmt.Errorf("download request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("download failed with HTTP %d", resp.StatusCode)
	}
	if resp.ContentLength > maxBytes {
		return nil, fmt.Errorf("download is too large")
	}
	if !isImageContentType(resp.Header.Get("Content-Type")) {
		return nil, fmt.Errorf("download content type is not an image")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("download read failed")
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("download is too large")
	}
	return body, nil
}

func (c *Client) downloadHTTPClient() *http.Client {
	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	clone := *httpClient
	existingCheckRedirect := clone.CheckRedirect
	clone.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if req == nil || req.URL == nil {
			return fmt.Errorf("download redirect URL is invalid")
		}
		if _, err := normalizeDownloadURL(req.URL.String()); err != nil {
			return fmt.Errorf("download redirect URL is not allowed")
		}
		if existingCheckRedirect != nil {
			return existingCheckRedirect(req, via)
		}
		if len(via) >= 10 {
			return fmt.Errorf("stopped after 10 redirects")
		}
		return nil
	}
	return &clone
}

func isImageContentType(contentType string) bool {
	contentType = strings.TrimSpace(strings.ToLower(contentType))
	if contentType == "" {
		return true
	}
	contentType, _, _ = strings.Cut(contentType, ";")
	return strings.HasPrefix(strings.TrimSpace(contentType), "image/")
}

func normalizeDownloadURL(rawURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "", fmt.Errorf("download URL is invalid")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("download URL must use http or https")
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("download URL host is missing")
	}
	if parsed.User != nil {
		return "", fmt.Errorf("download URL user info is not allowed")
	}
	if !isTrustedTumblrDownloadHost(parsed.Hostname()) {
		return "", fmt.Errorf("download URL host is not allowed")
	}
	if isUnsafeDownloadHost(parsed.Hostname()) {
		return "", fmt.Errorf("download URL host is not allowed")
	}
	return parsed.String(), nil
}

// IsDownloadURLAllowed reports whether Download would accept rawURL before making a request.
func IsDownloadURLAllowed(rawURL string) bool {
	_, err := normalizeDownloadURL(rawURL)
	return err == nil
}

func isTrustedTumblrDownloadHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	return host == "tumblr.com" || strings.HasSuffix(host, ".tumblr.com")
}

func isUnsafeDownloadHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if host == "" {
		return true
	}
	if host == "localhost" || strings.HasSuffix(host, ".localhost") ||
		host == "localdomain" || strings.HasSuffix(host, ".localdomain") ||
		host == "local" || strings.HasSuffix(host, ".local") {
		return true
	}
	hostNoZone, _, _ := strings.Cut(host, "%")
	addr, err := netip.ParseAddr(hostNoZone)
	if err != nil {
		return isLegacyNumericIPv4Host(hostNoZone)
	}
	addr = addr.Unmap()
	return addr.IsLoopback() ||
		addr.IsPrivate() ||
		addr.IsLinkLocalUnicast() ||
		addr.IsLinkLocalMulticast() ||
		addr.IsUnspecified() ||
		addr.IsMulticast() ||
		!addr.IsGlobalUnicast()
}

func isLegacyNumericIPv4Host(host string) bool {
	parts := strings.Split(host, ".")
	if len(parts) == 0 || len(parts) > 4 {
		return false
	}
	for _, part := range parts {
		if !isLegacyNumericIPv4Part(part) {
			return false
		}
	}
	return true
}

func isLegacyNumericIPv4Part(part string) bool {
	if part == "" {
		return false
	}
	if strings.HasPrefix(part, "0x") || strings.HasPrefix(part, "0X") {
		if len(part) == 2 {
			return false
		}
		for _, r := range part[2:] {
			if !unicode.IsDigit(r) && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
				return false
			}
		}
		return true
	}
	for _, r := range part {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

type multipartFormData struct {
	Fields          map[string]string
	FileField       string
	FileName        string
	FileContentType string
	FileData        []byte
}

func (m multipartFormData) reader() (io.Reader, string, error) {
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	keys := make([]string, 0, len(m.Fields))
	for key := range m.Fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := writer.WriteField(key, m.Fields[key]); err != nil {
			return nil, "", err
		}
	}
	headers := make(textproto.MIMEHeader)
	headers.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`, escapeMultipartQuotedString(m.FileField), escapeMultipartQuotedString(m.FileName)))
	headers.Set("Content-Type", m.FileContentType)
	part, err := writer.CreatePart(headers)
	if err != nil {
		return nil, "", err
	}
	if _, err = part.Write(m.FileData); err != nil {
		return nil, "", err
	}
	if err = writer.Close(); err != nil {
		return nil, "", err
	}
	return &buffer, writer.FormDataContentType(), nil
}

func escapeMultipartQuotedString(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	mutating := isMutating(method)
	if c.needsBootstrap(mutating) {
		if err := c.Bootstrap(ctx); err != nil {
			return err
		}
	}
	if mutating && !c.hasCSRFToken() {
		return fmt.Errorf("tumblr API CSRF token is missing")
	}

	var err error
	for attempt := 0; ; attempt++ {
		err = c.doOnce(ctx, method, path, query, body, out)
		if !isRetryableTransientSendError(method, path, err) || attempt >= len(apiTransientRetryDelays) {
			return err
		}
		delay := apiTransientRetryDelays[attempt]
		if delay <= 0 {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func isRetryableTransientSendError(method, path string, err error) bool {
	if method != http.MethodPost || path != "/v2/conversations/messages" {
		return false
	}
	var apiErr *Error
	return errors.As(err, &apiErr) && apiErr.StatusCode >= http.StatusInternalServerError && apiErr.StatusCode < 600
}

func (c *Client) doOnce(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	c.mu.RLock()
	apiBaseURL := c.apiBaseURL
	c.mu.RUnlock()
	requestURL, err := url.Parse(apiBaseURL + path)
	if err != nil {
		return err
	}
	values := requestURL.Query()
	for key, vals := range query {
		for _, val := range vals {
			values.Add(key, val)
		}
	}
	requestURL.RawQuery = values.Encode()

	var reader io.Reader
	contentType := ""
	if body != nil {
		switch typed := body.(type) {
		case multipartFormData:
			reader, contentType, err = typed.reader()
			if err != nil {
				return err
			}
		default:
			data, err := json.Marshal(body)
			if err != nil {
				return err
			}
			reader = bytes.NewReader(data)
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL.String(), reader)
	if err != nil {
		return err
	}
	c.setAPIHeaders(req, body != nil)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.apiHTTPClient(apiBaseURL).Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return fmt.Errorf("tumblr API request failed")
	}
	defer resp.Body.Close()

	if resp.Request != nil && resp.Request.URL != nil && isLoginPath(resp.Request.URL.Path) {
		return &Error{
			StatusCode: http.StatusUnauthorized,
			Status:     "Tumblr session is not logged in",
		}
	}
	if isAPIAuthStatus(resp.StatusCode) {
		return &Error{
			StatusCode: resp.StatusCode,
			Status:     safeErrorDetail(resp.Status),
		}
	}

	responseBody, err := readLimitedBody(resp.Body, maxAPIResponseBytes, "Tumblr API response")
	if err != nil {
		return err
	}
	if csrf := responseCSRFToken(resp.Header); csrf != "" {
		c.mu.Lock()
		c.csrfToken = csrf
		c.mu.Unlock()
	}

	var envelope struct {
		Meta     APIMeta         `json:"meta"`
		Response json.RawMessage `json:"response"`
		Errors   []APIError      `json:"errors"`
	}
	if len(responseBody) > 0 {
		if err = json.Unmarshal(responseBody, &envelope); err != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return fmt.Errorf("failed to parse Tumblr API envelope: %w", err)
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &Error{
			StatusCode: resp.StatusCode,
			Status:     safeErrorDetail(resp.Status),
			Errors:     safeAPIErrors(envelope.Errors),
			Body:       safeErrorBody(responseBody),
		}
	}
	if envelope.Meta.Status != 0 && (envelope.Meta.Status < 200 || envelope.Meta.Status >= 300) {
		return &Error{
			StatusCode: envelope.Meta.Status,
			Status:     safeAPIMetaStatus(envelope.Meta.Status, envelope.Meta.Msg),
			Errors:     safeAPIErrors(envelope.Errors),
			Body:       safeErrorBody(responseBody),
		}
	}
	if out == nil {
		return nil
	}
	if len(envelope.Response) == 0 || bytes.Equal(bytes.TrimSpace(envelope.Response), []byte("null")) {
		return fmt.Errorf("tumblr api response is missing response data")
	}
	if err = json.Unmarshal(envelope.Response, out); err != nil {
		return fmt.Errorf("failed to parse Tumblr response: %w", err)
	}
	return nil
}

func isAPIAuthStatus(statusCode int) bool {
	return statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden
}

func responseCSRFToken(headers http.Header) string {
	if csrf := normalizeOptionalHeaderCredential(headers.Get("X-CSRF")); csrf != "" {
		return csrf
	}
	return normalizeOptionalHeaderCredential(headers.Get("X-CSRF-Token"))
}

func (c *Client) apiHTTPClient(apiBaseURL string) *http.Client {
	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	clone := *httpClient
	existingCheckRedirect := clone.CheckRedirect
	clone.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if req == nil || req.URL == nil {
			return fmt.Errorf("tumblr API redirect URL is invalid")
		}
		if !isAllowedAPIRedirectURL(req.URL, apiBaseURL) {
			return fmt.Errorf("tumblr API redirect URL is not allowed")
		}
		if existingCheckRedirect != nil {
			return existingCheckRedirect(req, via)
		}
		if len(via) >= 10 {
			return fmt.Errorf("stopped after 10 redirects")
		}
		return nil
	}
	return &clone
}

func isAllowedAPIRedirectURL(redirectURL *url.URL, apiBaseURL string) bool {
	if redirectURL == nil || redirectURL.Scheme == "" || redirectURL.Host == "" || redirectURL.User != nil {
		return false
	}
	apiURL, err := url.Parse(strings.TrimRight(apiBaseURL, "/"))
	if err == nil && apiURL.Scheme != "" && apiURL.Host != "" && isSameOrigin(redirectURL, apiURL) {
		return true
	}
	return isTrustedTumblrAPIOrigin(redirectURL)
}

func readLimitedBody(reader io.Reader, maxBytes int64, description string) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read %s", description)
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("%s is too large", description)
	}
	return body, nil
}

func (c *Client) setBrowserHeaders(req *http.Request) {
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	if c.cookieHeader != "" {
		req.Header.Set("Cookie", c.cookieHeader)
	}
}

func (c *Client) setAPIHeaders(req *http.Request, hasBody bool) {
	c.mu.RLock()
	apiToken := c.apiToken
	cookieHeader := c.cookieHeader
	csrfToken := c.csrfToken
	apiVersion := c.apiVersion
	c.mu.RUnlock()
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Accept", "application/json;format=camelcase")
	if apiVersion != "" {
		req.Header.Set("X-Version", apiVersion)
	}
	if cookieHeader != "" {
		req.Header.Set("Cookie", cookieHeader)
	}
	if hasBody {
		req.Header.Set("Content-Type", "application/json; charset=utf8")
	}
	if csrfToken != "" && isMutating(req.Method) {
		req.Header.Set("X-CSRF", csrfToken)
	}
}

func isMutating(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}
