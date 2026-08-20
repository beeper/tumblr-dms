package tumblr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"path"
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

var authRefreshRetryDelays = []time.Duration{
	3 * time.Second,
	6 * time.Second,
}

type Options struct {
	WebBaseURL     string
	APIBaseURL     string
	UserAgent      string
	CookieHeader   string
	SessionCookies map[string]string
	APIToken       string
	CSRFToken      string
	APIVersion     string
	HTTPClient     *http.Client
}

type Client struct {
	mu             sync.RWMutex
	authRefreshMu  sync.Mutex
	authGeneration uint64
	webBaseURL     string
	apiBaseURL     string
	userAgent      string
	apiToken       string
	csrfToken      string
	apiVersion     string
	httpClient     *http.Client
	sessionURLs    []*url.URL
	sessionUpdates chan struct{}
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
	httpClientClone := *httpClient
	sessionCookies := NormalizeSessionCookies(opts.SessionCookies)
	if len(sessionCookies) == 0 {
		sessionCookies = SessionCookiesFromHeader(opts.CookieHeader)
	}
	jar, sessionURLs := newSessionCookieJar(webBaseURL, apiBaseURL, sessionCookies)
	httpClientClone.Jar = jar
	return &Client{
		webBaseURL:     webBaseURL,
		apiBaseURL:     apiBaseURL,
		userAgent:      userAgent,
		apiToken:       normalizeBearerToken(opts.APIToken),
		csrfToken:      normalizeOptionalHeaderCredential(opts.CSRFToken),
		apiVersion:     normalizeOptionalHeaderCredential(opts.APIVersion),
		httpClient:     &httpClientClone,
		sessionURLs:    sessionURLs,
		sessionUpdates: make(chan struct{}, 1),
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
	return SessionCookieHeader(SessionCookiesFromMap(cookies))
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

func CookieHeaderHasPair(header string) bool {
	return HasSessionCookies(SessionCookiesFromHeader(header))
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
	return SessionCookieHeader(c.SessionSnapshot().Cookies)
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

func (c *Client) SessionSnapshot() SessionSnapshot {
	if c == nil {
		return SessionSnapshot{}
	}
	c.mu.RLock()
	snapshot := SessionSnapshot{
		APIToken:   c.apiToken,
		CSRFToken:  c.csrfToken,
		APIVersion: c.apiVersion,
	}
	c.mu.RUnlock()
	if c.httpClient != nil {
		snapshot.Cookies = sessionCookiesFromJar(c.httpClient.Jar, c.sessionURLs)
	}
	if snapshot.Cookies == nil {
		snapshot.Cookies = make(map[string]string)
	}
	return snapshot
}

func (c *Client) SessionUpdates() <-chan struct{} {
	if c == nil {
		return nil
	}
	return c.sessionUpdates
}

func (c *Client) signalSessionUpdateIfChanged(previous SessionSnapshot) {
	if c == nil || previous.Equal(c.SessionSnapshot()) {
		return
	}
	select {
	case c.sessionUpdates <- struct{}{}:
	default:
	}
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
	c.authRefreshMu.Lock()
	defer c.authRefreshMu.Unlock()
	return c.bootstrap(ctx)
}

func (c *Client) bootstrapIfNeeded(ctx context.Context, mutating bool) error {
	c.authRefreshMu.Lock()
	defer c.authRefreshMu.Unlock()
	if !c.needsBootstrap(mutating) {
		return nil
	}
	return c.bootstrap(ctx)
}

func (c *Client) refreshAuthAfterFailure(ctx context.Context, failedGeneration uint64) error {
	c.authRefreshMu.Lock()
	defer c.authRefreshMu.Unlock()
	if c.currentAuthGeneration() != failedGeneration {
		return nil
	}
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := c.bootstrap(ctx)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err == nil || !IsAuthError(err) || attempt >= len(authRefreshRetryDelays) {
			return err
		}
		timer := time.NewTimer(authRefreshRetryDelays[attempt])
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Client) currentAuthGeneration() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.authGeneration
}

func (c *Client) bootstrap(ctx context.Context) error {
	previousSession := c.SessionSnapshot()
	defer c.signalSessionUpdateIfChanged(previousSession)
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
			Auth:    resp.StatusCode == http.StatusUnauthorized,
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
			return &BootstrapError{Message: "Tumblr messaging page included an invalid API token", Incomplete: true}
		}
		return &BootstrapError{Message: "Tumblr messaging page did not include an API token", Incomplete: true}
	}
	c.mu.Lock()
	if apiBaseURL != "" {
		c.apiBaseURL = apiBaseURL
	}
	c.apiToken = apiToken
	c.csrfToken = csrfToken
	c.authGeneration++
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
	err = c.doMessageSend(ctx, url.Values{
		"fields[blogs]": []string{conversationBlogFields},
	}, SendMessageRequest{
		ConversationID: conversationID,
		Type:           MessageTypeText,
		Participant:    selectedBlogName,
		Message:        text,
	}, &response)
	if err == nil {
		err = validateSendResponseConversation(&response, conversationID)
	}
	return &response, classifySendResponseError(err)
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
	err = c.doMessageSend(ctx, url.Values{
		"fields[blogs]": []string{conversationBlogFields},
	}, SendMessageRequest{
		Participants: participantIDs,
		Type:         MessageTypeText,
		Participant:  senderParticipantID,
		Message:      text,
	}, &response)
	if err == nil {
		err = validateSendResponseParticipants(&response, participantIDs)
	}
	return &response, classifySendResponseError(err)
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
	err = c.doMessageSend(ctx, url.Values{
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
	if err == nil {
		err = validateSendResponseConversation(&response, conversationID)
	}
	return &response, classifySendResponseError(err)
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
	err = c.doMessageSend(ctx, url.Values{
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
		err = validateSendResponseParticipants(&response, participantIDs)
	}
	return &response, classifySendResponseError(err)
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
	err = c.doMessageSend(ctx, url.Values{
		"fields[blogs]": []string{conversationBlogFields},
	}, SendMessageRequest{
		ConversationID: conversationID,
		Type:           MessageTypeSticker,
		Participant:    selectedBlogName,
		StickerID:      stickerID,
	}, &response)
	if err == nil {
		err = validateSendResponseConversation(&response, conversationID)
	}
	return &response, classifySendResponseError(err)
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
	err = c.doMessageSend(ctx, url.Values{
		"fields[blogs]": []string{conversationBlogFields},
	}, SendMessageRequest{
		Participants: participantIDs,
		Type:         MessageTypeSticker,
		Participant:  senderParticipantID,
		StickerID:    stickerID,
	}, &response)
	if err == nil {
		err = validateSendResponseParticipants(&response, participantIDs)
	}
	return &response, classifySendResponseError(err)
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
	err = c.doMessageSend(ctx, url.Values{
		"fields[blogs]": []string{conversationBlogFields},
	}, SendMessageRequest{
		ConversationID: conversationID,
		Type:           MessageTypePostRef,
		Participant:    senderParticipantID,
		Post:           post,
	}, &response)
	if err == nil {
		err = validateSendResponseConversation(&response, conversationID)
	}
	return &response, classifySendResponseError(err)
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
	err = c.doMessageSend(ctx, url.Values{
		"fields[blogs]": []string{conversationBlogFields},
	}, SendMessageRequest{
		Participants: participantIDs,
		Type:         MessageTypePostRef,
		Participant:  senderParticipantID,
		Post:         post,
	}, &response)
	if err == nil {
		err = validateSendResponseParticipants(&response, participantIDs)
	}
	return &response, classifySendResponseError(err)
}

func validateSendResponseConversation(response *SendMessageResponse, expectedConversationID string) error {
	if response == nil || response.Conversation == nil {
		return fmt.Errorf("send response conversation metadata is missing")
	}
	conversationID, err := requireIdentifierValue(response.Conversation.ID, "send response conversation ID")
	if err != nil {
		return err
	}
	if expectedConversationID != "" && conversationID != expectedConversationID {
		return fmt.Errorf("send response conversation ID did not match requested conversation ID")
	}
	return nil
}

func validateSendResponseParticipants(response *SendMessageResponse, expectedParticipantIDs []string) error {
	if err := validateSendResponseConversation(response, ""); err != nil {
		return err
	}
	actualParticipantIDs := make([]string, 0, len(response.Conversation.Participants))
	for _, participant := range response.Conversation.Participants {
		participantID, err := requireIdentifierValue(participant.UUID, "send response participant UUID")
		if err != nil {
			return err
		}
		actualParticipantIDs = append(actualParticipantIDs, participantID)
	}
	expectedParticipantIDs = append([]string(nil), expectedParticipantIDs...)
	sort.Strings(actualParticipantIDs)
	sort.Strings(expectedParticipantIDs)
	if len(actualParticipantIDs) != len(expectedParticipantIDs) {
		return fmt.Errorf("send response participants did not match requested participants")
	}
	for i := range actualParticipantIDs {
		if actualParticipantIDs[i] != expectedParticipantIDs[i] {
			return fmt.Errorf("send response participants did not match requested participants")
		}
	}
	return nil
}

func classifySendResponseError(err error) error {
	if err == nil {
		return nil
	}
	var sendErr *MessageSendError
	if errors.As(err, &sendErr) {
		return err
	}
	// At this point the POST returned a successful status, but its body did not
	// prove which conversation/message was accepted. Treat that as unknown.
	return &MessageSendError{Err: err, Definite: false}
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
	contentType, err := SniffImageMIME(input.Data)
	if err != nil {
		return ImageUpload{}, err
	}
	return ImageUpload{
		Data:        append([]byte(nil), input.Data...),
		FileName:    cleanUploadFileName(input.FileName, contentType),
		ContentType: contentType,
	}, nil
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
		extension := CanonicalImageExtension(contentType)
		baseName := strings.TrimSuffix(fileName, path.Ext(fileName))
		if strings.TrimSpace(baseName) == "" {
			baseName = "tumblr-image"
		}
		return baseName + extension
	}
	if extension := CanonicalImageExtension(contentType); extension != "" {
		return "tumblr-image" + extension
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
		if err := c.bootstrapIfNeeded(ctx, mutating); err != nil {
			return err
		}
	}
	if mutating && !c.hasCSRFToken() {
		return fmt.Errorf("tumblr API CSRF token is missing")
	}

	var err error
	authRetried := false
	transientAttempt := 0
	for {
		requestGeneration := c.currentAuthGeneration()
		err = c.doOnce(ctx, method, path, query, body, out)
		if ShouldRefreshAuth(err, mutating) && !authRetried {
			authRetried = true
			authErr := err
			if refreshErr := c.refreshAuthAfterFailure(ctx, requestGeneration); refreshErr != nil {
				return fmt.Errorf("tumblr API %s returned %v; authentication refresh failed: %w", method, authErr, refreshErr)
			}
			if mutating && !c.hasCSRFToken() {
				return fmt.Errorf("tumblr API CSRF token is missing")
			}
			continue
		}
		if !isRetryableTransientSendError(method, path, err) || transientAttempt >= len(apiTransientRetryDelays) {
			return err
		}
		delay := apiTransientRetryDelays[transientAttempt]
		transientAttempt++
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

// doMessageSend disables redirects and transient retries. It only retries once
// when Tumblr definitively rejected the first request for expired credentials
// and fresh credentials were loaded successfully.
func (c *Client) doMessageSend(ctx context.Context, query url.Values, body any, out any) error {
	if c.needsBootstrap(true) {
		if err := c.bootstrapIfNeeded(ctx, true); err != nil {
			return &MessageSendError{Err: err, Definite: true}
		}
	}
	if !c.hasCSRFToken() {
		return &MessageSendError{Err: fmt.Errorf("tumblr API CSRF token is missing"), Definite: true}
	}
	requestGeneration := c.currentAuthGeneration()
	err := c.doOnceWithRedirectPolicy(
		ctx,
		http.MethodPost,
		"/v2/conversations/messages",
		query,
		body,
		out,
		false,
	)
	if err == nil {
		return nil
	}
	definite := isDefiniteMessageRejection(err)
	sendErr := &MessageSendError{Err: err, Definite: definite}
	if !definite || !ShouldRefreshAuth(err, true) {
		return sendErr
	}
	if refreshErr := c.refreshAuthAfterFailure(ctx, requestGeneration); refreshErr != nil {
		sendErr.authRefreshError = fmt.Errorf("refresh Tumblr authentication after rejected message: %w", refreshErr)
		return sendErr
	}
	if !c.hasCSRFToken() {
		sendErr.authRefreshError = errors.New("refresh Tumblr authentication after rejected message: CSRF token is missing")
		return sendErr
	}

	retryErr := c.doOnceWithRedirectPolicy(
		ctx,
		http.MethodPost,
		"/v2/conversations/messages",
		query,
		body,
		out,
		false,
	)
	if retryErr == nil {
		return nil
	}
	return &MessageSendError{Err: retryErr, Definite: isDefiniteMessageRejection(retryErr)}
}

func isDefiniteMessageRejection(err error) bool {
	var apiErr *Error
	return errors.As(err, &apiErr) &&
		apiErr.StatusCode >= http.StatusBadRequest && apiErr.StatusCode < http.StatusInternalServerError
}

func isRetryableTransientSendError(method, path string, err error) bool {
	if method != http.MethodPost || path != "/v2/conversations/messages" {
		return false
	}
	var apiErr *Error
	return errors.As(err, &apiErr) && apiErr.StatusCode >= http.StatusInternalServerError && apiErr.StatusCode < 600
}

func (c *Client) doOnce(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	return c.doOnceWithRedirectPolicy(ctx, method, path, query, body, out, true)
}

func (c *Client) doOnceWithRedirectPolicy(ctx context.Context, method, path string, query url.Values, body any, out any, allowRedirects bool) error {
	previousSession := c.SessionSnapshot()
	defer c.signalSessionUpdateIfChanged(previousSession)
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
	if !allowRedirects {
		// NewRequest makes byte buffers replayable through GetBody. Clearing it
		// prevents the HTTP/2 transport from resubmitting a message body after an
		// ambiguous stream failure.
		req.GetBody = nil
	}
	c.setAPIHeaders(req, body != nil)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	httpClient := c.apiHTTPClient(apiBaseURL)
	if !allowRedirects {
		httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return fmt.Errorf("tumblr API request failed")
	}
	defer resp.Body.Close()
	if csrf := responseCSRFToken(resp.Header); csrf != "" {
		c.mu.Lock()
		c.csrfToken = csrf
		c.mu.Unlock()
	}

	if resp.Request != nil && resp.Request.URL != nil && isLoginPath(resp.Request.URL.Path) {
		return &Error{
			StatusCode: http.StatusUnauthorized,
			Status:     "Tumblr session is not logged in",
		}
	}
	responseBody, err := readLimitedBody(resp.Body, maxAPIResponseBytes, "Tumblr API response")
	if err != nil {
		if resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError {
			return &Error{
				StatusCode: resp.StatusCode,
				Status:     safeErrorDetail(resp.Status),
			}
		}
		return err
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
}

func (c *Client) setAPIHeaders(req *http.Request, hasBody bool) {
	c.mu.RLock()
	apiToken := c.apiToken
	csrfToken := c.csrfToken
	apiVersion := c.apiVersion
	c.mu.RUnlock()
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Accept", "application/json;format=camelcase")
	if apiVersion != "" {
		req.Header.Set("X-Version", apiVersion)
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
