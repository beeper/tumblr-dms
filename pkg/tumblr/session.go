package tumblr

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"sort"
	"strings"
	"sync"

	"golang.org/x/net/publicsuffix"
)

var sessionCookieNames = []string{
	"logged_in",
	"pfu",
	"sid",
	"tmgioct",
}

var sessionCookieNameSet = map[string]struct{}{
	"logged_in": {},
	"pfu":       {},
	"sid":       {},
	"tmgioct":   {},
}

var sessionCookieDomains = map[string]string{
	"logged_in": ".tumblr.com",
	"pfu":       "www.tumblr.com",
	"sid":       "www.tumblr.com",
	"tmgioct":   ".tumblr.com",
}

// CookieJar holds the live Tumblr session cookies. The HTTP client and the
// saved login share it, so saving the login always writes the current cookies.
type CookieJar struct {
	lock     sync.Mutex
	jar      http.CookieJar
	urls     []*url.URL
	onChange func()
}

var (
	_ http.CookieJar   = (*CookieJar)(nil)
	_ json.Marshaler   = (*CookieJar)(nil)
	_ json.Unmarshaler = (*CookieJar)(nil)
)

func NewCookieJar(cookies map[string]string) *CookieJar {
	jar, urls := newSessionCookieJar(DefaultWebBaseURL, DefaultAPIBaseURL, cookies)
	return &CookieJar{jar: jar, urls: urls}
}

func (j *CookieJar) Cookies(u *url.URL) []*http.Cookie {
	return j.jar.Cookies(u)
}

func (j *CookieJar) SetCookies(u *url.URL, cookies []*http.Cookie) {
	j.lock.Lock()
	before := sessionCookiesFromJar(j.jar, j.urls)
	j.jar.SetCookies(u, cookies)
	changed := !maps.Equal(before, sessionCookiesFromJar(j.jar, j.urls))
	onChange := j.onChange
	j.lock.Unlock()
	if changed && onChange != nil {
		onChange()
	}
}

// OnChange sets a function to call after Tumblr changes a session cookie.
func (j *CookieJar) OnChange(fn func()) {
	j.lock.Lock()
	j.onChange = fn
	j.lock.Unlock()
}

func (j *CookieJar) Values() map[string]string {
	if j == nil {
		return map[string]string{}
	}
	j.lock.Lock()
	defer j.lock.Unlock()
	return sessionCookiesFromJar(j.jar, j.urls)
}

func (j *CookieJar) MarshalJSON() ([]byte, error) {
	return json.Marshal(j.Values())
}

func (j *CookieJar) UnmarshalJSON(data []byte) error {
	var cookies map[string]string
	if err := json.Unmarshal(data, &cookies); err != nil {
		return err
	}
	jar, urls := newSessionCookieJar(DefaultWebBaseURL, DefaultAPIBaseURL, cookies)
	j.lock.Lock()
	j.jar = jar
	j.urls = urls
	j.lock.Unlock()
	return nil
}

func HasSessionCookies(cookies map[string]string) bool {
	return len(NormalizeSessionCookies(cookies)) > 0
}

func NormalizeSessionCookies(cookies map[string]string) map[string]string {
	normalized := make(map[string]string, len(sessionCookieNames))
	for _, name := range sessionCookieNames {
		value := strings.TrimSpace(cookies[name])
		if validCookieMapPair(name, value) {
			normalized[name] = value
		}
	}
	return normalized
}

func SessionCookiesFromMap(input map[string]string) map[string]string {
	cookies := SessionCookiesFromHeader(input["cookie_header"])
	for _, name := range sessionCookieNames {
		value := strings.TrimSpace(input[name])
		if validCookieMapPair(name, value) {
			cookies[name] = value
		}
	}
	return cookies
}

func SessionCookiesFromHeader(input string) map[string]string {
	cookies := make(map[string]string, len(sessionCookieNames))
	header := normalizeCookieHeader(input)
	for _, part := range strings.Split(header, ";") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)
		if _, allowed := sessionCookieNameSet[name]; allowed && validCookieMapPair(name, value) {
			cookies[name] = value
		}
	}
	return cookies
}

func SessionCookieHeader(cookies map[string]string) string {
	cookies = NormalizeSessionCookies(cookies)
	if len(cookies) == 0 {
		return ""
	}
	names := make([]string, 0, len(cookies))
	for name := range cookies {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+"="+cookies[name])
	}
	return strings.Join(parts, "; ")
}

func newSessionCookieJar(webBaseURL, apiBaseURL string, cookies map[string]string) (http.CookieJar, []*url.URL) {
	jar, err := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	if err != nil {
		jar, _ = cookiejar.New(nil)
	}
	urls := sessionCookieURLs(webBaseURL, apiBaseURL)
	seedSessionCookies(jar, urls, cookies)
	return jar, urls
}

func sessionCookieURLs(webBaseURL, apiBaseURL string) []*url.URL {
	candidates := []string{
		webBaseURL,
		apiBaseURL,
		DefaultWebBaseURL,
		"https://tumblr.com",
		"https://api.tumblr.com",
	}
	urls := make([]*url.URL, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		parsed, err := url.Parse(candidate)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil {
			continue
		}
		if !isTrustedTumblrWebOrigin(parsed) && !isTrustedTumblrAPIOrigin(parsed) {
			continue
		}
		parsed.Path = "/"
		parsed.RawPath = ""
		parsed.RawQuery = ""
		parsed.Fragment = ""
		key := strings.ToLower(parsed.Scheme + "://" + parsed.Host)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		urls = append(urls, parsed)
	}
	return urls
}

func seedSessionCookies(jar http.CookieJar, urls []*url.URL, cookies map[string]string) {
	if jar == nil || len(urls) == 0 {
		return
	}
	cookies = NormalizeSessionCookies(cookies)
	if len(cookies) == 0 {
		return
	}
	target := urls[0]
	for _, candidate := range urls {
		if strings.EqualFold(candidate.Hostname(), "www.tumblr.com") {
			target = candidate
			break
		}
	}
	values := make([]*http.Cookie, 0, len(cookies))
	for _, name := range sessionCookieNames {
		value, ok := cookies[name]
		if !ok {
			continue
		}
		values = append(values, &http.Cookie{
			Name:     name,
			Value:    value,
			Path:     "/",
			Domain:   sessionCookieDomains[name],
			Secure:   true,
			HttpOnly: true,
		})
	}
	jar.SetCookies(target, values)
}

func sessionCookiesFromJar(jar http.CookieJar, urls []*url.URL) map[string]string {
	cookies := make(map[string]string, len(sessionCookieNames))
	if jar == nil {
		return cookies
	}
	for _, target := range urls {
		for _, cookie := range jar.Cookies(target) {
			if _, allowed := sessionCookieNameSet[cookie.Name]; !allowed {
				continue
			}
			if _, alreadyCaptured := cookies[cookie.Name]; alreadyCaptured {
				continue
			}
			if validCookieMapPair(cookie.Name, cookie.Value) {
				cookies[cookie.Name] = cookie.Value
			}
		}
	}
	return cookies
}
