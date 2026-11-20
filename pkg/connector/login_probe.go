package connector

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Probe-only: raw GETs of /messaging with the captured session to see what
// Tumblr's bot challenge keys on.
func probeTumblrChallenge(ctx context.Context, log *zerolog.Logger, cookieHeader, ua string) {
	type variant struct {
		name    string
		h1      bool
		browser bool
		ua      string
	}
	variants := []variant{
		{"h2_plain", false, false, ua},
		{"h1_plain", true, false, ua},
		{"h2_browser_headers", false, true, ua},
		{"h1_browser_headers", true, true, ua},
	}
	for _, v := range variants {
		tr := &http.Transport{Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: !v.h1}
		if v.h1 {
			tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		}
		c := &http.Client{Transport: tr, Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://www.tumblr.com/messaging", nil)
		req.Header.Set("User-Agent", v.ua)
		req.Header.Set("Cookie", cookieHeader)
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
		if v.browser {
			req.Header.Set("Accept-Language", "en-US,en;q=0.9")
			req.Header.Set("Upgrade-Insecure-Requests", "1")
			req.Header.Set("Sec-Fetch-Site", "same-origin")
			req.Header.Set("Sec-Fetch-Mode", "navigate")
			req.Header.Set("Sec-Fetch-Dest", "document")
			req.Header.Set("Sec-Fetch-User", "?1")
			req.Header.Set("Referer", "https://www.tumblr.com/")
			req.Header.Set("X-Requested-With", "com.beeper.android.dev")
		}
		resp, err := c.Do(req)
		if err != nil {
			log.Info().Str("probe", "raw_"+v.name).Err(err).Msg("Tumblr challenge probe")
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4000))
		resp.Body.Close()
		var setCookies []string
		for _, sc := range resp.Header.Values("Set-Cookie") {
			name, _, _ := strings.Cut(sc, "=")
			attrs := ""
			if _, rest, ok := strings.Cut(sc, ";"); ok {
				attrs = rest
			}
			setCookies = append(setCookies, name+";"+attrs)
		}
		sort.Strings(setCookies)
		challenged := strings.Contains(string(body), "__challenge")
		log.Info().Str("probe", "raw_"+v.name).Str("proto", resp.Proto).Int("status", resp.StatusCode).
			Bool("challenged", challenged).Str("location", resp.Header.Get("Location")).
			Strs("set_cookie_names", setCookies).Str("cookie_names_sent", cookieNames(cookieHeader)).
			Msg("Tumblr challenge probe")
	}
}

func cookieNames(h string) string {
	var names []string
	for _, p := range strings.Split(h, ";") {
		n, _, _ := strings.Cut(strings.TrimSpace(p), "=")
		names = append(names, n)
	}
	return fmt.Sprint(names)
}
