package connector

import (
	"os/exec"
	"runtime"
	"strings"
)

// Probe-only: lets a test device pick the login WebView user agent with
// `adb shell setprop debug.beeper.tumblr_ua <preset>` without rebuilding.
var tumblrLoginUserAgentPresets = map[string]string{
	"default":        "",
	"chrome_mobile":  "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36",
	"beeper_mobile":  "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Beeper/3.110.1 Chrome/146.0.0.0 Mobile Safari/537.36",
	"beeper_desktop": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Beeper/3.110.1 Chrome/146.0.0.0 Safari/537.36",
	"chrome_desktop": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
	"safari_mac":     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
	"safari_ios":     "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
}

const tumblrLoginUserAgentDefault = "default"

func tumblrLoginWebViewUserAgent() string {
	preset := tumblrLoginUserAgentDefault
	if runtime.GOOS == "android" {
		if out, err := exec.Command("/system/bin/getprop", "debug.beeper.tumblr_ua").Output(); err == nil {
			if v := strings.TrimSpace(string(out)); v != "" {
				preset = v
			}
		}
	}
	if ua, ok := tumblrLoginUserAgentPresets[preset]; ok {
		return ua
	}
	return tumblrLoginUserAgentPresets[tumblrLoginUserAgentDefault]
}
