package connector

import (
	"os/exec"
	"runtime"
	"strings"
)

// Probe-only: lets a test device pick the login WebView user agent with
// `adb shell setprop debug.beeper.tumblr_ua <preset>` without rebuilding.
// Preset "custom" concatenates debug.beeper.tumblr_ua_1.._3 (each prop value
// is capped at 91 bytes).
var tumblrLoginUserAgentPresets = map[string]string{
	"default":        "",
	"chrome_mobile":  "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Mobile Safari/537.36",
	"beeper_mobile":  "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Beeper/3.110.1 Chrome/134.0.0.0 Mobile Safari/537.36",
	"beeper_desktop": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Beeper/3.110.1 Chrome/134.0.0.0 Safari/537.36",
	"chrome_desktop": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
	"safari_mac":     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
	"safari_ios":     "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
}

const tumblrLoginUserAgentDefault = "default"

func getAndroidProp(name string) string {
	if runtime.GOOS != "android" {
		return ""
	}
	out, err := exec.Command("/system/bin/getprop", name).Output()
	if err != nil {
		return ""
	}
	return strings.TrimRight(string(out), "\r\n")
}

func tumblrLoginWebViewUserAgent() string {
	preset := tumblrLoginUserAgentDefault
	if v := strings.TrimSpace(getAndroidProp("debug.beeper.tumblr_ua")); v != "" {
		preset = v
	}
	if preset == "custom" {
		return strings.ReplaceAll(getAndroidProp("debug.beeper.tumblr_ua_1")+getAndroidProp("debug.beeper.tumblr_ua_2")+getAndroidProp("debug.beeper.tumblr_ua_3"), "_", " ")
	}
	if ua, ok := tumblrLoginUserAgentPresets[preset]; ok {
		return ua
	}
	return tumblrLoginUserAgentPresets[tumblrLoginUserAgentDefault]
}
