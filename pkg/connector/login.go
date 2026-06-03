package connector

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblrid"
)

const (
	loginFlowCookies = "cookies"

	loginStepIDCookies  = "com.ifixrobots.tumblr_dms.login.cookies"
	loginStepIDComplete = "com.ifixrobots.tumblr_dms.login.complete"

	loginInstructions = "Open Tumblr in a private browser window and sign in. The bridge captures Tumblr's web session from the signed-in page."

	tumblrExtractJSSession = `
new Promise(resolve => {
	const readJSONString = value => {
		if (!value) return ""
		try {
			return JSON.parse('"' + value + '"')
		} catch {
			return value
		}
	}
	const extract = () => {
		const html = document.documentElement?.innerHTML || ""
		const out = {}
		const cookieHeader = document.cookie || ""
		if (cookieHeader) out.cookie_header = cookieHeader
		const apiToken = readJSONString(html.match(/"API_TOKEN"\s*:\s*"([^"]+)"/)?.[1])
		if (apiToken) out.api_token = apiToken
		const csrfToken = readJSONString(html.match(/"csrfToken"\s*:\s*"([^"]*)"/)?.[1])
		if (csrfToken) out.csrf_token = csrfToken
		return out.api_token ? out : null
	}
	const existing = extract()
	if (existing) {
		resolve(existing)
		return
	}
	const started = Date.now()
	const timer = setInterval(() => {
		const next = extract()
		if (next || Date.now() - started > 30000) {
			clearInterval(timer)
			resolve(next)
		}
	}, 250)
})
`
)

var tumblrBrowserSessionCookieFields = []bridgev2.LoginCookieField{
	{
		ID:       "pfu",
		Required: false,
		Sources:  tumblrCookieSources("pfu"),
	},
	{
		ID:       "sid",
		Required: false,
		Sources:  tumblrCookieSources("sid"),
	},
	{
		ID:       "logged_in",
		Required: false,
		Sources:  tumblrCookieSources("logged_in"),
	},
	{
		ID:       "tmgioct",
		Required: false,
		Sources:  tumblrCookieSources("tmgioct"),
	},
}

func tumblrCookieSources(name string) []bridgev2.LoginCookieFieldSource {
	return []bridgev2.LoginCookieFieldSource{{
		Type: bridgev2.LoginCookieTypeCookie,
		Name: name,
	}}
}

type TumblrLogin struct {
	User *bridgev2.User
	tc   *TumblrConnector
	flow string
}

var _ bridgev2.LoginProcessCookies = (*TumblrLogin)(nil)
var _ bridgev2.LoginProcessWithOverride = (*TumblrLogin)(nil)

func (tc *TumblrConnector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{
		{
			Name:        "Browser cookies",
			Description: "Log in by capturing Tumblr session cookies from a browser session",
			ID:          loginFlowCookies,
		},
	}
}

func (tc *TumblrConnector) CreateLogin(_ context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	if flowID != loginFlowCookies {
		return nil, bridgev2.ErrInvalidLoginFlowID
	}
	if tc == nil {
		return nil, fmt.Errorf("tumblr connector is missing")
	}
	if user == nil {
		return nil, fmt.Errorf("matrix user is required to start tumblr login")
	}
	return &TumblrLogin{User: user, tc: tc, flow: flowID}, nil
}

func (tl *TumblrLogin) Start(context.Context) (*bridgev2.LoginStep, error) {
	if err := tl.validateConnector(); err != nil {
		return nil, err
	}
	return tl.cookieStep(loginInstructions), nil
}

func (tl *TumblrLogin) StartWithOverride(context.Context, *bridgev2.UserLogin) (*bridgev2.LoginStep, error) {
	if err := tl.validateConnector(); err != nil {
		return nil, err
	}
	return tl.cookieStep("Re-authenticate the existing Tumblr login. " + loginInstructions), nil
}

func (tl *TumblrLogin) cookieStep(instructions string) *bridgev2.LoginStep {
	fields := []bridgev2.LoginCookieField{
		{
			ID:       "cookie_header",
			Required: false,
			Sources: []bridgev2.LoginCookieFieldSource{
				{Type: bridgev2.LoginCookieTypeRequestHeader, Name: "Cookie", RequestURLRegex: `^https://www\.tumblr\.com/.*`},
				{Type: bridgev2.LoginCookieTypeSpecial, Name: "com.ifixrobots.tumblr_dms.cookie_header"},
			},
		},
	}
	fields = append(fields, tumblrBrowserSessionCookieFields...)
	fields = append(fields,
		bridgev2.LoginCookieField{
			ID:       "api_token",
			Required: true,
			Sources: []bridgev2.LoginCookieFieldSource{
				{Type: bridgev2.LoginCookieTypeRequestHeader, Name: "Authorization", RequestURLRegex: `^https://www\.tumblr\.com/api/.*`},
				{Type: bridgev2.LoginCookieTypeSpecial, Name: "com.ifixrobots.tumblr_dms.api_token"},
			},
		},
		bridgev2.LoginCookieField{
			ID:       "csrf_token",
			Required: false,
			Sources: []bridgev2.LoginCookieFieldSource{
				{Type: bridgev2.LoginCookieTypeRequestHeader, Name: "X-CSRF", RequestURLRegex: `^https://www\.tumblr\.com/api/.*`},
				{Type: bridgev2.LoginCookieTypeRequestHeader, Name: "X-CSRF-Token", RequestURLRegex: `^https://www\.tumblr\.com/api/.*`},
				{Type: bridgev2.LoginCookieTypeSpecial, Name: "com.ifixrobots.tumblr_dms.csrf_token"},
			},
		},
		bridgev2.LoginCookieField{
			ID:       "api_version",
			Required: false,
			Sources: []bridgev2.LoginCookieFieldSource{
				{Type: bridgev2.LoginCookieTypeRequestHeader, Name: "X-Version", RequestURLRegex: `^https://www\.tumblr\.com/api/.*`},
			},
		},
	)
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeCookies,
		StepID:       loginStepIDCookies,
		Instructions: instructions,
		CookiesParams: &bridgev2.LoginCookiesParams{
			URL:               "https://www.tumblr.com/messages",
			UserAgent:         tl.tc.Config.BrowserUserAgent(),
			WaitForURLPattern: `^https://www\.tumblr\.com/(?:messages|messaging|dashboard|blog/[^/?#]+/messages)(?:[/?#].*)?$`,
			Fields:            fields,
			ExtractJS:         tumblrExtractJSSession,
		},
	}
}

func (tl *TumblrLogin) Cancel() {}

func (tl *TumblrLogin) SubmitCookies(ctx context.Context, cookies map[string]string) (*bridgev2.LoginStep, error) {
	return tl.submitCookieInput(ctx, cookies)
}

func (tl *TumblrLogin) submitCookieInput(ctx context.Context, cookies map[string]string) (*bridgev2.LoginStep, error) {
	if err := tl.validateConnector(); err != nil {
		return nil, err
	}
	tl.logSubmittedFields(cookies)
	cookieHeader := tumblr.CookieHeaderFromMap(cookies)
	apiToken, csrfToken, apiVersion := loginTokensFromInput(cookies)
	if cookieHeader == "" && apiToken == "" {
		return nil, fmt.Errorf("tumblr session cookies or API token are required")
	}
	if cookieHeader != "" && !tumblr.CookieHeaderHasPair(cookieHeader) {
		return nil, fmt.Errorf("tumblr session cookies must include at least one name=value cookie")
	}
	if tl.User == nil {
		return nil, fmt.Errorf("matrix user is required to complete tumblr login")
	}

	client := tumblr.NewClient(tumblr.Options{
		CookieHeader: cookieHeader,
		APIToken:     apiToken,
		CSRFToken:    csrfToken,
		APIVersion:   apiVersion,
		UserAgent:    tl.tc.Config.BrowserUserAgent(),
		HTTPClient:   tl.tc.newHTTPClient(),
	})
	if apiToken == "" {
		if err := client.Bootstrap(ctx); err != nil {
			return nil, tumblrLoginValidationError("failed to validate Tumblr session", err)
		}
	}
	if client.APIToken() == "" {
		return nil, tumblrLoginValidationError("failed to validate Tumblr session", fmt.Errorf("tumblr session did not include an API token"))
	}
	userInfo, err := client.CurrentUser(ctx)
	if err != nil {
		tl.logValidationError(err)
		return nil, tumblrLoginValidationError("failed to load Tumblr account info", err)
	}
	blog, err := selectMessagingBlog(userInfo)
	if err != nil {
		return nil, err
	}

	meta := &UserLoginMetadata{
		CookieHeader:     cookieHeader,
		APIToken:         client.APIToken(),
		CSRFToken:        client.CSRFToken(),
		APIVersion:       client.APIVersion(),
		UserName:         userName(userInfo, blog),
		SelectedBlogName: blog.Name,
		SelectedBlogUUID: blog.UUID,
	}
	remoteName := blog.Name
	remoteProfile := &status.RemoteProfile{
		Username: remoteName,
		Name:     displayName(tl.tc, blog),
	}
	loginID := tumblrid.MakeUserLoginID(blog.UUID)
	if loginID == "" {
		loginID = tumblrid.MakeUserLoginID(blog.Name)
	}
	userLogin, err := tl.User.NewLogin(
		ctx,
		&database.UserLogin{
			ID:            loginID,
			Metadata:      meta,
			RemoteName:    remoteName,
			RemoteProfile: *remoteProfile,
		},
		&bridgev2.NewLoginParams{
			DeleteOnConflict: true,
			LoadUserLogin: func(ctx context.Context, login *bridgev2.UserLogin) error {
				return tl.tc.LoadUserLogin(ctx, login)
			},
		},
	)
	if err != nil {
		return nil, err
	}
	go userLogin.Client.Connect(context.WithoutCancel(ctx))

	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       loginStepIDComplete,
		Instructions: fmt.Sprintf("Successfully logged into Tumblr as %s", remoteName),
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: userLogin.ID,
			UserLogin:   userLogin,
		},
	}, nil
}

func (tl *TumblrLogin) logSubmittedFields(input map[string]string) {
	if tl == nil || tl.User == nil {
		return
	}
	fields := make([]string, 0, len(input))
	for key, value := range input {
		fields = append(fields, fmt.Sprintf("%s:%d", key, len(value)))
	}
	sort.Strings(fields)
	tl.User.Log.Debug().Strs("fields", fields).Msg("Received Tumblr login credential fields")
}

func (tl *TumblrLogin) logValidationError(err error) {
	if tl == nil || tl.User == nil || err == nil {
		return
	}
	tl.User.Log.Debug().Err(err).Msg("Tumblr login validation failed")
}

func (tl *TumblrLogin) validateConnector() error {
	if tl == nil || tl.tc == nil {
		return fmt.Errorf("tumblr connector is missing")
	}
	return nil
}

func tumblrLoginValidationError(message string, err error) error {
	if tumblr.IsAuthError(err) {
		return bridgev2.RespError{
			ErrCode:    "FI.MAU.TUMBLRDMS.BAD_CREDENTIALS",
			Err:        "Tumblr rejected those cookies. Please log in again and export a fresh request.",
			StatusCode: http.StatusUnauthorized,
		}
	}
	return fmt.Errorf("%s: %w", message, err)
}

func loginTokensFromInput(input map[string]string) (apiToken, csrfToken, apiVersion string) {
	curlText := input["cookie_header"]
	apiToken = normalizeBearerToken(input["api_token"])
	if apiToken == "" {
		apiToken = normalizeBearerToken(tumblr.HeaderValueFromText(curlText, "Authorization"))
	}
	csrfToken = normalizeOptionalHeaderCredential(input["csrf_token"])
	if csrfToken == "" {
		csrfToken = normalizeOptionalHeaderCredential(tumblr.HeaderValueFromText(curlText, "X-CSRF"))
	}
	if csrfToken == "" {
		csrfToken = normalizeOptionalHeaderCredential(tumblr.HeaderValueFromText(curlText, "X-CSRF-Token"))
	}
	apiVersion = normalizeOptionalHeaderCredential(input["api_version"])
	if apiVersion == "" {
		apiVersion = normalizeOptionalHeaderCredential(tumblr.HeaderValueFromText(curlText, "X-Version"))
	}
	return apiToken, csrfToken, apiVersion
}

func selectMessagingBlog(info *tumblr.UserInfoResponse) (*tumblr.Blog, error) {
	if info == nil {
		return nil, fmt.Errorf("tumblr account response was empty")
	}
	for i := range info.Blogs {
		if info.Blogs[i].CanMessage {
			if blog, ok := normalizedMessagingBlog(info.Blogs[i]); ok {
				return &blog, nil
			}
		}
	}
	for i := range info.Blogs {
		if info.Blogs[i].Primary {
			if blog, ok := normalizedMessagingBlog(info.Blogs[i]); ok {
				return &blog, nil
			}
		}
	}
	for i := range info.Blogs {
		if blog, ok := normalizedMessagingBlog(info.Blogs[i]); ok {
			return &blog, nil
		}
	}
	if len(info.Blogs) == 0 {
		return nil, fmt.Errorf("tumblr account has no blogs")
	}
	return nil, fmt.Errorf("tumblr account has no blogs with both name and uuid in a valid format")
}

func selectedBlogFromCurrentUser(info *tumblr.UserInfoResponse, meta *UserLoginMetadata) (*tumblr.Blog, error) {
	if info == nil {
		return nil, fmt.Errorf("tumblr account response was empty")
	}
	if meta == nil {
		return nil, fmt.Errorf("tumblr login metadata is missing")
	}
	for i := range info.Blogs {
		if info.Blogs[i].UUID != meta.SelectedBlogUUID {
			continue
		}
		blog, ok := normalizedMessagingBlog(info.Blogs[i])
		if !ok {
			return nil, fmt.Errorf("selected tumblr blog is missing name or uuid or has invalid identifiers")
		}
		return &blog, nil
	}
	return nil, fmt.Errorf("selected tumblr blog is not available in the current account")
}

func normalizedMessagingBlog(blog tumblr.Blog) (tumblr.Blog, bool) {
	blog.Name = tumblr.NormalizeBlogName(blog.Name)
	if blog.Name == "" || !validRemoteID(blog.UUID) {
		return tumblr.Blog{}, false
	}
	return blog, true
}

func userName(info *tumblr.UserInfoResponse, blog *tumblr.Blog) string {
	if info != nil && info.User != nil && info.User.Name != "" {
		if normalized := tumblr.NormalizeBlogName(info.User.Name); normalized != "" {
			return normalized
		}
	}
	if blog != nil {
		return blog.Name
	}
	return ""
}

func displayName(tc *TumblrConnector, blog *tumblr.Blog) string {
	if blog == nil {
		return ""
	}
	if tc == nil {
		return fallbackDisplayname(blog.Name, blog.Title)
	}
	return tc.Config.FormatDisplayname(blog.Name, blog.Title)
}

func normalizeBearerToken(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return ""
	}
	if strings.ContainsAny(input, "\r\n") {
		return ""
	}
	if len(input) < len("bearer") || !strings.EqualFold(input[:len("bearer")], "bearer") {
		return normalizeOptionalHeaderCredential(input)
	}
	remainder := strings.TrimSpace(input[len("bearer"):])
	if remainder == "" || remainder == input[len("bearer"):] {
		return normalizeOptionalHeaderCredential(input)
	}
	return normalizeOptionalHeaderCredential(remainder)
}
