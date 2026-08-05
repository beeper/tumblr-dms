package connector

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblrid"
)

const (
	loginFlowCookies = "cookies"

	loginStepIDCookies  = "com.ifixrobots.tumblr_dms.login.cookies"
	loginStepIDComplete = "com.ifixrobots.tumblr_dms.login.complete"

	loginInstructions = "Sign in to Tumblr in the window that opens, then open Messages. Beeper stores the session data needed to keep your DMs connected."

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
		return out.cookie_header || out.api_token ? out : null
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
	User     *bridgev2.User
	tc       *TumblrConnector
	flow     string
	override *bridgev2.UserLogin
}

var tumblrReauthLocks sync.Map

func lockTumblrReauthentication(loginID networkid.UserLoginID) func() {
	lockValue, _ := tumblrReauthLocks.LoadOrStore(loginID, &sync.Mutex{})
	lock := lockValue.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}

var _ bridgev2.LoginProcessCookies = (*TumblrLogin)(nil)
var _ bridgev2.LoginProcessWithOverride = (*TumblrLogin)(nil)

func (tc *TumblrConnector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{
		{
			Name:        "Sign in to Tumblr",
			Description: "Connect Tumblr DMs through Tumblr's sign-in page.",
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
	tl.override = nil
	return tl.cookieStep(loginInstructions), nil
}

func (tl *TumblrLogin) StartWithOverride(_ context.Context, override *bridgev2.UserLogin) (*bridgev2.LoginStep, error) {
	if err := tl.validateConnector(); err != nil {
		return nil, err
	}
	if override == nil || override.UserLogin == nil {
		return nil, fmt.Errorf("tumblr login to reconnect is missing")
	}
	if tl.User == nil || override.UserMXID != tl.User.MXID {
		return nil, fmt.Errorf("tumblr login does not belong to this matrix user")
	}
	tl.override = override
	loginName := "this Tumblr blog"
	if override != nil && strings.TrimSpace(override.RemoteName) != "" {
		loginName = "@" + strings.TrimPrefix(strings.TrimSpace(override.RemoteName), "@")
	}
	return tl.cookieStep(fmt.Sprintf("Sign in to Tumblr again to reconnect %s. This refreshes the saved session and keeps the same sending blog.", loginName)), nil
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
			Required: false,
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
	sessionCookies := tumblr.SessionCookiesFromMap(cookies)
	apiToken, csrfToken, apiVersion := loginTokensFromInput(cookies)
	if !tumblr.HasSessionCookies(sessionCookies) {
		return nil, tumblrIncompleteLoginError()
	}
	if tl.User == nil {
		return nil, fmt.Errorf("matrix user is required to complete tumblr login")
	}
	replacementLogin := tl.override
	if tl.override != nil {
		unlock := lockTumblrReauthentication(tl.override.ID)
		defer unlock()
	}

	client := tumblr.NewClient(tumblr.Options{
		SessionCookies: sessionCookies,
		APIToken:       apiToken,
		CSRFToken:      csrfToken,
		APIVersion:     apiVersion,
		UserAgent:      tl.tc.Config.BrowserUserAgent(),
		HTTPClient:     tl.tc.newHTTPClient(),
	})
	if err := client.Bootstrap(ctx); err != nil {
		tl.logValidationError(err)
		return nil, tumblrLoginValidationError(err)
	}
	if client.APIToken() == "" {
		return nil, tumblrIncompleteLoginError()
	}
	userInfo, err := client.CurrentUser(ctx)
	if err != nil {
		tl.logValidationError(err)
		return nil, tumblrLoginValidationError(err)
	}
	var blog *tumblr.Blog
	if tl.override != nil {
		var overrideMeta *UserLoginMetadata
		if oldClient, ok := tl.override.Client.(*TumblrClient); ok {
			overrideMeta, err = oldClient.loginMetadataSnapshot()
			if err != nil {
				return nil, fmt.Errorf("read saved tumblr login: %w", err)
			}
		} else if rawMeta, ok := tl.override.Metadata.(*UserLoginMetadata); ok && rawMeta != nil {
			overrideMeta = rawMeta.clone()
		}
		if overrideMeta == nil || !validRemoteID(strings.TrimSpace(overrideMeta.SelectedBlogUUID)) {
			return nil, tumblrReauthMismatchError("The saved Tumblr blog is missing its exact account ID. Remove it and connect it again.")
		}
		blog, err = selectedBlogFromCurrentUser(userInfo, overrideMeta)
		if err != nil {
			tl.logValidationError(err)
			return nil, tumblrReauthMismatchError("This sign-in does not include the same Tumblr blog. Sign in to the account that owns the saved blog.")
		}
	} else {
		blog, err = selectMessagingBlog(userInfo)
		if err != nil {
			tl.logValidationError(err)
			return nil, tumblrNoMessagingBlogError()
		}
	}

	snapshot := client.SessionSnapshot()
	if !tumblr.HasSessionCookies(snapshot.Cookies) {
		return nil, tumblrIncompleteLoginError()
	}
	meta := &UserLoginMetadata{
		SessionCookies:   snapshot.Cookies,
		APIToken:         snapshot.APIToken,
		CSRFToken:        snapshot.CSRFToken,
		APIVersion:       snapshot.APIVersion,
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
	if tl.override != nil && loginID != tl.override.ID {
		return nil, tumblrReauthMismatchError("This sign-in returned a different Tumblr blog. Sign in to the account that owns the saved blog.")
	}
	if replacementLogin == nil {
		unlock := lockTumblrReauthentication(loginID)
		defer unlock()
		existing, lookupErr := tl.User.Bridge.GetExistingUserLoginByID(ctx, loginID)
		if lookupErr != nil {
			return nil, fmt.Errorf("check for existing tumblr login: %w", lookupErr)
		}
		if existing != nil && existing.UserMXID == tl.User.MXID {
			replacementLogin = existing
		}
	}

	var previousClient bridgev2.NetworkAPI
	var previousMetadata any
	var previousRemoteName string
	var previousRemoteProfile status.RemoteProfile
	if replacementLogin != nil {
		previousClient = replacementLogin.Client
		if oldClient, ok := previousClient.(*TumblrClient); ok {
			oldClient.retireForReplacement()
		}
		if previousClient != nil {
			previousClient.Disconnect()
		}
		// Disconnect waits for the old generation's session and push workers, so
		// all reusable metadata is stable before NewLogin updates the shared login.
		previousMetadata = replacementLogin.Metadata
		if replacementMeta, ok := replacementLogin.Metadata.(*UserLoginMetadata); ok && replacementMeta != nil {
			previousMetadata = replacementMeta.clone()
			meta.PushKeys = replacementMeta.PushKeys.clone()
		}
		previousRemoteName = replacementLogin.RemoteName
		previousRemoteProfile = replacementLogin.RemoteProfile
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
		if replacementLogin != nil {
			replacementClient := replacementLogin.Client
			if replacementClient != nil && replacementClient != previousClient {
				if newClient, ok := replacementClient.(*TumblrClient); ok {
					newClient.retireForReplacement()
				}
				replacementClient.Disconnect()
			}
			replacementLogin.Metadata = previousMetadata
			replacementLogin.RemoteName = previousRemoteName
			replacementLogin.RemoteProfile = previousRemoteProfile
			replacementLogin.Client = previousClient
			if previousClient != nil {
				if oldClient, ok := previousClient.(*TumblrClient); ok {
					oldClient.reactivateAfterReplacementFailure()
				}
				go previousClient.Connect(tl.backgroundContext())
			}
		}
		return nil, err
	}
	go userLogin.Client.Connect(tl.backgroundContext())

	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       loginStepIDComplete,
		Instructions: fmt.Sprintf("Tumblr DMs is connected as @%s.", strings.TrimPrefix(remoteName, "@")),
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: userLogin.ID,
			UserLogin:   userLogin,
		},
	}, nil
}

func (tl *TumblrLogin) backgroundContext() context.Context {
	if tl != nil && tl.tc != nil && tl.tc.Bridge != nil && tl.tc.Bridge.BackgroundCtx != nil {
		return tl.tc.Bridge.BackgroundCtx
	}
	return context.Background()
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

func tumblrLoginValidationError(err error) error {
	if tumblr.IsAuthError(err) {
		return bridgev2.RespError{
			ErrCode:    "FI.MAU.TUMBLRDMS.BAD_CREDENTIALS",
			Err:        "Tumblr couldn't verify this sign-in. Please sign in again in the Tumblr window.",
			StatusCode: http.StatusUnauthorized,
		}
	}
	if tumblr.IsIncompleteSession(err) {
		return tumblrIncompleteLoginError()
	}
	if tumblr.IsForbidden(err) {
		return bridgev2.RespError{
			ErrCode:    "FI.MAU.TUMBLRDMS.LOGIN_FORBIDDEN",
			Err:        "Tumblr did not allow Beeper to finish signing in. Please try again later.",
			StatusCode: http.StatusForbidden,
		}
	}
	return bridgev2.RespError{
		ErrCode:    "FI.MAU.TUMBLRDMS.LOGIN_UNAVAILABLE",
		Err:        "Tumblr couldn't be reached to finish signing in. Please try again.",
		StatusCode: http.StatusBadGateway,
	}
}

func tumblrIncompleteLoginError() error {
	return bridgev2.RespError{
		ErrCode:    "FI.MAU.TUMBLRDMS.INCOMPLETE_LOGIN",
		Err:        "Tumblr sign-in did not finish. Keep the window open until Messages loads, then try again.",
		StatusCode: http.StatusBadRequest,
	}
}

func tumblrNoMessagingBlogError() error {
	return bridgev2.RespError{
		ErrCode:    "FI.MAU.TUMBLRDMS.NO_MESSAGING_BLOG",
		Err:        "This Tumblr account didn't return a blog that can use private messages.",
		StatusCode: http.StatusForbidden,
	}
}

func tumblrReauthMismatchError(message string) error {
	return bridgev2.RespError{
		ErrCode:    "FI.MAU.TUMBLRDMS.REAUTH_ACCOUNT_MISMATCH",
		Err:        message,
		StatusCode: http.StatusConflict,
	}
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
	if len(info.Blogs) == 0 {
		return nil, fmt.Errorf("tumblr account has no blogs")
	}
	return nil, fmt.Errorf("tumblr account has no valid blogs that can use private messages")
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
		if !info.Blogs[i].CanMessage {
			return nil, fmt.Errorf("selected tumblr blog cannot use private messages")
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
