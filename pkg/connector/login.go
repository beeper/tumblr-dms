package connector

import (
	"context"
	"encoding/json"
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
	loginFlowPassword = "password"
	loginFlowCookies  = "cookies"

	loginStepIDCredentials         = "com.ifixrobots.tumblr_dms.login.credentials"
	loginStepIDTwoFactor           = "com.ifixrobots.tumblr_dms.login.two_factor"
	loginStepIDBrowserVerification = "com.ifixrobots.tumblr_dms.login.browser_verification"
	loginStepIDBlog                = "com.ifixrobots.tumblr_dms.login.blog"
	loginStepIDCookies             = "com.ifixrobots.tumblr_dms.login.cookies"
	loginStepIDComplete            = "com.ifixrobots.tumblr_dms.login.complete"

	loginFieldIdentifier = "identifier"
	loginFieldPassword   = "password"
	loginFieldTwoFactor  = "two_factor"
	loginFieldBlackbox   = "blackbox_session_id"
	loginFieldCaptcha    = "captcha_token"
	loginFieldBlog       = "blog"

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

	step            string
	client          *tumblr.Client
	userInfo        *tumblr.UserInfoResponse
	blogs           map[string]tumblr.Blog
	blogOptions     []string
	identifier      string
	password        string
	twoFactor       string
	captchaRequired bool
	browserAttempt  uint64
}

var tumblrReauthLocks sync.Map

func lockTumblrReauthentication(loginID networkid.UserLoginID) func() {
	lockValue, _ := tumblrReauthLocks.LoadOrStore(loginID, &sync.Mutex{})
	lock := lockValue.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}

var _ bridgev2.LoginProcessCookies = (*TumblrLogin)(nil)
var _ bridgev2.LoginProcessUserInput = (*TumblrLogin)(nil)
var _ bridgev2.LoginProcessWithOverride = (*TumblrLogin)(nil)

func (tc *TumblrConnector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{
		{
			Name:        "Email & password",
			Description: "Sign in through Beeper without opening Tumblr's full website.",
			ID:          loginFlowPassword,
		},
		{
			Name:        "Browser sign-in",
			Description: "Sign in through Tumblr's website.",
			ID:          loginFlowCookies,
		},
	}
}

func (tc *TumblrConnector) CreateLogin(_ context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	if flowID == "" {
		flowID = loginFlowPassword
	}
	if flowID != loginFlowPassword && flowID != loginFlowCookies {
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
	tl.reset()
	if tl.flow == loginFlowPassword {
		return tl.credentialsStep("Enter the email and password for your Tumblr account."), nil
	}
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
	tl.reset()
	tl.override = override
	loginName := "this Tumblr blog"
	if override != nil && strings.TrimSpace(override.RemoteName) != "" {
		loginName = "@" + strings.TrimPrefix(strings.TrimSpace(override.RemoteName), "@")
	}
	instructions := fmt.Sprintf("Sign in to Tumblr again to reconnect %s. This refreshes the saved session and keeps the same sending blog.", loginName)
	if tl.flow == loginFlowPassword {
		return tl.credentialsStep(instructions), nil
	}
	return tl.cookieStep(instructions), nil
}

func (tl *TumblrLogin) reset() {
	tl.step = ""
	tl.clearPendingAuthentication()
	tl.browserAttempt = 0
	tl.override = nil
}

func (tl *TumblrLogin) clearPendingAuthentication() {
	tl.client = nil
	tl.userInfo = nil
	tl.blogs = nil
	tl.blogOptions = nil
	tl.identifier = ""
	tl.password = ""
	tl.twoFactor = ""
	tl.captchaRequired = false
}

func (tl *TumblrLogin) credentialsStep(instructions string) *bridgev2.LoginStep {
	tl.step = loginStepIDCredentials
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeUserInput,
		StepID:       loginStepIDCredentials,
		Instructions: instructions,
		UserInputParams: &bridgev2.LoginUserInputParams{Fields: []bridgev2.LoginInputDataField{
			{
				Type: bridgev2.LoginInputFieldTypeEmail,
				ID:   loginFieldIdentifier,
				Name: "Email",
			},
			{
				Type: bridgev2.LoginInputFieldTypePassword,
				ID:   loginFieldPassword,
				Name: "Password",
			},
		}},
	}
}

func (tl *TumblrLogin) twoFactorStep(instructions string) *bridgev2.LoginStep {
	tl.step = loginStepIDTwoFactor
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeUserInput,
		StepID:       loginStepIDTwoFactor,
		Instructions: instructions,
		UserInputParams: &bridgev2.LoginUserInputParams{Fields: []bridgev2.LoginInputDataField{
			{
				Type: bridgev2.LoginInputFieldType2FACode,
				ID:   loginFieldTwoFactor,
				Name: "Authentication code",
			},
		}},
	}
}

func (tl *TumblrLogin) browserVerificationStep(instructions string) (*bridgev2.LoginStep, error) {
	if tl.client == nil {
		return nil, fmt.Errorf("tumblr browser verification client is missing")
	}
	tl.browserAttempt++
	attempt := tl.browserAttempt
	extractJS, err := tumblrBrowserVerificationExtractionJS(tl.client.RecaptchaSiteKey(), tl.captchaRequired, attempt)
	if err != nil {
		return nil, err
	}
	fields := []bridgev2.LoginCookieField{{
		ID:       loginFieldBlackbox,
		Required: true,
		Sources: []bridgev2.LoginCookieFieldSource{{
			Type: bridgev2.LoginCookieTypeSpecial,
			Name: loginFieldBlackbox,
		}},
	}}
	if tl.captchaRequired {
		fields = append(fields, bridgev2.LoginCookieField{
			ID:       loginFieldCaptcha,
			Required: true,
			Sources: []bridgev2.LoginCookieFieldSource{{
				Type: bridgev2.LoginCookieTypeSpecial,
				Name: loginFieldCaptcha,
			}},
		})
	}
	tl.step = loginStepIDBrowserVerification
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeCookies,
		StepID:       loginStepIDBrowserVerification,
		Instructions: instructions,
		CookiesParams: &bridgev2.LoginCookiesParams{
			URL:       fmt.Sprintf("https://www.tumblr.com/login#beeper-native-login-%d", attempt),
			UserAgent: tl.tc.Config.BrowserUserAgent(),
			Hidden:    true,
			ExtractJS: extractJS,
			Fields:    fields,
		},
	}, nil
}

func tumblrBrowserVerificationExtractionJS(siteKey string, needsCaptcha bool, attempt uint64) (string, error) {
	siteKey = strings.TrimSpace(siteKey)
	if needsCaptcha && siteKey == "" {
		return "", fmt.Errorf("tumblr login did not provide a CAPTCHA site key")
	}
	keyJSON, err := json.Marshal(siteKey)
	if err != nil {
		return "", fmt.Errorf("encode Tumblr CAPTCHA site key: %w", err)
	}
	return fmt.Sprintf(`new Promise((resolve, reject) => {
	const attempt = %d
	if (window.__mautrixTumblrVerificationAttempt === attempt && window.__mautrixTumblrVerificationPromise) {
		window.__mautrixTumblrVerificationPromise.then(resolve, reject)
		return
	}
	const siteKey = %s
	const needsCaptcha = %t
	window.__mautrixTumblrVerificationAttempt = attempt
	window.__mautrixTumblrVerificationPromise = (async () => {
		const collectBlackbox = async () => {
			const deadline = Date.now() + 6000
			while (!(window.Blackbox && window.Blackbox.initialized === true)) {
				if (Date.now() >= deadline) {
					throw new Error('Tumblr browser verification did not initialize')
				}
				await new Promise(resolve => setTimeout(resolve, 100))
			}
			if (window.__mautrixTumblrBlackboxConsumed && typeof window.Blackbox.reset === 'function') {
				await Promise.race([
					window.Blackbox.reset(),
					new Promise(resolve => setTimeout(resolve, 6000)),
				])
			}
			const result = await Promise.race([
				window.Blackbox.collect(),
				new Promise(resolve => setTimeout(() => resolve(undefined), 6000)),
			])
			const sessionID = typeof result?.sessionId === 'string' ? result.sessionId.trim() : ''
			if (!sessionID) throw new Error('Tumblr browser verification returned an empty session')
			window.__mautrixTumblrBlackboxConsumed = true
			return sessionID
		}

		const collectCaptcha = () => new Promise((resolveCaptcha, rejectCaptcha) => {
			if (window.grecaptcha?.enterprise) {
				window.grecaptcha.enterprise.ready(() => {
					window.grecaptcha.enterprise.execute(siteKey, { action: 'login' }).then(resolveCaptcha, rejectCaptcha)
				})
				return
			}
			window.___grecaptcha_cfg = window.___grecaptcha_cfg || { fns: [] }
			window.___grecaptcha_cfg.fns = window.___grecaptcha_cfg.fns || []
			window.___grecaptcha_cfg.fns.push(() => {
				if (!window.grecaptcha?.enterprise) {
					rejectCaptcha(new Error('Tumblr CAPTCHA did not load'))
					return
				}
				window.grecaptcha.enterprise.ready(() => {
					window.grecaptcha.enterprise.execute(siteKey, { action: 'login' }).then(resolveCaptcha, rejectCaptcha)
				})
			})
			const scriptID = 'recaptcha-' + siteKey
			if (document.getElementById(scriptID)) return
			const script = document.createElement('script')
			script.id = scriptID
			script.src = 'https://www.recaptcha.net/recaptcha/enterprise.js?render=' + encodeURIComponent(siteKey)
			script.async = true
			script.defer = true
			script.onerror = () => rejectCaptcha(new Error('Failed to load Tumblr CAPTCHA'))
			(document.head || document.documentElement).append(script)
		})

		const empty = { blackbox_session_id: '' }
		if (needsCaptcha) empty.captcha_token = ''
		try {
			const [blackboxSessionID, captchaToken] = await Promise.all([
				collectBlackbox(),
				needsCaptcha ? collectCaptcha() : Promise.resolve(''),
			])
			const result = { blackbox_session_id: blackboxSessionID }
			if (needsCaptcha) {
				if (typeof captchaToken !== 'string' || captchaToken.trim() === '') {
					return empty
				}
				result.captcha_token = captchaToken.trim()
			}
			return result
		} catch {
			return empty
		}
	})()
	window.__mautrixTumblrVerificationPromise.then(resolve, reject)
})`, attempt, string(keyJSON), needsCaptcha), nil
}

func (tl *TumblrLogin) blogStep(instructions string) *bridgev2.LoginStep {
	tl.step = loginStepIDBlog
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeUserInput,
		StepID:       loginStepIDBlog,
		Instructions: instructions,
		UserInputParams: &bridgev2.LoginUserInputParams{Fields: []bridgev2.LoginInputDataField{
			{
				Type:    bridgev2.LoginInputFieldTypeSelect,
				ID:      loginFieldBlog,
				Name:    "Tumblr inbox",
				Options: append([]string(nil), tl.blogOptions...),
			},
		}},
	}
}

func (tl *TumblrLogin) cookieStep(instructions string) *bridgev2.LoginStep {
	fields := []bridgev2.LoginCookieField{
		{
			ID:       "cookie_header",
			Required: true,
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

func (tl *TumblrLogin) Cancel() {
	tl.reset()
}

func (tl *TumblrLogin) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	if err := tl.validateConnector(); err != nil {
		return nil, err
	}
	switch tl.step {
	case loginStepIDCredentials:
		return tl.submitCredentials(ctx, input)
	case loginStepIDTwoFactor:
		return tl.submitTwoFactor(ctx, input)
	case loginStepIDBlog:
		return tl.submitBlog(ctx, input)
	default:
		return nil, fmt.Errorf("unexpected tumblr login step")
	}
}

func (tl *TumblrLogin) submitCredentials(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	identifier := strings.TrimSpace(input[loginFieldIdentifier])
	password := input[loginFieldPassword]
	if identifier == "" || password == "" {
		return tl.credentialsStep("Enter both your Tumblr email and password."), nil
	}

	client := tumblr.NewClient(tumblr.Options{
		UserAgent:  tl.tc.Config.BrowserUserAgent(),
		HTTPClient: tl.tc.newHTTPClient(),
	})
	if err := client.PrepareLogin(ctx); err != nil {
		tl.logValidationError(err)
		return nil, tumblrLoginValidationError(err)
	}
	tl.client = client
	tl.identifier = identifier
	tl.password = password
	tl.twoFactor = ""
	tl.captchaRequired = false
	return tl.browserVerificationStep("Preparing Tumblr's secure sign-in.")
}

func (tl *TumblrLogin) submitTwoFactor(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	code := strings.TrimSpace(input[loginFieldTwoFactor])
	if code == "" {
		return tl.twoFactorStep("Enter the code from your authenticator app, or use a single-use backup code."), nil
	}
	tl.twoFactor = code
	return tl.browserVerificationStep("Preparing Tumblr's secure sign-in.")
}

func (tl *TumblrLogin) submitPassword(ctx context.Context, blackboxSessionID, captchaToken string) (*bridgev2.LoginStep, error) {
	if tl.client == nil || tl.identifier == "" || tl.password == "" {
		return nil, fmt.Errorf("tumblr password login is not ready")
	}
	err := tl.client.PasswordLogin(ctx, tumblr.PasswordLoginRequest{
		Identifier:        tl.identifier,
		Password:          tl.password,
		TwoFactor:         tl.twoFactor,
		CaptchaToken:      captchaToken,
		BlackboxSessionID: blackboxSessionID,
	})
	if err != nil {
		needsTwoFactor := tumblr.LoginNeedsTwoFactor(err)
		needsCaptcha := tumblr.LoginNeedsCaptcha(err)
		switch {
		case tumblr.LoginRequiresPasswordReset(err):
			tl.clearPendingAuthentication()
			return nil, tumblrPasswordResetRequiredError()
		case tumblr.LoginRequiresConsent(err):
			tl.clearPendingAuthentication()
			return nil, tumblrConsentRequiredError()
		case tumblr.LoginAccountPendingDeletion(err):
			tl.clearPendingAuthentication()
			return nil, tumblrAccountPendingDeletionError()
		case needsTwoFactor && tl.twoFactor != "":
			tl.captchaRequired = needsCaptcha
			tl.twoFactor = ""
			return tl.twoFactorStep("That authentication code was not accepted. Check the code and try again."), nil
		case needsTwoFactor:
			tl.captchaRequired = needsCaptcha
			return tl.twoFactorStep("Enter the code from your authenticator app, or use a single-use backup code."), nil
		case needsCaptcha:
			tl.captchaRequired = true
			return tl.browserVerificationStep("Preparing Tumblr's secure sign-in.")
		case tumblr.IsLoginInputError(err) && tl.twoFactor != "":
			tl.twoFactor = ""
			return tl.twoFactorStep("That authentication code was not accepted. Check the code and try again."), nil
		case tumblr.IsLoginInputError(err):
			tl.clearPendingAuthentication()
			return tl.credentialsStep("Tumblr did not accept those credentials. Check them and try again."), nil
		default:
			tl.clearPendingAuthentication()
			tl.logValidationError(err)
			return nil, tumblrLoginValidationError(err)
		}
	}
	tl.identifier = ""
	tl.password = ""
	tl.twoFactor = ""
	return tl.finishAuthentication(ctx, tl.client)
}

func (tl *TumblrLogin) submitBlog(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	selection := strings.TrimSpace(input[loginFieldBlog])
	blog, ok := tl.blogs[selection]
	if !ok {
		return tl.blogStep("Choose the Tumblr inbox you want to connect."), nil
	}
	if tl.client == nil || tl.userInfo == nil {
		return nil, fmt.Errorf("tumblr login session expired before inbox selection")
	}
	return tl.completeLogin(ctx, tl.client, tl.userInfo, &blog)
}

func (tl *TumblrLogin) SubmitCookies(ctx context.Context, cookies map[string]string) (*bridgev2.LoginStep, error) {
	if tl.step == loginStepIDBrowserVerification {
		return tl.submitBrowserVerification(ctx, cookies)
	}
	return tl.submitCookieInput(ctx, cookies)
}

func (tl *TumblrLogin) submitBrowserVerification(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	blackboxSessionID := strings.TrimSpace(input[loginFieldBlackbox])
	captchaToken := strings.TrimSpace(input[loginFieldCaptcha])
	if blackboxSessionID == "" || (tl.captchaRequired && captchaToken == "") {
		tl.clearPendingAuthentication()
		return tl.credentialsStep("Tumblr's secure sign-in check did not finish. Enter your credentials and try again, or use Browser sign-in."), nil
	}
	tl.captchaRequired = false
	return tl.submitPassword(ctx, blackboxSessionID, captchaToken)
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
	client := tumblr.NewClient(tumblr.Options{
		SessionCookies: sessionCookies,
		APIToken:       apiToken,
		CSRFToken:      csrfToken,
		APIVersion:     apiVersion,
		UserAgent:      tl.tc.Config.BrowserUserAgent(),
		HTTPClient:     tl.tc.newHTTPClient(),
	})
	return tl.finishAuthentication(ctx, client)
}

func (tl *TumblrLogin) finishAuthentication(ctx context.Context, client *tumblr.Client) (*bridgev2.LoginStep, error) {
	if client == nil {
		return nil, fmt.Errorf("tumblr login client is missing")
	}
	if tl.User == nil {
		return nil, fmt.Errorf("matrix user is required to complete tumblr login")
	}
	if tl.override != nil {
		unlock := lockTumblrReauthentication(tl.override.ID)
		defer unlock()
	}
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
		blog, err := selectedBlogFromCurrentUser(userInfo, overrideMeta)
		if err != nil {
			tl.logValidationError(err)
			return nil, tumblrReauthMismatchError("This sign-in does not include the same Tumblr blog. Sign in to the account that owns the saved blog.")
		}
		return tl.completeLogin(ctx, client, userInfo, blog)
	}

	blogs := messagingBlogs(userInfo)
	if len(blogs) == 0 {
		err = fmt.Errorf("tumblr account has no valid blogs that can use private messages")
		tl.logValidationError(err)
		return nil, tumblrNoMessagingBlogError()
	}
	if len(blogs) == 1 {
		return tl.completeLogin(ctx, client, userInfo, &blogs[0])
	}

	tl.client = client
	tl.userInfo = userInfo
	tl.blogs = make(map[string]tumblr.Blog, len(blogs))
	tl.blogOptions = make([]string, 0, len(blogs))
	for _, blog := range blogs {
		label := "@" + blog.Name
		tl.blogs[label] = blog
		tl.blogOptions = append(tl.blogOptions, label)
	}
	return tl.blogStep("Choose the Tumblr inbox you want to connect."), nil
}

func (tl *TumblrLogin) completeLogin(ctx context.Context, client *tumblr.Client, userInfo *tumblr.UserInfoResponse, blog *tumblr.Blog) (*bridgev2.LoginStep, error) {
	if blog == nil {
		return nil, fmt.Errorf("tumblr blog selection is missing")
	}
	replacementLogin := tl.override
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
	var conflictingLogin *bridgev2.UserLogin
	if replacementLogin == nil {
		unlock := lockTumblrReauthentication(loginID)
		defer unlock()
		existing, lookupErr := tl.User.Bridge.GetExistingUserLoginByID(ctx, loginID)
		if lookupErr != nil {
			return nil, fmt.Errorf("check for existing tumblr login: %w", lookupErr)
		}
		if existing != nil && existing.UserMXID == tl.User.MXID {
			replacementLogin = existing
		} else if existing != nil {
			conflictingLogin = existing
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
	} else if conflictingLogin != nil {
		if oldClient, ok := conflictingLogin.Client.(*TumblrClient); ok {
			oldClient.retireForReplacement()
		}
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
	tl.reset()

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
			Err:        "Tumblr couldn't verify this sign-in. Check your credentials and try again.",
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

func tumblrPasswordResetRequiredError() error {
	return bridgev2.RespError{
		ErrCode:    "FI.MAU.TUMBLRDMS.PASSWORD_RESET_REQUIRED",
		Err:        "Tumblr requires this account's password to be reset before it can sign in.",
		StatusCode: http.StatusForbidden,
	}
}

func tumblrConsentRequiredError() error {
	return bridgev2.RespError{
		ErrCode:    "FI.MAU.TUMBLRDMS.CONSENT_REQUIRED",
		Err:        "Tumblr requires account consent to be completed on tumblr.com before signing in.",
		StatusCode: http.StatusForbidden,
	}
}

func tumblrAccountPendingDeletionError() error {
	return bridgev2.RespError{
		ErrCode:    "FI.MAU.TUMBLRDMS.ACCOUNT_PENDING_DELETION",
		Err:        "This Tumblr account is pending deletion and can't be connected.",
		StatusCode: http.StatusForbidden,
	}
}

func tumblrIncompleteLoginError() error {
	return bridgev2.RespError{
		ErrCode:    "FI.MAU.TUMBLRDMS.INCOMPLETE_LOGIN",
		Err:        "Tumblr sign-in did not return a usable session. Try again.",
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

func messagingBlogs(info *tumblr.UserInfoResponse) []tumblr.Blog {
	if info == nil {
		return nil
	}
	blogs := make([]tumblr.Blog, 0, len(info.Blogs))
	seen := make(map[string]struct{}, len(info.Blogs))
	for _, candidate := range info.Blogs {
		if !candidate.CanMessage || candidate.IsGroupChannel {
			continue
		}
		blog, ok := normalizedMessagingBlog(candidate)
		if !ok {
			continue
		}
		if _, duplicate := seen[blog.UUID]; duplicate {
			continue
		}
		seen[blog.UUID] = struct{}{}
		blogs = append(blogs, blog)
	}
	sort.Slice(blogs, func(i, j int) bool {
		if blogs[i].Primary != blogs[j].Primary {
			return blogs[i].Primary
		}
		return blogs[i].Name < blogs[j].Name
	})
	return blogs
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
		if !info.Blogs[i].CanMessage || info.Blogs[i].IsGroupChannel {
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
