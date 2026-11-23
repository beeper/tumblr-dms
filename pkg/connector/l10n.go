package connector

import "go.mau.fi/util/l10n"

var L = l10n.NewNamespace("tumblr-dms")

var (
	msgFlowPasswordName                           = L.Define("login.flow.password.name", "Email & password")
	msgFlowPasswordDescription                    = L.Define("login.flow.password.description", "Sign in to Tumblr through Beeper.")
	msgFlowBrowserName                            = L.Define("login.flow.browser.name", "Browser")
	msgFlowBrowserDescription                     = L.Define("login.flow.browser.description", "Sign in on tumblr.com. Works with Google and Apple sign-in.")
	msgFlowGoogleName                             = L.Define("login.flow.google.name", "Google")
	msgFlowGoogleDescription                      = L.Define("login.flow.google.description", "Sign in with the Google account linked to your Tumblr account.")
	msgFlowAppleName                              = L.Define("login.flow.apple.name", "Apple")
	msgFlowAppleDescription                       = L.Define("login.flow.apple.description", "Sign in with the Apple account linked to your Tumblr account.")
	msgStepRequestFailedInstructions              = L.Define("login.step.request_failed.instructions", "The request did not complete on this device. Please try again.")
	msgStepEmailRequiredInstructions              = L.Define("login.step.email_required.instructions", "Use the email address registered to your Tumblr account, not a blog name or username.")
	msgStepPreparingInstructions                  = L.Define("login.step.preparing.instructions", "Preparing Tumblr's secure sign-in.")
	msgStepTwoFactorInstructions                  = L.Define("login.step.two_factor.instructions", "Enter the code from your authenticator app, or use a single-use backup code.")
	msgStepTwoFactorRejectedInstructions          = L.Define("login.step.two_factor_rejected.instructions", "That authentication code was not accepted. Check the code and try again.")
	msgStepPasswordRejectedBrowserInstructions    = L.Define("login.step.password_rejected_browser.instructions", "Tumblr still did not accept that email and password. Sign in on tumblr.com instead. This works with Google and Apple sign-in too.")
	msgStepPasswordRejectedInstructions           = L.Define("login.step.password_rejected.instructions", "Tumblr did not accept that email and password. Check the registered account email and password and try again.")
	msgStepSignInFailedInstructions               = L.Define("login.step.sign_in_failed.instructions", "Tumblr couldn't complete that sign-in. Make sure you're using the registered account email, not a blog name, and try again.")
	msgStepBlogInstructions                       = L.Define("login.step.blog.instructions", "Choose the Tumblr inbox you want to connect.")
	msgStepVerificationFailedBrowserInstructions  = L.Define("login.step.verification_failed_browser.instructions", "Tumblr's secure sign-in check did not finish on this device. Sign in on tumblr.com instead. This works with Google and Apple sign-in too.")
	msgStepVerificationFailedInstructions         = L.Define("login.step.verification_failed.instructions", "Tumblr's secure sign-in check did not finish. Enter your credentials and try again.")
	msgStepVerificationBrowserChangedInstructions = L.Define("login.step.verification_browser_changed.instructions", "Tumblr's secure sign-in browser changed. Enter your credentials and try again.")
	msgStepNoBrowserIdentityInstructions          = L.Define("login.step.no_browser_identity.instructions", "Tumblr's sign-in window did not return a browser identity. Please try again.")
	msgFieldEmailName                             = L.Define("login.field.identifier.name", "Email")
	msgFieldPasswordName                          = L.Define("login.field.password.name", "Password")
	msgFieldTwoFactorName                         = L.Define("login.field.two_factor.name", "Authentication code")
	msgFieldBlogName                              = L.Define("login.field.blog.name", "Tumblr inbox")
	msgStepSignInInstructions                     = L.Define("login.step.sign_in.instructions", "Sign in to Tumblr in the window that opens, then open Messages. Beeper stores the session data needed to keep your DMs connected.")
	msgStepSignInGoogleInstructions               = L.Define("login.step.sign_in_google.instructions", "Sign in with Google in the window that opens. Beeper stores the session data needed to keep your Tumblr DMs connected.")
	msgStepSignInAppleInstructions                = L.Define("login.step.sign_in_apple.instructions", "Sign in with Apple in the window that opens. Beeper stores the session data needed to keep your Tumblr DMs connected.")
	msgStepReconnectInstructions                  = L.Define(
		"login.step.reconnect.instructions", "Sign in to Tumblr again to reconnect {blog}. This refreshes the saved session and keeps the same sending blog.",
		l10n.WithComment("blog is the Tumblr blog handle with a leading @, e.g. @exampleblog"),
	)
	msgStepReconnectBlogInstructions = L.Define("login.step.reconnect_blog.instructions", "Sign in to Tumblr again to reconnect this Tumblr blog. This refreshes the saved session and keeps the same sending blog.")
	msgStepCompleteInstructions      = L.Define(
		"login.step.complete.instructions", "Tumblr is connected as @{blog}.",
		l10n.WithComment("blog is the Tumblr blog name without a leading @, e.g. exampleblog"),
	)
	msgStepCredentialsInstructions        = L.Define("login.step.credentials.instructions", "Enter the email and password for your Tumblr account.")
	msgStepCredentialsMissingInstructions = L.Define("login.step.credentials_missing.instructions", "Enter both your Tumblr email and password.")
)
