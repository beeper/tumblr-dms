package connector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"github.com/beeper/tumblr-dms/pkg/tumblr"
)

func init() {
	status.BridgeStateHumanErrors.Update(status.BridgeStateErrorMap{
		"tumblr-bad-credentials":        "Tumblr couldn't verify the saved sign-in. Please sign in again to reconnect to Tumblr.",
		"tumblr-invalid-login-metadata": "Tumblr sign-in information is incomplete. Please sign in again to reconnect to Tumblr.",
		"tumblr-incomplete-session":     "Tumblr didn't return a usable session for the saved sign-in. Please sign in again to reconnect to Tumblr.",
		"tumblr-client-unavailable":     "Beeper couldn't start the Tumblr connection.",
		"tumblr-bootstrap-failed":       "Beeper couldn't verify the saved Tumblr sign-in because of an unexpected error.",
		"tumblr-current-user-failed":    "Beeper couldn't load your Tumblr account because of an unexpected error.",
		"tumblr-unavailable":            "Tumblr is temporarily unavailable. Beeper will keep trying to reconnect.",
		"tumblr-requests-forbidden":     "Tumblr is refusing requests from this account right now, so messages can't be sent or received. Beeper will reconnect when Tumblr allows it again.",
		"tumblr-sync-failed":            "Beeper is having trouble reaching Tumblr, so new messages may be delayed. Beeper will keep retrying.",
	})
}

func tumblrHTTPStatus(err error) int {
	var apiErr *tumblr.Error
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode
	}
	var bootstrapErr *tumblr.BootstrapError
	if errors.As(err, &bootstrapErr) {
		return bootstrapErr.StatusCode
	}
	return 0
}

// isTumblrTemporaryFailure reports failures that are expected to clear up on
// their own: rate limits, server errors and failures to reach Tumblr at all.
func isTumblrTemporaryFailure(err error) bool {
	if statusCode := tumblrHTTPStatus(err); statusCode != 0 {
		return statusCode == http.StatusTooManyRequests || statusCode >= http.StatusInternalServerError
	}
	var netErr net.Error
	return errors.As(err, &netErr) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded)
}

func isTumblrRequestsForbidden(err error) bool {
	return !tumblr.IsSessionInvalid(err) && tumblrHTTPStatus(err) == http.StatusForbidden
}

// wrapTumblrMatrixError maps a failure from a Matrix event handler to a
// message status.
func (tc *TumblrClient) wrapTumblrMatrixError(err error, fallback string) error {
	if err == nil || errors.As(err, &bridgev2.MessageStatus{}) {
		return err
	}
	return tc.tumblrMatrixErrorStatus(err, fallback)
}

func (tc *TumblrClient) tumblrMatrixErrorStatus(err error, fallback string) bridgev2.MessageStatus {
	wrapped := bridgev2.WrapErrorInStatus(err).WithSendNotice(true)
	// Only a definite rejection by Tumblr, or failing before any request, proves
	// the action didn't happen. Other failures may come after Tumblr acted.
	rejected := wrapped.WithIsCertain(true)
	if errors.Is(err, bridgev2.ErrNotLoggedIn) {
		return rejected.WithMessage("not connected to Tumblr")
	}
	if tumblr.IsSessionInvalid(err) {
		return rejected.
			WithErrorReason(event.MessageStatusNetworkError).
			WithMessage("Tumblr sign-in expired, reconnect Tumblr and try again")
	}
	var sendErr *tumblr.MessageSendError
	if errors.As(err, &sendErr) && sendErr.AuthRefreshFailed() {
		// The rejection may only have been caused by stale credentials, and
		// refreshing them failed for another reason, so the sign-in may still be valid.
		return rejected.WithMessage("couldn't refresh the Tumblr sign-in, try again")
	}
	var apiErr *tumblr.Error
	if !errors.As(err, &apiErr) {
		return wrapped.WithMessage(fallback)
	}
	switch {
	case apiErr.StatusCode == http.StatusForbidden && tc.remoteForbidden.Load():
		return rejected.
			WithErrorReason(event.MessageStatusNetworkError).
			WithMessage("Tumblr is refusing requests from this account right now, try again later")
	case apiErr.StatusCode == http.StatusForbidden:
		return rejected.
			WithErrorReason(event.MessageStatusNoPermission).
			WithStatus(event.MessageStatusFail).
			WithMessage("Tumblr didn't allow this")
	case apiErr.StatusCode == http.StatusNotFound:
		return rejected.
			WithErrorReason(event.MessageStatusNoPermission).
			WithStatus(event.MessageStatusFail).
			WithMessage("this Tumblr conversation is no longer available")
	case apiErr.StatusCode == http.StatusRequestEntityTooLarge:
		return rejected.
			WithErrorReason(event.MessageStatusUnsupported).
			WithStatus(event.MessageStatusFail).
			WithMessage("too large for Tumblr")
	case apiErr.StatusCode == http.StatusTooManyRequests:
		return rejected.
			WithErrorReason(event.MessageStatusNetworkError).
			WithMessage("Tumblr is rate limiting requests, try again later")
	case apiErr.StatusCode >= http.StatusInternalServerError:
		return wrapped.
			WithErrorReason(event.MessageStatusNetworkError).
			WithMessage("Tumblr is temporarily unavailable, try again later")
	case apiErr.StatusCode >= http.StatusBadRequest:
		return rejected.
			WithErrorReason(event.MessageStatusNetworkError).
			WithStatus(event.MessageStatusFail).
			WithMessage("Tumblr rejected the request")
	default:
		return wrapped.WithMessage(fallback)
	}
}

// wrapTumblrRespError maps a Tumblr failure in a provisioning method to a
// RespError, keeping the original error in the chain.
func wrapTumblrRespError(err error) error {
	if err == nil || errors.As(err, &bridgev2.RespError{}) || errors.Is(err, bridgev2.ErrNotLoggedIn) {
		return err
	}
	var mapped bridgev2.RespError
	switch statusCode := tumblrHTTPStatus(err); {
	case tumblr.IsSessionInvalid(err):
		mapped = bridgev2.RespError(mautrix.MForbidden.WithMessage("Tumblr sign-in expired. Reconnect Tumblr and try again."))
	case statusCode == http.StatusTooManyRequests:
		mapped = bridgev2.RespError(mautrix.MLimitExceeded.WithMessage("Tumblr is limiting requests right now. Try again later.").WithStatus(http.StatusBadRequest))
	case statusCode == http.StatusForbidden:
		mapped = bridgev2.RespError(mautrix.MForbidden.WithMessage("Tumblr didn't allow this request."))
	case isTumblrTemporaryFailure(err):
		mapped = bridgev2.RespError(mautrix.MUnknown.WithMessage("Tumblr couldn't be reached. Try again later."))
	default:
		mapped = bridgev2.RespError(mautrix.MUnknown.WithMessage("Tumblr returned an unexpected error."))
	}
	return fmt.Errorf("%w: %w", mapped, err)
}

func tumblrLoginSessionExpiredError() error {
	return bridgev2.RespError(mautrix.MBadState.WithMessage("This Tumblr sign-in expired. Start signing in again.").WithStatus(http.StatusBadRequest))
}
