package tumblr

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

const defaultLoginAPIVersion = "redpop/3/0//redpop/"

type PasswordLoginRequest struct {
	Identifier string `json:"username"`
	Password   string `json:"password"`
	TwoFactor  string `json:"tfa_token,omitempty"`

	GrantType         string `json:"grant_type"`
	CaptchaToken      string `json:"captcha_token"`
	BlackboxSessionID string `json:"blackbox_session_id,omitempty"`
}

func (c *Client) PrepareLogin(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.webBaseURL+"/login", nil)
	if err != nil {
		return err
	}
	c.setBrowserHeaders(req)
	resp, err := c.bootstrapHTTPClient().Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return &BootstrapError{Message: "tumblr login page request failed"}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &BootstrapError{Message: fmt.Sprintf("failed to load Tumblr login page: HTTP %d", resp.StatusCode)}
	}
	body, err := readLimitedBody(resp.Body, maxBootstrapPageBytes, "Tumblr login page")
	if err != nil {
		return err
	}
	if err = c.bootstrapFromHTML(string(body)); err != nil {
		return err
	}
	c.mu.Lock()
	if c.apiVersion == "" {
		c.apiVersion = defaultLoginAPIVersion
	}
	c.mu.Unlock()
	return nil
}

func (c *Client) PasswordLogin(ctx context.Context, req PasswordLoginRequest) error {
	req.Identifier = strings.TrimSpace(req.Identifier)
	if req.Identifier == "" || req.Password == "" {
		return errors.New("tumblr email and password are required")
	}
	req.GrantType = "password"
	previousCSRF := c.CSRFToken()
	err := c.doOnce(ctx, http.MethodPost, "/v2/oauth2/token", nil, req, nil)
	if !loginHasErrorCode(err, 1023) {
		return err
	}
	currentCSRF := c.CSRFToken()
	if currentCSRF == "" || currentCSRF == previousCSRF {
		return err
	}
	return c.doOnce(ctx, http.MethodPost, "/v2/oauth2/token", nil, req, nil)
}

func loginHasErrorCode(err error, code int) bool {
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		return false
	}
	for _, item := range apiErr.Errors {
		if item.Code == code {
			return true
		}
	}
	return false
}

func LoginNeedsTwoFactor(err error) bool {
	var apiErr *Error
	return errors.As(err, &apiErr) &&
		(apiErr.StatusCode == http.StatusConflict || apiErr.ErrorCode == "tfa_invalid")
}

func LoginNeedsCaptcha(err error) bool {
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.CaptchaRequired || apiErr.ErrorCode == "captcha_invalid"
}

func IsLoginInputError(err error) bool {
	var apiErr *Error
	return errors.As(err, &apiErr) &&
		(apiErr.StatusCode == http.StatusBadRequest || apiErr.StatusCode == http.StatusUnauthorized)
}

func LoginRequiresPasswordReset(err error) bool {
	var apiErr *Error
	return errors.As(err, &apiErr) && apiErr.ErrorCode == "password_reset"
}

func LoginRequiresConsent(err error) bool {
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusForbidden {
		return false
	}
	for _, item := range apiErr.Errors {
		if item.Code == 1027 {
			return true
		}
	}
	return false
}

func LoginAccountPendingDeletion(err error) bool {
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		return false
	}
	for _, item := range apiErr.Errors {
		if item.Code == 16015 {
			return true
		}
	}
	return false
}
