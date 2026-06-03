package tumblr

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

const maxErrorDetailRunes = 180

var (
	errorBearerPattern       = regexp.MustCompile(`(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+`)
	errorHeaderSecretPattern = regexp.MustCompile(`(?i)\b(authorization|cookie|set-cookie|x-csrf(?:-token)?)\s*[:=]\s*[^,\r\n]+`)
	errorTokenPattern        = regexp.MustCompile(`(?i)\b(api[_-]?token|csrf[_-]?token|oauth[_-]?token|access[_-]?token|refresh[_-]?token|token)\s*[:=]\s*[^,\s;)}\]]+`)
	errorTumblrIDPattern     = regexp.MustCompile(`(?i)\b(conversation[_-]?id|message[_-]?id|participants?|blog[_-]?(?:name|uuid)|blog|username|tumblelog|uuid)\s*[:=]\s*[^,\s;)}\]]+`)
	errorMsgPattern          = regexp.MustCompile(`(?i)\b(msg\s*[:=]\s*)[^,\r\n"')}\]]+`)
	errorJSONFieldPattern    = regexp.MustCompile(`(?i)("(?:conversation_id|conversationId|message_id|messageId|participants?|blog_name|blogName|blog_uuid|blogUUID|blog|username|tumblelog|uuid|name|text|message|body|msg)"\s*:\s*")([^"\\]*(?:\\.[^"\\]*)*)(")`)
	errorJSONArrayPattern    = regexp.MustCompile(`(?i)("(?:participants|participant|blog|username|tumblelog|msg)"\s*:\s*)\[[^\]]*\]`)
	errorEmailPattern        = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)
	errorURLPattern          = regexp.MustCompile(`https?://[^\s"'<>]+`)
	errorUUIDPattern         = regexp.MustCompile(`\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`)
	errorLongIDPattern       = regexp.MustCompile(`\b[0-9]{12,}\b`)
	errorLongSecretPattern   = regexp.MustCompile(`\b[A-Za-z0-9._~+/=-]{32,}\b`)
)

type Error struct {
	StatusCode int
	Status     string
	Errors     []APIError
	Body       string
}

func (e *Error) Error() string {
	if e == nil {
		return "tumblr API returned an empty error"
	}
	if len(e.Errors) > 0 {
		detail := e.Errors[0].Detail
		if detail == "" {
			detail = e.Errors[0].Title
		}
		detail = safeErrorDetail(detail)
		if detail == "" {
			detail = safeErrorDetail(e.Status)
		}
		if detail == "" {
			return fmt.Sprintf("tumblr API returned %d", e.StatusCode)
		}
		return fmt.Sprintf("tumblr API returned %d: %s", e.StatusCode, detail)
	}
	if status := safeErrorDetail(e.Status); status != "" {
		return fmt.Sprintf("tumblr API returned %d: %s", e.StatusCode, status)
	}
	return fmt.Sprintf("tumblr API returned %d", e.StatusCode)
}

func (e *Error) IsAuthError() bool {
	if e == nil {
		return false
	}
	if e.StatusCode == http.StatusUnauthorized || e.StatusCode == http.StatusForbidden {
		return true
	}
	for _, apiErr := range e.Errors {
		if apiErr.Logout {
			return true
		}
	}
	return false
}

func (e *Error) IsNotFound() bool {
	if e == nil {
		return false
	}
	return e.StatusCode == http.StatusNotFound
}

func IsAuthError(err error) bool {
	var apiErr *Error
	if errors.As(err, &apiErr) && apiErr.IsAuthError() {
		return true
	}
	var bootstrapErr *BootstrapError
	return errors.As(err, &bootstrapErr) && bootstrapErr.IsAuthError()
}

func IsNotFound(err error) bool {
	var apiErr *Error
	return errors.As(err, &apiErr) && apiErr.IsNotFound()
}

type BootstrapError struct {
	Message string
	Auth    bool
}

func (e *BootstrapError) Error() string {
	if e == nil {
		return "tumblr bootstrap returned an empty error"
	}
	return e.Message
}

func (e *BootstrapError) IsAuthError() bool {
	return e != nil && e.Auth
}

func safeErrorDetail(input string) string {
	detail := strings.Join(strings.Fields(input), " ")
	if detail == "" {
		return ""
	}
	detail = errorBearerPattern.ReplaceAllString(detail, "Bearer <redacted>")
	detail = errorHeaderSecretPattern.ReplaceAllString(detail, "$1: <redacted>")
	detail = errorTokenPattern.ReplaceAllString(detail, "$1=<redacted>")
	detail = errorTumblrIDPattern.ReplaceAllString(detail, "$1=<redacted>")
	detail = errorMsgPattern.ReplaceAllString(detail, "$1<redacted>")
	detail = errorJSONFieldPattern.ReplaceAllString(detail, "$1<redacted>$3")
	detail = errorJSONArrayPattern.ReplaceAllString(detail, `$1["<redacted>"]`)
	detail = errorEmailPattern.ReplaceAllString(detail, "<email>")
	detail = errorURLPattern.ReplaceAllString(detail, "<url>")
	detail = errorUUIDPattern.ReplaceAllString(detail, "<id>")
	detail = errorLongIDPattern.ReplaceAllString(detail, "<id>")
	detail = errorLongSecretPattern.ReplaceAllString(detail, "<redacted>")
	return truncateRunes(detail, maxErrorDetailRunes)
}

func safeErrorBody(body []byte) string {
	return safeErrorDetail(string(body))
}

func safeAPIMetaStatus(statusCode int, msg string) string {
	if strings.TrimSpace(msg) == "" {
		return ""
	}
	if statusText := http.StatusText(statusCode); statusText != "" {
		return statusText
	}
	if strings.EqualFold(strings.TrimSpace(msg), "OK") {
		return "OK"
	}
	return ""
}

func safeAPIErrors(errors []APIError) []APIError {
	if len(errors) == 0 {
		return nil
	}
	safeErrors := make([]APIError, len(errors))
	for i, apiErr := range errors {
		safeErrors[i] = APIError{
			Title:  safeErrorDetail(apiErr.Title),
			Code:   apiErr.Code,
			Detail: safeErrorDetail(apiErr.Detail),
			Logout: apiErr.Logout,
		}
	}
	return safeErrors
}

func truncateRunes(input string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	runes := []rune(input)
	if len(runes) <= maxRunes {
		return input
	}
	return string(runes[:maxRunes]) + "..."
}
