package connector

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	pushreceiver "github.com/beeper/push-receiver"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/ifixrobots/tumblr-dms/pkg/msgconv"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
)

func (tc *TumblrConnector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{
		Portal: func() any {
			return &PortalMetadata{}
		},
		Message: func() any {
			return &MessageMetadata{}
		},
		UserLogin: func() any {
			return &UserLoginMetadata{}
		},
	}
}

type PortalMetadata struct {
	ConversationID         string   `json:"conversation_id,omitempty"`
	PendingParticipantIDs  []string `json:"pending_participant_ids,omitempty"`
	PendingParticipantName string   `json:"pending_participant_name,omitempty"`
	ParticipantHash        string   `json:"participant_hash,omitempty"`
}

type MessageMetadata = msgconv.MessageMetadata

type UserLoginMetadata struct {
	SessionCookies   map[string]string `json:"session_cookies,omitempty"`
	APIToken         string            `json:"api_token,omitempty"`
	CSRFToken        string            `json:"csrf_token,omitempty"`
	APIVersion       string            `json:"api_version,omitempty"`
	UserName         string            `json:"user_name,omitempty"`
	SelectedBlogName string            `json:"selected_blog_name,omitempty"`
	SelectedBlogUUID string            `json:"selected_blog_uuid,omitempty"`
	PushKeys         *PushKeys         `json:"push_keys,omitempty"`
}

type PushKeys struct {
	Active   *PushRegistration   `json:"active,omitempty"`
	Pending  *PushRegistration   `json:"pending,omitempty"`
	Retiring []*PushRegistration `json:"retiring,omitempty"`
	P256DH   []byte              `json:"p256dh,omitempty"`
	Auth     []byte              `json:"auth,omitempty"`
	Private  []byte              `json:"private,omitempty"`
}

type PushRegistration struct {
	Token           string   `json:"token,omitempty"`
	FCMAppID        string   `json:"fcm_app_id,omitempty"`
	AndroidID       string   `json:"android_id,omitempty"`
	SecurityToken   string   `json:"security_token,omitempty"`
	LastCheckinTS   int64    `json:"last_checkin_ts,omitempty"`
	FCMRegisteredTS int64    `json:"fcm_registered_ts,omitempty"`
	PersistentIDs   []string `json:"persistent_ids,omitempty"`
}

func (m *UserLoginMetadata) clone() *UserLoginMetadata {
	if m == nil {
		return nil
	}
	cloned := *m
	cloned.SessionCookies = make(map[string]string, len(m.SessionCookies))
	for name, value := range m.SessionCookies {
		cloned.SessionCookies[name] = value
	}
	cloned.PushKeys = m.PushKeys.clone()
	return &cloned
}

func (r *PushRegistration) clone() *PushRegistration {
	if r == nil {
		return nil
	}
	cloned := *r
	cloned.PersistentIDs = append([]string(nil), r.PersistentIDs...)
	return &cloned
}

func (keys *PushKeys) clone() *PushKeys {
	if keys == nil {
		return nil
	}
	cloned := *keys
	cloned.Active = keys.Active.clone()
	cloned.Pending = keys.Pending.clone()
	cloned.Retiring = make([]*PushRegistration, 0, len(keys.Retiring))
	for _, registration := range keys.Retiring {
		if registration != nil {
			cloned.Retiring = append(cloned.Retiring, registration.clone())
		}
	}
	cloned.P256DH = append([]byte(nil), keys.P256DH...)
	cloned.Auth = append([]byte(nil), keys.Auth...)
	cloned.Private = append([]byte(nil), keys.Private...)
	return &cloned
}

func (m *UserLoginMetadata) ensurePushKeys() (bool, error) {
	if m == nil {
		return false, fmt.Errorf("tumblr login metadata is missing")
	}
	if m.PushKeys != nil && len(m.PushKeys.P256DH) > 0 && len(m.PushKeys.Auth) == 16 && len(m.PushKeys.Private) > 0 {
		return false, nil
	}
	privateKey, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return false, fmt.Errorf("failed to generate web push key: %w", err)
	}
	authSecret := make([]byte, 16)
	if _, err = rand.Read(authSecret); err != nil {
		return false, fmt.Errorf("failed to generate web push auth secret: %w", err)
	}
	m.PushKeys = &PushKeys{
		P256DH:  privateKey.PublicKey().Bytes(),
		Auth:    authSecret,
		Private: privateKey.Bytes(),
	}
	return true, nil
}

func (r *PushRegistration) credentials() (*pushreceiver.GCMCredentials, error) {
	if r == nil || strings.TrimSpace(r.AndroidID) == "" || strings.TrimSpace(r.SecurityToken) == "" {
		return nil, nil
	}
	androidID, err := strconv.ParseUint(strings.TrimSpace(r.AndroidID), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("tumblr push receiver Android ID is invalid: %w", err)
	}
	securityToken, err := strconv.ParseUint(strings.TrimSpace(r.SecurityToken), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("tumblr push receiver security token is invalid: %w", err)
	}
	return &pushreceiver.GCMCredentials{
		AndroidID:     androidID,
		SecurityToken: securityToken,
	}, nil
}

func (r *PushRegistration) setCredentials(creds *pushreceiver.GCMCredentials) {
	if r == nil || creds == nil {
		return
	}
	r.AndroidID = strconv.FormatUint(creds.AndroidID, 10)
	r.SecurityToken = strconv.FormatUint(creds.SecurityToken, 10)
}

func (m *UserLoginMetadata) encodedPushKeys() (p256dh, auth string, err error) {
	if m == nil || m.PushKeys == nil {
		return "", "", fmt.Errorf("tumblr web push keys are missing")
	}
	if len(m.PushKeys.P256DH) == 0 || len(m.PushKeys.Auth) == 0 {
		return "", "", fmt.Errorf("tumblr web push public key or auth secret is missing")
	}
	return base64.RawURLEncoding.EncodeToString(m.PushKeys.P256DH),
		base64.RawURLEncoding.EncodeToString(m.PushKeys.Auth),
		nil
}

func (m *UserLoginMetadata) pushPrivateKey() (*ecdh.PrivateKey, error) {
	if m == nil || m.PushKeys == nil || len(m.PushKeys.Private) == 0 {
		return nil, fmt.Errorf("tumblr web push private key is missing")
	}
	privateKey, err := ecdh.P256().NewPrivateKey(m.PushKeys.Private)
	if err != nil {
		return nil, fmt.Errorf("tumblr web push private key is invalid: %w", err)
	}
	return privateKey, nil
}

func (m *UserLoginMetadata) String() string {
	if m == nil {
		return "UserLoginMetadata<nil>"
	}
	return fmt.Sprintf(
		"UserLoginMetadata{session_cookies:%s api_token:%s csrf_token:%s api_version:%s user_name:%s selected_blog_name:%s selected_blog_uuid:%s push_keys:%s}",
		redactedSessionCookiesValue(m.SessionCookies),
		redactedMetadataValue(m.APIToken),
		redactedMetadataValue(m.CSRFToken),
		redactedMetadataValue(m.APIVersion),
		redactedMetadataValue(m.UserName),
		redactedMetadataValue(m.SelectedBlogName),
		redactedMetadataValue(m.SelectedBlogUUID),
		redactedPushKeysValue(m.PushKeys),
	)
}

func (m *UserLoginMetadata) GoString() string {
	return m.String()
}

func (m *UserLoginMetadata) MarshalZerologObject(e *zerolog.Event) {
	if m == nil {
		e.Str("value", "<nil>")
		return
	}
	e.Int("session_cookie_count", len(tumblr.NormalizeSessionCookies(m.SessionCookies))).
		Str("api_token", redactedMetadataValue(m.APIToken)).
		Str("csrf_token", redactedMetadataValue(m.CSRFToken)).
		Str("api_version", redactedMetadataValue(m.APIVersion)).
		Str("user_name", redactedMetadataValue(m.UserName)).
		Str("selected_blog_name", redactedMetadataValue(m.SelectedBlogName)).
		Str("selected_blog_uuid", redactedMetadataValue(m.SelectedBlogUUID)).
		Bool("has_push_keys", m.PushKeys != nil).
		Bool("has_active_push_registration", m.PushKeys != nil && m.PushKeys.Active != nil).
		Bool("has_pending_push_registration", m.PushKeys != nil && m.PushKeys.Pending != nil).
		Int("retiring_push_registration_count", retiringPushRegistrationCount(m.PushKeys))
}

func retiringPushRegistrationCount(keys *PushKeys) int {
	if keys == nil {
		return 0
	}
	return len(keys.Retiring)
}

func redactedMetadataValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return "<empty>"
	}
	return "<redacted>"
}

func redactedPushKeysValue(keys *PushKeys) string {
	if keys == nil {
		return "<empty>"
	}
	return "<redacted>"
}

func redactedSessionCookiesValue(cookies map[string]string) string {
	if len(tumblr.NormalizeSessionCookies(cookies)) == 0 {
		return "<empty>"
	}
	return "<redacted>"
}

func validateUserLoginMetadata(raw any) (*UserLoginMetadata, error) {
	if _, err := normalizedUserLoginMetadata(raw); err != nil {
		return nil, err
	}
	return raw.(*UserLoginMetadata), nil
}

func normalizeUserLoginMetadata(raw any) (*UserLoginMetadata, error) {
	normalized, err := normalizedUserLoginMetadata(raw)
	if err != nil {
		return nil, err
	}
	meta := raw.(*UserLoginMetadata)
	*meta = *normalized
	return meta, nil
}

func normalizedUserLoginMetadata(raw any) (*UserLoginMetadata, error) {
	meta, ok := raw.(*UserLoginMetadata)
	if !ok || meta == nil {
		return nil, fmt.Errorf("tumblr login metadata is missing")
	}
	normalized := *meta
	sessionCookies := tumblr.NormalizeSessionCookies(meta.SessionCookies)
	apiToken := normalizeBearerToken(meta.APIToken)
	if !tumblr.HasSessionCookies(sessionCookies) {
		return nil, fmt.Errorf("tumblr session cookies are missing")
	}
	selectedBlogName := strings.TrimSpace(meta.SelectedBlogName)
	if selectedBlogName == "" {
		return nil, fmt.Errorf("selected tumblr blog name is missing")
	}
	selectedBlogName = tumblr.NormalizeBlogName(selectedBlogName)
	if selectedBlogName == "" {
		return nil, fmt.Errorf("selected tumblr blog name is invalid")
	}
	selectedBlogUUID := strings.TrimSpace(meta.SelectedBlogUUID)
	if selectedBlogUUID == "" {
		return nil, fmt.Errorf("selected tumblr blog uuid is missing")
	}
	if !validRemoteID(selectedBlogUUID) {
		return nil, fmt.Errorf("selected tumblr blog uuid is invalid")
	}
	normalized.SessionCookies = tumblr.NormalizeSessionCookies(sessionCookies)
	normalized.APIToken = apiToken
	normalized.CSRFToken = normalizeOptionalHeaderCredential(meta.CSRFToken)
	normalized.APIVersion = normalizeOptionalHeaderCredential(meta.APIVersion)
	normalized.UserName = normalizeOptionalMetadataBlogName(meta.UserName)
	normalized.SelectedBlogName = selectedBlogName
	normalized.SelectedBlogUUID = selectedBlogUUID
	return &normalized, nil
}

func normalizeOptionalMetadataBlogName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return tumblr.NormalizeBlogName(value)
}

func normalizeOptionalHeaderCredential(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || containsMetadataControl(value) {
		return ""
	}
	return value
}

func containsMetadataControl(value string) bool {
	return strings.IndexFunc(value, unicode.IsControl) >= 0
}

func containsMetadataSpaceOrControl(value string) bool {
	return strings.IndexFunc(value, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) >= 0
}

func (tc *TumblrClient) validatedLoginMetadata() (*UserLoginMetadata, error) {
	if tc == nil {
		return nil, fmt.Errorf("tumblr login metadata is missing")
	}
	tc.loginMetadataLock.Lock()
	defer tc.loginMetadataLock.Unlock()
	meta, err := tc.validatedLoginMetadataLocked()
	if err != nil {
		return nil, err
	}
	return meta.clone(), nil
}

// validatedLoginMetadataLocked returns the live metadata pointer. The caller
// must hold loginMetadataLock for the entire time it reads or mutates the value.
func (tc *TumblrClient) validatedLoginMetadataLocked() (*UserLoginMetadata, error) {
	if tc == nil || tc.userLogin == nil || tc.userLogin.UserLogin == nil {
		return nil, fmt.Errorf("tumblr login metadata is missing")
	}
	return validateUserLoginMetadata(tc.userLogin.Metadata)
}
