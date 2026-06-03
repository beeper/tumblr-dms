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
	CookieHeader     string    `json:"cookie_header"`
	APIToken         string    `json:"api_token,omitempty"`
	CSRFToken        string    `json:"csrf_token,omitempty"`
	APIVersion       string    `json:"api_version,omitempty"`
	UserName         string    `json:"user_name,omitempty"`
	SelectedBlogName string    `json:"selected_blog_name,omitempty"`
	SelectedBlogUUID string    `json:"selected_blog_uuid,omitempty"`
	PushKeys         *PushKeys `json:"push_keys,omitempty"`
}

type PushKeys struct {
	Token           string   `json:"token,omitempty"`
	FCMAppID        string   `json:"fcm_app_id,omitempty"`
	AndroidID       string   `json:"android_id,omitempty"`
	SecurityToken   string   `json:"security_token,omitempty"`
	LastCheckinTS   int64    `json:"last_checkin_ts,omitempty"`
	FCMRegisteredTS int64    `json:"fcm_registered_ts,omitempty"`
	PersistentIDs   []string `json:"persistent_ids,omitempty"`
	P256DH          []byte   `json:"p256dh,omitempty"`
	Auth            []byte   `json:"auth,omitempty"`
	Private         []byte   `json:"private,omitempty"`
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
	token := ""
	fcmAppID := ""
	androidID := ""
	securityToken := ""
	lastCheckinTS := int64(0)
	fcmRegisteredTS := int64(0)
	persistentIDs := []string(nil)
	if m.PushKeys != nil {
		token = m.PushKeys.Token
		fcmAppID = m.PushKeys.FCMAppID
		androidID = m.PushKeys.AndroidID
		securityToken = m.PushKeys.SecurityToken
		lastCheckinTS = m.PushKeys.LastCheckinTS
		fcmRegisteredTS = m.PushKeys.FCMRegisteredTS
		persistentIDs = append(persistentIDs, m.PushKeys.PersistentIDs...)
	}
	m.PushKeys = &PushKeys{
		Token:           token,
		FCMAppID:        fcmAppID,
		AndroidID:       androidID,
		SecurityToken:   securityToken,
		LastCheckinTS:   lastCheckinTS,
		FCMRegisteredTS: fcmRegisteredTS,
		PersistentIDs:   persistentIDs,
		P256DH:          privateKey.PublicKey().Bytes(),
		Auth:            authSecret,
		Private:         privateKey.Bytes(),
	}
	return true, nil
}

func (m *UserLoginMetadata) pushReceiverCredentials() (*pushreceiver.GCMCredentials, error) {
	if m == nil || m.PushKeys == nil || strings.TrimSpace(m.PushKeys.AndroidID) == "" || strings.TrimSpace(m.PushKeys.SecurityToken) == "" {
		return nil, nil
	}
	androidID, err := strconv.ParseUint(strings.TrimSpace(m.PushKeys.AndroidID), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("tumblr push receiver Android ID is invalid: %w", err)
	}
	securityToken, err := strconv.ParseUint(strings.TrimSpace(m.PushKeys.SecurityToken), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("tumblr push receiver security token is invalid: %w", err)
	}
	return &pushreceiver.GCMCredentials{
		AndroidID:     androidID,
		SecurityToken: securityToken,
	}, nil
}

func (m *UserLoginMetadata) setPushReceiverCredentials(creds *pushreceiver.GCMCredentials) {
	if m == nil || m.PushKeys == nil || creds == nil {
		return
	}
	m.PushKeys.AndroidID = strconv.FormatUint(creds.AndroidID, 10)
	m.PushKeys.SecurityToken = strconv.FormatUint(creds.SecurityToken, 10)
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
		"UserLoginMetadata{cookie_header:%s api_token:%s csrf_token:%s api_version:%s user_name:%s selected_blog_name:%s selected_blog_uuid:%s push_keys:%s}",
		redactedMetadataValue(m.CookieHeader),
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
	e.Str("cookie_header", redactedMetadataValue(m.CookieHeader)).
		Str("api_token", redactedMetadataValue(m.APIToken)).
		Str("csrf_token", redactedMetadataValue(m.CSRFToken)).
		Str("api_version", redactedMetadataValue(m.APIVersion)).
		Str("user_name", redactedMetadataValue(m.UserName)).
		Str("selected_blog_name", redactedMetadataValue(m.SelectedBlogName)).
		Str("selected_blog_uuid", redactedMetadataValue(m.SelectedBlogUUID)).
		Bool("has_push_keys", m.PushKeys != nil).
		Bool("has_push_token", m.PushKeys != nil && strings.TrimSpace(m.PushKeys.Token) != "").
		Bool("has_push_receiver", m.PushKeys != nil && strings.TrimSpace(m.PushKeys.AndroidID) != "" && strings.TrimSpace(m.PushKeys.SecurityToken) != "").
		Bool("has_push_fcm_app_id", m.PushKeys != nil && strings.TrimSpace(m.PushKeys.FCMAppID) != "")
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

func validateUserLoginMetadata(raw any) (*UserLoginMetadata, error) {
	meta, ok := raw.(*UserLoginMetadata)
	if !ok || meta == nil {
		return nil, fmt.Errorf("tumblr login metadata is missing")
	}
	cookieHeader := tumblr.CookieHeaderFromMap(map[string]string{"cookie_header": meta.CookieHeader})
	apiToken := normalizeBearerToken(meta.APIToken)
	if strings.TrimSpace(cookieHeader) == "" && apiToken == "" {
		return nil, fmt.Errorf("tumblr cookie header or API token is missing")
	}
	if strings.TrimSpace(cookieHeader) != "" && !tumblr.CookieHeaderHasPair(cookieHeader) {
		return nil, fmt.Errorf("tumblr cookie header must include at least one name=value cookie")
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
	meta.CookieHeader = cookieHeader
	meta.APIToken = apiToken
	meta.CSRFToken = normalizeOptionalHeaderCredential(meta.CSRFToken)
	meta.APIVersion = normalizeOptionalHeaderCredential(meta.APIVersion)
	meta.UserName = normalizeOptionalMetadataBlogName(meta.UserName)
	meta.SelectedBlogName = selectedBlogName
	meta.SelectedBlogUUID = selectedBlogUUID
	return meta, nil
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
	if tc == nil || tc.userLogin == nil || tc.userLogin.UserLogin == nil {
		return nil, fmt.Errorf("tumblr login metadata is missing")
	}
	return validateUserLoginMetadata(tc.userLogin.Metadata)
}
