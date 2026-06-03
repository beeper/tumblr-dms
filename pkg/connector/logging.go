package connector

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
)

func logIdentifierHash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:8])
}

func logMessageType(messageType string) string {
	switch messageType {
	case "", tumblr.MessageTypeText:
		return tumblr.MessageTypeText
	case tumblr.MessageTypeImage, tumblr.MessageTypeSticker, tumblr.MessageTypePostRef:
		return messageType
	default:
		return "UNKNOWN"
	}
}
