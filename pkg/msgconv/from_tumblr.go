package msgconv

import (
	"strings"
	"unicode"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
)

const (
	maxTextLength                  = tumblr.MaxMessageTextRunes
	displayNameTruncation          = "..."
	maxPostSummaryRunes            = 160
	maxUnsupportedMessageTypeRunes = 80
)

type MessageMetadata struct {
	Type string `json:"type,omitempty"`
}

func (m *MessageMetadata) CopyFrom(other any) {
	otherMeta, ok := other.(*MessageMetadata)
	if !ok || otherMeta == nil || m == nil {
		return
	}
	if m.Type == "" {
		m.Type = otherMeta.Type
	}
}

func CanUseImageMedia(message tumblr.Message) bool {
	switch message.Type {
	case tumblr.MessageTypeImage, tumblr.MessageTypeSticker:
		return message.BestImage() != nil
	default:
		return false
	}
}

func ConvertTumblrMessage(message tumblr.Message) *bridgev2.ConvertedMessage {
	msgType := event.MsgText
	body := ""
	switch message.Type {
	case tumblr.MessageTypeText, "":
		if message.Content != nil {
			body = message.Content.Text
		}
	case tumblr.MessageTypePostRef:
		msgType = event.MsgNotice
		body = noticePostBody(message.Post)
	case tumblr.MessageTypeImage:
		msgType = event.MsgNotice
		body = "Tumblr image message"
	case tumblr.MessageTypeSticker:
		msgType = event.MsgNotice
		body = "Tumblr sticker message"
	default:
		msgType = event.MsgNotice
		body = "Unsupported Tumblr message type: " + noticeMessageType(message.Type)
	}
	body = cleanMessageBody(body)
	if body == "" {
		body = "Unsupported empty Tumblr message"
		msgType = event.MsgNotice
	}
	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{{
			Type: event.EventMessage,
			Content: &event.MessageEventContent{
				MsgType: msgType,
				Body:    body,
			},
			DBMetadata: &MessageMetadata{Type: MessageMetadataType(message.Type)},
		}},
	}
}

func MessageMetadataType(messageType string) string {
	switch messageType {
	case "", tumblr.MessageTypeText:
		return tumblr.MessageTypeText
	case tumblr.MessageTypePostRef, tumblr.MessageTypeImage, tumblr.MessageTypeSticker:
		return messageType
	default:
		return noticeMessageType(messageType)
	}
}

func cleanMessageBody(body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return ""
	}
	runes := []rune(body)
	if len(runes) <= maxTextLength {
		return body
	}
	truncation := []rune(displayNameTruncation)
	if maxTextLength <= len(truncation) {
		return string(runes[:maxTextLength])
	}
	return string(runes[:maxTextLength-len(truncation)]) + displayNameTruncation
}

func noticePostSummary(summary string) string {
	fields := strings.FieldsFunc(summary, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	})
	normalized := strings.Join(fields, " ")
	if normalized == "" {
		return ""
	}
	runes := []rune(normalized)
	if len(runes) <= maxPostSummaryRunes {
		return normalized
	}
	return string(runes[:maxPostSummaryRunes]) + displayNameTruncation
}

func noticePostBody(post *tumblr.PostRef) string {
	if post == nil {
		return "Tumblr post"
	}
	if post.IsUnavailable() {
		return "Tumblr post is no longer available"
	}
	summary := noticePostSummary(post.Summary)
	postURL := strings.TrimSpace(post.BestURL())
	switch {
	case summary != "" && postURL != "":
		return "Tumblr post: " + summary + "\n" + postURL
	case summary != "":
		return "Tumblr post: " + summary
	case postURL != "":
		return "Tumblr post\n" + postURL
	default:
		return "Tumblr post"
	}
}

func noticeMessageType(messageType string) string {
	fields := strings.FieldsFunc(messageType, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	})
	normalized := strings.Join(fields, " ")
	if normalized == "" {
		return "unknown"
	}
	runes := []rune(normalized)
	if len(runes) <= maxUnsupportedMessageTypeRunes {
		return normalized
	}
	return string(runes[:maxUnsupportedMessageTypeRunes]) + "..."
}
