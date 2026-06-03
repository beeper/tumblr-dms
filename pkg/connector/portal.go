package connector

import (
	"errors"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/ifixrobots/tumblr-dms/pkg/tumblrid"
)

func conversationIDFromPortal(portal *bridgev2.Portal, requiredError string) (string, error) {
	if portal == nil || portal.Portal == nil {
		return "", errors.New(requiredError)
	}
	if meta, ok := portal.Metadata.(*PortalMetadata); ok && meta != nil && meta.ConversationID != "" {
		if !validRemoteID(meta.ConversationID) {
			return "", fmt.Errorf("tumblr conversation id contains invalid characters")
		}
		return meta.ConversationID, nil
	}
	conversationID := tumblrid.ParsePortalID(portal.ID)
	if conversationID == "" {
		return "", fmt.Errorf("tumblr conversation id is missing")
	}
	if isPendingDMPortalID(conversationID) {
		return "", fmt.Errorf("tumblr conversation has not been created yet")
	}
	if !validRemoteID(conversationID) {
		return "", fmt.Errorf("tumblr conversation id contains invalid characters")
	}
	return conversationID, nil
}
