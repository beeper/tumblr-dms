package tumblrid

import "maunium.net/go/mautrix/bridgev2/networkid"

func MakePortalID(conversationID string) networkid.PortalID {
	return networkid.PortalID(conversationID)
}

func ParsePortalID(portalID networkid.PortalID) string {
	return string(portalID)
}

func MakeUserID(userID string) networkid.UserID {
	return networkid.UserID(userID)
}

func ParseUserID(userID networkid.UserID) string {
	return string(userID)
}

func MakeUserLoginID(userID string) networkid.UserLoginID {
	return networkid.UserLoginID(userID)
}

func ParseUserLoginID(userLoginID networkid.UserLoginID) string {
	return string(userLoginID)
}

func MakeMessageID(messageID string) networkid.MessageID {
	return networkid.MessageID(messageID)
}

func MakePortalKey(conversationID string, loginID networkid.UserLoginID, splitPortals bool) networkid.PortalKey {
	key := networkid.PortalKey{ID: MakePortalID(conversationID)}
	if splitPortals {
		key.Receiver = loginID
	}
	return key
}
