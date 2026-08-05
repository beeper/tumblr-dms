package tumblrdb

import (
	"context"
	"time"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

type ConversationSyncQuery struct {
	BridgeID networkid.BridgeID
	*dbutil.QueryHelper[*ConversationSync]
}

type ConversationSync struct {
	BridgeID               networkid.BridgeID
	UserLoginID            networkid.UserLoginID
	ConversationID         string
	CompletedHeadMessageID string
	UpdatedAt              time.Time
}

const (
	getConversationSyncQuery = `
		SELECT bridge_id, user_login_id, conversation_id, completed_head_message_id, updated_ts
		FROM tumblr_conversation_sync
		WHERE bridge_id=$1 AND user_login_id=$2 AND conversation_id=$3
	`
	setConversationSyncQuery = `
		INSERT INTO tumblr_conversation_sync (
			bridge_id, user_login_id, conversation_id, completed_head_message_id, updated_ts
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (bridge_id, user_login_id, conversation_id)
		DO UPDATE SET
			completed_head_message_id=excluded.completed_head_message_id,
			updated_ts=excluded.updated_ts
	`
	deleteConversationSyncQuery = `
		DELETE FROM tumblr_conversation_sync
		WHERE bridge_id=$1 AND user_login_id=$2 AND conversation_id=$3
	`
)

func (cq *ConversationSyncQuery) Get(
	ctx context.Context,
	loginID networkid.UserLoginID,
	conversationID string,
) (*ConversationSync, error) {
	return cq.QueryOne(ctx, getConversationSyncQuery, cq.BridgeID, loginID, conversationID)
}

func (cq *ConversationSyncQuery) SetCompletedHead(
	ctx context.Context,
	loginID networkid.UserLoginID,
	conversationID string,
	messageID string,
	at time.Time,
) error {
	return cq.Exec(ctx, setConversationSyncQuery, cq.BridgeID, loginID, conversationID, messageID, at.UnixMilli())
}

func (cq *ConversationSyncQuery) Delete(
	ctx context.Context,
	loginID networkid.UserLoginID,
	conversationID string,
) error {
	return cq.Exec(ctx, deleteConversationSyncQuery, cq.BridgeID, loginID, conversationID)
}

func (state *ConversationSync) Scan(row dbutil.Scannable) (*ConversationSync, error) {
	var updatedTS int64
	err := row.Scan(
		&state.BridgeID,
		&state.UserLoginID,
		&state.ConversationID,
		&state.CompletedHeadMessageID,
		&updatedTS,
	)
	if err != nil {
		return nil, err
	}
	state.UpdatedAt = timeFromMillis(updatedTS)
	return state, nil
}
