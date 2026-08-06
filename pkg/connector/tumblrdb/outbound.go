package tumblrdb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

type OutboundSendState string

const (
	OutboundSendPrepared     OutboundSendState = "prepared"
	OutboundSendSubmitting   OutboundSendState = "submitting"
	OutboundSendAwaitingEcho OutboundSendState = "awaiting_echo"
	OutboundSendUncertain    OutboundSendState = "uncertain"
	OutboundSendResolved     OutboundSendState = "resolved"
	OutboundSendCompleted    OutboundSendState = "completed"
	OutboundSendNotSubmitted OutboundSendState = "not_submitted"
	OutboundSendUnconfirmed  OutboundSendState = "unconfirmed"
)

// Completed outbound rows are dedupe receipts for the lifetime of their
// user_login. Their due timestamp is the largest representable database integer
// so workers never select them.
const OutboundCompletedReceiptUnixMilli int64 = 1<<63 - 1

type OutboundMessageType string

const (
	OutboundMessageText    OutboundMessageType = "TEXT"
	OutboundMessageImage   OutboundMessageType = "IMAGE"
	OutboundMessagePostRef OutboundMessageType = "POSTREF"
)

type OutboundContentHash [sha256.Size]byte

const md5DigestHexLength = 32

func HashOutboundContent(content []byte) OutboundContentHash {
	return sha256.Sum256(content)
}

func isLowerHexDigest(value string, length int) bool {
	if len(value) != length || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded)*2 == length
}

type OutboundSendQuery struct {
	BridgeID networkid.BridgeID
	*dbutil.QueryHelper[*OutboundSend]
}

type OutboundSend struct {
	BridgeID              networkid.BridgeID
	UserLoginID           networkid.UserLoginID
	TransactionID         networkid.TransactionID
	PortalKey             networkid.PortalKey
	ConversationID        string
	BindingConversationID string
	MatrixRoomID          id.RoomID
	MatrixEventID         id.EventID
	MatrixSenderID        id.UserID
	SenderID              networkid.UserID
	InputTransactionID    networkid.RawTransactionID
	MessageType           OutboundMessageType
	ContentHash           OutboundContentHash
	SourceMediaDigest     string
	BaselineMessageIDs    []networkid.MessageID
	RemoteMessageID       networkid.MessageID
	State                 OutboundSendState
	TerminalAt            time.Time
	StatusNotifiedAt      time.Time
	StatusClaimToken      string
	StatusClaimExpiresAt  time.Time
	AttemptCount          int
	NextAttemptAt         time.Time
	SendStartedAt         time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

var (
	ErrOutboundSendNotPending       = errors.New("outbound send is missing or no longer pending")
	ErrOutboundSendAlreadyCompleted = errors.New("outbound Matrix event is already mapped")
	ErrOutboundSendAlreadyHandled   = errors.New("outbound Matrix event already has a durable handling record")
	ErrOutboundSendIdentityChanged  = errors.New("outbound send immutable identity changed")
	ErrOutboundSendBindingConflict  = errors.New("outbound send is already bound to another conversation")
	ErrOutboundSendRoomMismatch     = errors.New("outbound send target does not own the source Matrix room")
)

const outboundSendColumns = `
	bridge_id, user_login_id, transaction_id, portal_id, portal_receiver, conversation_id,
	binding_conversation_id, matrix_room_id, matrix_event_id, matrix_sender_id, sender_id, input_transaction_id, message_type,
	content_hash, source_media_digest, baseline_ids_json, remote_message_id, state, terminal_ts, status_notified_ts,
	status_claim_token, status_claim_expires_ts, attempt_count, next_attempt_ts, send_started_ts, created_ts, updated_ts
`

const (
	insertOutboundSendQuery = `
		INSERT INTO tumblr_outbound_send (` + outboundSendColumns + `)
		SELECT
			$1, $2, $3, $4, $5, $6, '', $7, $8, $9, $10, $11, $12, $13, $14, $15, '', 'prepared',
			0, 0, '', 0, 0, $16, $17, $18, $18
		WHERE NOT EXISTS (
			SELECT 1 FROM message AS completed
			WHERE completed.bridge_id=$1 AND completed.mxid=$8
		)
		AND NOT EXISTS (
			SELECT 1 FROM tumblr_outbound_receipt AS receipt
			WHERE receipt.bridge_id=$1 AND receipt.matrix_event_id=$8
		)
		ON CONFLICT DO NOTHING
	`
	getOutboundSendQuery = `
		SELECT ` + outboundSendColumns + `
		FROM tumblr_outbound_send
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
	`
	outboundMatrixEventMappedQuery = `
		SELECT EXISTS (
			SELECT 1 FROM message WHERE bridge_id=$1 AND mxid=$2
		)
	`
	outboundMatrixEventClaimedQuery = `
		SELECT EXISTS (
			SELECT 1 FROM tumblr_outbound_send WHERE bridge_id=$1 AND matrix_event_id=$2
		)
	`
	outboundTerminalReceiptExistsQuery = `
		SELECT EXISTS (
			SELECT 1 FROM tumblr_outbound_receipt WHERE bridge_id=$1 AND matrix_event_id=$2
		)
	`
	insertOutboundTerminalReceiptQuery = `
		INSERT INTO tumblr_outbound_receipt (
			bridge_id, matrix_event_id, user_login_id, transaction_id, terminal_ts
		)
		SELECT bridge_id, matrix_event_id, user_login_id, transaction_id, terminal_ts
		FROM tumblr_outbound_send
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3 AND terminal_ts>0
		ON CONFLICT DO NOTHING
	`
	getOutboundTerminalReceiptIdentityQuery = `
		SELECT receipt.user_login_id, receipt.transaction_id
		FROM tumblr_outbound_send AS outbound
		JOIN tumblr_outbound_receipt AS receipt
		  ON receipt.bridge_id=outbound.bridge_id
		 AND receipt.matrix_event_id=outbound.matrix_event_id
		WHERE outbound.bridge_id=$1 AND outbound.user_login_id=$2 AND outbound.transaction_id=$3
	`
	getOutboundSendByRemoteIDQuery = `
		SELECT ` + outboundSendColumns + `
		FROM tumblr_outbound_send
		WHERE bridge_id=$1 AND user_login_id=$2 AND conversation_id=$3
		  AND remote_message_id=$4 AND state='resolved'
	`
	getNextDueOutboundSendQuery = `
		SELECT ` + outboundSendColumns + `
		FROM tumblr_outbound_send
		WHERE bridge_id=$1 AND user_login_id=$2
		  AND state<>'completed'
		  AND next_attempt_ts<=$3
		  AND (status_claim_token='' OR status_claim_expires_ts<=$3)
		ORDER BY next_attempt_ts, send_started_ts, created_ts, transaction_id
		LIMIT 1
	`
	listUnboundOutboundSendsQuery = `
		SELECT ` + outboundSendColumns + `
		FROM tumblr_outbound_send
		WHERE bridge_id=$1 AND user_login_id=$2 AND conversation_id=''
		  AND remote_message_id='' AND terminal_ts=0 AND state IN ('submitting', 'uncertain')
		ORDER BY send_started_ts, created_ts, transaction_id
	`
	listMatchableOutboundSendsQuery = `
		SELECT ` + outboundSendColumns + `
		FROM tumblr_outbound_send
		WHERE bridge_id=$1 AND user_login_id=$2 AND conversation_id=$3
		  AND terminal_ts=0 AND state IN ('submitting', 'awaiting_echo', 'uncertain')
		ORDER BY send_started_ts, created_ts, transaction_id
	`
	prepareOutboundSendConversationQuery = `
		UPDATE tumblr_outbound_send
		SET binding_conversation_id=$4, next_attempt_ts=$5, updated_ts=$6
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND conversation_id='' AND remote_message_id='' AND terminal_ts=0
		  AND state IN ('prepared', 'submitting', 'awaiting_echo', 'uncertain')
		  AND (binding_conversation_id='' OR binding_conversation_id=$4)
	`
	markOutboundSendSubmittingQuery = `
		UPDATE tumblr_outbound_send
		SET state='submitting', baseline_ids_json=$4, send_started_ts=$5,
		    next_attempt_ts=$6, updated_ts=$7
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND state='prepared' AND status_notified_ts=0 AND remote_message_id='' AND terminal_ts=0
	`
	markPreparedOutboundNotSubmittedQuery = `
		UPDATE tumblr_outbound_send
		SET state='not_submitted', terminal_ts=$4, next_attempt_ts=$5, updated_ts=$4
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND state='prepared' AND status_notified_ts=0 AND remote_message_id='' AND terminal_ts=0
	`
	markSubmittingOutboundNotSubmittedQuery = `
		UPDATE tumblr_outbound_send
		SET state='not_submitted', terminal_ts=$4, next_attempt_ts=$5, updated_ts=$4
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND state='submitting' AND status_notified_ts=0 AND status_claim_token=''
		  AND remote_message_id='' AND terminal_ts=0
	`
	claimOutboundStatusDeliveryQuery = `
		UPDATE tumblr_outbound_send
		SET status_claim_token=$4, status_claim_expires_ts=$5,
		    next_attempt_ts=CASE WHEN next_attempt_ts>$5 THEN $5 ELSE next_attempt_ts END,
		    updated_ts=$6
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND status_notified_ts=0 AND terminal_ts>0
		  AND state IN ('not_submitted', 'unconfirmed')
		  AND (status_claim_token='' OR status_claim_expires_ts<=$6)
	`
	finishOutboundStatusDeliveryQuery = `
		UPDATE tumblr_outbound_send
		SET status_notified_ts=$5,
		    next_attempt_ts=$6,
		    status_claim_token='', status_claim_expires_ts=0, updated_ts=$5
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND status_claim_token=$4 AND status_notified_ts=0 AND terminal_ts>0
		  AND state IN ('not_submitted', 'unconfirmed')
	`
	releaseOutboundStatusDeliveryQuery = `
		UPDATE tumblr_outbound_send
		SET status_claim_token='', status_claim_expires_ts=0, updated_ts=$5
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND status_claim_token=$4 AND status_notified_ts=0
	`
	bindOutboundSendConversationQuery = `
		UPDATE tumblr_outbound_send
		SET portal_id=$4, portal_receiver=$5, conversation_id=$6,
		    binding_conversation_id='', next_attempt_ts=$7, updated_ts=$8
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND remote_message_id='' AND terminal_ts=0 AND state IN ('prepared', 'submitting', 'awaiting_echo', 'uncertain')
		  AND (
			(conversation_id='' AND binding_conversation_id=$6)
			OR (conversation_id=$6 AND binding_conversation_id='' AND portal_id=$4 AND portal_receiver=$5)
		  )
		  AND EXISTS (
			SELECT 1 FROM portal AS target
			WHERE target.bridge_id=tumblr_outbound_send.bridge_id
			  AND target.id=$4 AND target.receiver=$5
			  AND target.mxid=tumblr_outbound_send.matrix_room_id
		  )
	`
	markOutboundSendAwaitingEchoQuery = `
		UPDATE tumblr_outbound_send
		SET state='awaiting_echo', next_attempt_ts=$4, updated_ts=$5
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND conversation_id<>'' AND remote_message_id='' AND terminal_ts=0
		  AND state IN ('submitting', 'awaiting_echo', 'uncertain')
	`
	markOutboundSendUncertainQuery = `
		UPDATE tumblr_outbound_send
		SET state='uncertain', attempt_count=attempt_count+1,
		    next_attempt_ts=$4, updated_ts=$5
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND terminal_ts=0 AND state IN ('submitting', 'awaiting_echo', 'uncertain')
	`
	markOutboundSendUnconfirmedQuery = `
		UPDATE tumblr_outbound_send
		SET state='unconfirmed', remote_message_id='', terminal_ts=$4, next_attempt_ts=$5, updated_ts=$6
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND terminal_ts=0 AND remote_message_id=''
		  AND state IN ('submitting', 'awaiting_echo', 'uncertain')
	`
	markResolvedOutboundSendUnconfirmedQuery = `
		UPDATE tumblr_outbound_send
		SET state='unconfirmed', remote_message_id='', terminal_ts=$5, next_attempt_ts=$6, updated_ts=$7
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND state='resolved' AND remote_message_id=$4 AND terminal_ts=0
	`
	markOutboundSendCompletedQuery = `
		UPDATE tumblr_outbound_send
		SET state='completed', terminal_ts=$5, next_attempt_ts=$6, updated_ts=$5
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND state='resolved' AND remote_message_id=$4 AND terminal_ts=0
		  AND status_notified_ts=0 AND status_claim_token=''
	`
	terminalizeOutboundConversationQuery = `
		UPDATE tumblr_outbound_send
		SET state=CASE WHEN state='prepared' THEN 'not_submitted' ELSE 'unconfirmed' END,
		    terminal_ts=$4, next_attempt_ts=$5, updated_ts=$4
		WHERE bridge_id=$1 AND user_login_id=$2
		  AND (conversation_id=$3 OR binding_conversation_id=$3)
		  AND remote_message_id='' AND terminal_ts=0
		  AND state IN ('prepared', 'submitting', 'awaiting_echo', 'uncertain')
		RETURNING transaction_id
	`
	scheduleOutboundSendRetryQuery = `
		UPDATE tumblr_outbound_send
		SET attempt_count=attempt_count+1, next_attempt_ts=$4, updated_ts=$5
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND state IN ('prepared', 'submitting', 'awaiting_echo', 'uncertain', 'resolved', 'completed', 'not_submitted', 'unconfirmed')
	`
	claimOutboundRemoteMessageQuery = `
		UPDATE tumblr_outbound_send
		SET remote_message_id=$4, state='resolved', next_attempt_ts=$6, updated_ts=$7
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND conversation_id=$5
		  AND (
					(state IN ('submitting', 'awaiting_echo', 'uncertain') AND remote_message_id='' AND terminal_ts=0
					AND status_claim_token=''
				AND NOT EXISTS (
					SELECT 1
					FROM tumblr_outbound_send AS claimed
					WHERE claimed.bridge_id=$1 AND claimed.user_login_id=$2
					  AND claimed.conversation_id=tumblr_outbound_send.conversation_id
					  AND claimed.remote_message_id=$4
				)
				AND NOT EXISTS (
					SELECT 1
					FROM message AS mapped
					WHERE mapped.bridge_id=$1 AND mapped.id=$4
					  AND (mapped.room_receiver=tumblr_outbound_send.portal_receiver OR mapped.room_receiver='')
				))
			OR (state='resolved' AND remote_message_id=$4)
		  )
	`
	deletePreparedOutboundSendQuery = `
		DELETE FROM tumblr_outbound_send
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND state='prepared' AND remote_message_id='' AND terminal_ts=0
	`
	deleteNotSubmittedOutboundSendQuery = `
		DELETE FROM tumblr_outbound_send
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND state='not_submitted' AND remote_message_id='' AND terminal_ts>0
		  AND status_notified_ts>0 AND status_claim_token=''
		  AND EXISTS (
			SELECT 1 FROM tumblr_outbound_receipt AS receipt
			WHERE receipt.bridge_id=tumblr_outbound_send.bridge_id
			  AND receipt.matrix_event_id=tumblr_outbound_send.matrix_event_id
			  AND receipt.user_login_id=tumblr_outbound_send.user_login_id
			  AND receipt.transaction_id=tumblr_outbound_send.transaction_id
		  )
	`
	deleteUnconfirmedOutboundSendQuery = `
		DELETE FROM tumblr_outbound_send
		WHERE bridge_id=$1 AND user_login_id=$2 AND transaction_id=$3
		  AND state='unconfirmed' AND remote_message_id='' AND terminal_ts>0
		  AND status_notified_ts>0 AND status_claim_token=''
		  AND EXISTS (
			SELECT 1 FROM tumblr_outbound_receipt AS receipt
			WHERE receipt.bridge_id=tumblr_outbound_send.bridge_id
			  AND receipt.matrix_event_id=tumblr_outbound_send.matrix_event_id
			  AND receipt.user_login_id=tumblr_outbound_send.user_login_id
			  AND receipt.transaction_id=tumblr_outbound_send.transaction_id
		  )
	`
)

func (oq *OutboundSendQuery) Get(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
) (*OutboundSend, error) {
	if strings.TrimSpace(string(loginID)) == "" {
		return nil, fmt.Errorf("user login ID is required to get an outbound send")
	}
	if strings.TrimSpace(string(txnID)) == "" {
		return nil, fmt.Errorf("transaction ID is required to get an outbound send")
	}
	return oq.QueryOne(ctx, getOutboundSendQuery, oq.BridgeID, loginID, txnID)
}

func (oq *OutboundSendQuery) GetByRemoteID(
	ctx context.Context,
	loginID networkid.UserLoginID,
	conversationID string,
	remoteID networkid.MessageID,
) (*OutboundSend, error) {
	if strings.TrimSpace(string(loginID)) == "" {
		return nil, fmt.Errorf("user login ID is required to get an outbound send by remote ID")
	}
	if strings.TrimSpace(conversationID) == "" {
		return nil, fmt.Errorf("conversation ID is required to get an outbound send by remote ID")
	}
	if strings.TrimSpace(string(remoteID)) == "" {
		return nil, fmt.Errorf("remote message ID is required to get an outbound send")
	}
	return oq.QueryOne(
		ctx,
		getOutboundSendByRemoteIDQuery,
		oq.BridgeID,
		loginID,
		conversationID,
		remoteID,
	)
}

func (oq *OutboundSendQuery) LoadOrCreate(
	ctx context.Context,
	send *OutboundSend,
) (persisted *OutboundSend, created bool, err error) {
	if send == nil {
		return nil, false, fmt.Errorf("outbound send is required")
	}
	if send.BridgeID != "" && send.BridgeID != oq.BridgeID {
		return nil, false, fmt.Errorf("outbound send bridge ID does not match query bridge ID")
	}
	send.BridgeID = oq.BridgeID
	if err = validateOutboundSendIdentity(send); err != nil {
		return nil, false, err
	}
	existing, err := oq.Get(ctx, send.UserLoginID, send.TransactionID)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		if err = validateOutboundSendReentry(existing, send); err != nil {
			return nil, false, err
		}
		return existing, false, nil
	}

	if send.State != "" && send.State != OutboundSendPrepared {
		return nil, false, fmt.Errorf("new outbound send state must be %q", OutboundSendPrepared)
	}
	if send.BindingConversationID != "" || send.RemoteMessageID != "" || send.AttemptCount != 0 ||
		!send.TerminalAt.IsZero() || !send.StatusNotifiedAt.IsZero() || send.StatusClaimToken != "" ||
		!send.StatusClaimExpiresAt.IsZero() {
		return nil, false, fmt.Errorf("new outbound send cannot already be resolved or attempted")
	}
	if send.SendStartedAt.IsZero() {
		return nil, false, fmt.Errorf("outbound send start time is required")
	}
	now := time.Now()
	if send.SendStartedAt.UnixMilli() > now.UnixMilli() {
		return nil, false, fmt.Errorf("outbound send start time cannot be in the future")
	}
	if err = validateNextAttempt(send.NextAttemptAt, now); err != nil {
		return nil, false, fmt.Errorf("invalid new outbound send schedule: %w", err)
	}
	baselineJSON, err := marshalBaselineMessageIDs(send.BaselineMessageIDs)
	if err != nil {
		return nil, false, err
	}

	for range 2 {
		result, insertErr := oq.GetDB().Exec(
			ctx,
			insertOutboundSendQuery,
			oq.BridgeID,
			send.UserLoginID,
			send.TransactionID,
			send.PortalKey.ID,
			send.PortalKey.Receiver,
			send.ConversationID,
			send.MatrixRoomID,
			send.MatrixEventID,
			send.MatrixSenderID,
			send.SenderID,
			send.InputTransactionID,
			send.MessageType,
			hex.EncodeToString(send.ContentHash[:]),
			send.SourceMediaDigest,
			baselineJSON,
			send.NextAttemptAt.UnixMilli(),
			send.SendStartedAt.UnixMilli(),
			now.UnixMilli(),
		)
		created, err = rowWasChanged(result, insertErr)
		if err != nil {
			return nil, false, err
		}
		if created {
			send.State = OutboundSendPrepared
			send.CreatedAt = now
			send.UpdatedAt = now
			return send, true, nil
		}
		existing, err = oq.Get(ctx, send.UserLoginID, send.TransactionID)
		if err != nil {
			return nil, false, err
		}
		if existing != nil {
			if err = validateOutboundSendReentry(existing, send); err != nil {
				return nil, false, err
			}
			return existing, false, nil
		}
		mapped, mappedErr := oq.isMatrixEventMapped(ctx, send.MatrixEventID)
		if mappedErr != nil {
			return nil, false, mappedErr
		}
		if mapped {
			return nil, false, ErrOutboundSendAlreadyCompleted
		}
		claimed, claimedErr := oq.isMatrixEventClaimed(ctx, send.MatrixEventID)
		if claimedErr != nil {
			return nil, false, claimedErr
		}
		receipted, receiptErr := oq.hasTerminalReceipt(ctx, send.MatrixEventID)
		if receiptErr != nil {
			return nil, false, receiptErr
		}
		if claimed || receipted {
			return nil, false, ErrOutboundSendAlreadyHandled
		}
	}
	return nil, false, fmt.Errorf("outbound send changed concurrently while loading or creating")
}

func (oq *OutboundSendQuery) isMatrixEventMapped(ctx context.Context, eventID id.EventID) (bool, error) {
	var mapped bool
	if err := oq.GetDB().QueryRow(ctx, outboundMatrixEventMappedQuery, oq.BridgeID, eventID).Scan(&mapped); err != nil {
		return false, fmt.Errorf("check completed outbound Matrix event: %w", err)
	}
	return mapped, nil
}

func (oq *OutboundSendQuery) isMatrixEventClaimed(ctx context.Context, eventID id.EventID) (bool, error) {
	var claimed bool
	if err := oq.GetDB().QueryRow(ctx, outboundMatrixEventClaimedQuery, oq.BridgeID, eventID).Scan(&claimed); err != nil {
		return false, fmt.Errorf("check claimed outbound Matrix event: %w", err)
	}
	return claimed, nil
}

func (oq *OutboundSendQuery) hasTerminalReceipt(ctx context.Context, eventID id.EventID) (bool, error) {
	var receipted bool
	if err := oq.GetDB().QueryRow(ctx, outboundTerminalReceiptExistsQuery, oq.BridgeID, eventID).Scan(&receipted); err != nil {
		return false, fmt.Errorf("check terminal outbound Matrix event receipt: %w", err)
	}
	return receipted, nil
}

func (oq *OutboundSendQuery) GetNextDue(
	ctx context.Context,
	loginID networkid.UserLoginID,
	now time.Time,
) (*OutboundSend, error) {
	if strings.TrimSpace(string(loginID)) == "" {
		return nil, fmt.Errorf("user login ID is required to get the next due outbound send")
	}
	if now.IsZero() {
		return nil, fmt.Errorf("current time is required to get the next due outbound send")
	}
	return oq.QueryOne(ctx, getNextDueOutboundSendQuery, oq.BridgeID, loginID, now.UnixMilli())
}

func (oq *OutboundSendQuery) ListUnbound(
	ctx context.Context,
	loginID networkid.UserLoginID,
) ([]*OutboundSend, error) {
	if strings.TrimSpace(string(loginID)) == "" {
		return nil, fmt.Errorf("user login ID is required to list unbound outbound sends")
	}
	return oq.QueryMany(ctx, listUnboundOutboundSendsQuery, oq.BridgeID, loginID)
}

func (oq *OutboundSendQuery) ListMatchable(
	ctx context.Context,
	loginID networkid.UserLoginID,
	conversationID string,
) ([]*OutboundSend, error) {
	if strings.TrimSpace(conversationID) == "" {
		return nil, fmt.Errorf("conversation ID is required to list matchable outbound sends")
	}
	return oq.QueryMany(ctx, listMatchableOutboundSendsQuery, oq.BridgeID, loginID, conversationID)
}

func (oq *OutboundSendQuery) PrepareConversationBinding(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	conversationID string,
	nextAttemptAt time.Time,
) error {
	if strings.TrimSpace(conversationID) == "" {
		return fmt.Errorf("conversation ID is required to prepare an outbound binding")
	}
	now := time.Now()
	if err := validateNextAttempt(nextAttemptAt, now); err != nil {
		return fmt.Errorf("invalid outbound binding preparation schedule: %w", err)
	}
	result, err := oq.GetDB().Exec(
		ctx,
		prepareOutboundSendConversationQuery,
		oq.BridgeID,
		loginID,
		txnID,
		conversationID,
		nextAttemptAt.UnixMilli(),
		now.UnixMilli(),
	)
	changed, err := rowWasChanged(result, err)
	if err != nil || changed {
		return err
	}
	existing, err := oq.Get(ctx, loginID, txnID)
	if err != nil {
		return err
	}
	if existing == nil || existing.IsTerminal() || existing.State == OutboundSendResolved {
		return ErrOutboundSendNotPending
	}
	if existing.ConversationID == conversationID && existing.PortalKey.ID == networkid.PortalID(conversationID) {
		return nil
	}
	if existing.ConversationID != "" || (existing.BindingConversationID != "" && existing.BindingConversationID != conversationID) {
		return ErrOutboundSendBindingConflict
	}
	if existing.BindingConversationID == conversationID {
		return nil
	}
	return ErrOutboundSendNotPending
}

func (oq *OutboundSendQuery) MarkSubmitting(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	baselineMessageIDs []networkid.MessageID,
	sendStartedAt time.Time,
	nextAttemptAt time.Time,
) (bool, error) {
	now := time.Now()
	if sendStartedAt.IsZero() || sendStartedAt.After(now) {
		return false, fmt.Errorf("valid outbound submit start time is required")
	}
	if err := validateNextAttempt(nextAttemptAt, now); err != nil {
		return false, fmt.Errorf("invalid outbound submit schedule: %w", err)
	}
	baselineJSON, err := marshalBaselineMessageIDs(baselineMessageIDs)
	if err != nil {
		return false, err
	}
	result, err := oq.GetDB().Exec(
		ctx,
		markOutboundSendSubmittingQuery,
		oq.BridgeID,
		loginID,
		txnID,
		baselineJSON,
		sendStartedAt.UnixMilli(),
		nextAttemptAt.UnixMilli(),
		now.UnixMilli(),
	)
	changed, err := rowWasChanged(result, err)
	return changed, err
}

func (oq *OutboundSendQuery) ClaimPreparedNotSubmitted(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	terminalAt time.Time,
	nextAttemptAt time.Time,
) (bool, error) {
	return oq.markNotSubmitted(
		ctx,
		markPreparedOutboundNotSubmittedQuery,
		loginID,
		txnID,
		terminalAt,
		nextAttemptAt,
	)
}

func (oq *OutboundSendQuery) MarkSubmittingNotSubmitted(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	terminalAt time.Time,
	nextAttemptAt time.Time,
) (bool, error) {
	return oq.markNotSubmitted(
		ctx,
		markSubmittingOutboundNotSubmittedQuery,
		loginID,
		txnID,
		terminalAt,
		nextAttemptAt,
	)
}

func (oq *OutboundSendQuery) markNotSubmitted(
	ctx context.Context,
	query string,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	terminalAt time.Time,
	nextAttemptAt time.Time,
) (bool, error) {
	if terminalAt.IsZero() {
		return false, fmt.Errorf("outbound not-submitted terminal time is required")
	}
	if !nextAttemptAt.After(terminalAt) {
		return false, fmt.Errorf("outbound not-submitted retry must follow its terminal time")
	}
	return oq.withTerminalReceipt(ctx, loginID, txnID, func(txCtx context.Context) (bool, error) {
		result, err := oq.GetDB().Exec(
			txCtx,
			query,
			oq.BridgeID,
			loginID,
			txnID,
			terminalAt.UnixMilli(),
			nextAttemptAt.UnixMilli(),
		)
		return rowWasChanged(result, err)
	})
}

func (oq *OutboundSendQuery) withTerminalReceipt(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	transition func(context.Context) (bool, error),
) (changed bool, err error) {
	if transition == nil {
		return false, fmt.Errorf("outbound terminal transition is required")
	}
	err = oq.GetDB().DoTxn(ctx, nil, func(txCtx context.Context) error {
		changed, err = transition(txCtx)
		if err != nil || !changed {
			return err
		}
		return oq.ensureTerminalReceipt(txCtx, loginID, txnID)
	})
	return changed, err
}

func (oq *OutboundSendQuery) ensureTerminalReceipt(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
) error {
	if _, err := oq.GetDB().Exec(
		ctx,
		insertOutboundTerminalReceiptQuery,
		oq.BridgeID,
		loginID,
		txnID,
	); err != nil {
		return fmt.Errorf("save permanent outbound event receipt: %w", err)
	}
	var receiptLoginID networkid.UserLoginID
	var receiptTransactionID networkid.TransactionID
	err := oq.GetDB().QueryRow(
		ctx,
		getOutboundTerminalReceiptIdentityQuery,
		oq.BridgeID,
		loginID,
		txnID,
	).Scan(&receiptLoginID, &receiptTransactionID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("permanent outbound event receipt was not saved")
	}
	if err != nil {
		return fmt.Errorf("verify permanent outbound event receipt: %w", err)
	}
	if receiptLoginID != loginID || receiptTransactionID != txnID {
		return fmt.Errorf("%w: terminal event receipt belongs to another outbound send", ErrOutboundSendIdentityChanged)
	}
	return nil
}

func (oq *OutboundSendQuery) ClaimStatusDelivery(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	claimToken string,
	expiresAt time.Time,
) (bool, error) {
	if strings.TrimSpace(claimToken) == "" {
		return false, fmt.Errorf("outbound status claim token is required")
	}
	now := time.Now()
	if !expiresAt.After(now) {
		return false, fmt.Errorf("outbound status claim expiry must be in the future")
	}
	result, err := oq.GetDB().Exec(
		ctx,
		claimOutboundStatusDeliveryQuery,
		oq.BridgeID,
		loginID,
		txnID,
		claimToken,
		expiresAt.UnixMilli(),
		now.UnixMilli(),
	)
	return rowWasChanged(result, err)
}

func (oq *OutboundSendQuery) FinishStatusDelivery(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	claimToken string,
	notifiedAt time.Time,
	terminalExpiresAt time.Time,
) error {
	if strings.TrimSpace(claimToken) == "" {
		return fmt.Errorf("outbound status claim token is required")
	}
	if notifiedAt.IsZero() {
		return fmt.Errorf("outbound status notification time is required")
	}
	if !terminalExpiresAt.After(notifiedAt) {
		return fmt.Errorf("outbound terminal expiry must follow its notification time")
	}
	result, err := oq.GetDB().Exec(
		ctx,
		finishOutboundStatusDeliveryQuery,
		oq.BridgeID,
		loginID,
		txnID,
		claimToken,
		notifiedAt.UnixMilli(),
		terminalExpiresAt.UnixMilli(),
	)
	changed, err := rowWasChanged(result, err)
	if err != nil || changed {
		return err
	}
	existing, err := oq.Get(ctx, loginID, txnID)
	if err != nil {
		return err
	}
	if existing != nil && !existing.StatusNotifiedAt.IsZero() {
		return nil
	}
	return ErrOutboundSendNotPending
}

func (oq *OutboundSendQuery) ReleaseStatusDelivery(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	claimToken string,
) error {
	if strings.TrimSpace(claimToken) == "" {
		return fmt.Errorf("outbound status claim token is required")
	}
	_, err := oq.GetDB().Exec(
		ctx,
		releaseOutboundStatusDeliveryQuery,
		oq.BridgeID,
		loginID,
		txnID,
		claimToken,
		time.Now().UnixMilli(),
	)
	return err
}

func (oq *OutboundSendQuery) BindConversation(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	conversationID string,
	portalKey networkid.PortalKey,
	nextAttemptAt time.Time,
) error {
	if strings.TrimSpace(conversationID) == "" {
		return fmt.Errorf("conversation ID is required to bind an outbound send")
	}
	if portalKey.IsEmpty() {
		return fmt.Errorf("portal key is required to bind an outbound send")
	}
	if string(portalKey.ID) != conversationID {
		return fmt.Errorf("outbound portal ID must match the conversation ID")
	}
	now := time.Now()
	if err := validateNextAttempt(nextAttemptAt, now); err != nil {
		return fmt.Errorf("invalid outbound binding schedule: %w", err)
	}
	result, err := oq.GetDB().Exec(
		ctx,
		bindOutboundSendConversationQuery,
		oq.BridgeID,
		loginID,
		txnID,
		portalKey.ID,
		portalKey.Receiver,
		conversationID,
		nextAttemptAt.UnixMilli(),
		now.UnixMilli(),
	)
	changed, err := rowWasChanged(result, err)
	if err != nil || changed {
		return err
	}
	existing, err := oq.Get(ctx, loginID, txnID)
	if err != nil {
		return err
	}
	if existing != nil && existing.ConversationID != "" &&
		(existing.ConversationID != conversationID || existing.PortalKey != portalKey) {
		return ErrOutboundSendBindingConflict
	}
	if existing != nil && existing.ConversationID == conversationID && existing.PortalKey == portalKey && existing.BindingConversationID == "" {
		return nil
	}
	if existing != nil && existing.ConversationID == "" && existing.BindingConversationID == conversationID {
		return ErrOutboundSendRoomMismatch
	}
	return ErrOutboundSendNotPending
}

func (oq *OutboundSendQuery) MarkAwaitingEcho(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	nextAttemptAt time.Time,
) error {
	now := time.Now()
	if err := validateNextAttempt(nextAttemptAt, now); err != nil {
		return fmt.Errorf("invalid awaiting-echo schedule: %w", err)
	}
	result, err := oq.GetDB().Exec(
		ctx,
		markOutboundSendAwaitingEchoQuery,
		oq.BridgeID,
		loginID,
		txnID,
		nextAttemptAt.UnixMilli(),
		now.UnixMilli(),
	)
	return requirePendingOutboundSend(result, err)
}

func (oq *OutboundSendQuery) MarkUncertain(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	nextAttemptAt time.Time,
) error {
	now := time.Now()
	if err := validateNextAttempt(nextAttemptAt, now); err != nil {
		return fmt.Errorf("invalid uncertain-send schedule: %w", err)
	}
	result, err := oq.GetDB().Exec(
		ctx,
		markOutboundSendUncertainQuery,
		oq.BridgeID,
		loginID,
		txnID,
		nextAttemptAt.UnixMilli(),
		now.UnixMilli(),
	)
	return requirePendingOutboundSend(result, err)
}

func (oq *OutboundSendQuery) MarkUnconfirmed(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	terminalAt time.Time,
	nextAttemptAt time.Time,
) error {
	if terminalAt.IsZero() {
		return fmt.Errorf("unconfirmed-send terminal time is required")
	}
	if nextAttemptAt.UnixMilli() <= terminalAt.UnixMilli() {
		return fmt.Errorf("unconfirmed-send notification must be scheduled after the terminal time")
	}
	changed, err := oq.withTerminalReceipt(ctx, loginID, txnID, func(txCtx context.Context) (bool, error) {
		result, execErr := oq.GetDB().Exec(
			txCtx,
			markOutboundSendUnconfirmedQuery,
			oq.BridgeID,
			loginID,
			txnID,
			terminalAt.UnixMilli(),
			nextAttemptAt.UnixMilli(),
			terminalAt.UnixMilli(),
		)
		return rowWasChanged(result, execErr)
	})
	if err != nil {
		return err
	}
	if !changed {
		return ErrOutboundSendNotPending
	}
	return nil
}

func (oq *OutboundSendQuery) MarkResolvedUnconfirmed(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	remoteID networkid.MessageID,
	terminalAt time.Time,
	nextAttemptAt time.Time,
) error {
	if strings.TrimSpace(string(remoteID)) == "" {
		return fmt.Errorf("remote message ID is required to terminalize a resolved outbound send")
	}
	if terminalAt.IsZero() {
		return fmt.Errorf("resolved unconfirmed-send terminal time is required")
	}
	if nextAttemptAt.UnixMilli() <= terminalAt.UnixMilli() {
		return fmt.Errorf("resolved unconfirmed-send notification must be scheduled after the terminal time")
	}
	changed, err := oq.withTerminalReceipt(ctx, loginID, txnID, func(txCtx context.Context) (bool, error) {
		result, execErr := oq.GetDB().Exec(
			txCtx,
			markResolvedOutboundSendUnconfirmedQuery,
			oq.BridgeID,
			loginID,
			txnID,
			remoteID,
			terminalAt.UnixMilli(),
			nextAttemptAt.UnixMilli(),
			terminalAt.UnixMilli(),
		)
		return rowWasChanged(result, execErr)
	})
	if err != nil {
		return err
	}
	if !changed {
		return ErrOutboundSendNotPending
	}
	return nil
}

func (oq *OutboundSendQuery) MarkCompleted(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	remoteID networkid.MessageID,
	terminalAt time.Time,
) error {
	if strings.TrimSpace(string(remoteID)) == "" {
		return fmt.Errorf("remote message ID is required to complete an outbound send")
	}
	if terminalAt.IsZero() {
		return fmt.Errorf("outbound completion time is required")
	}
	return oq.GetDB().DoTxn(ctx, nil, func(txCtx context.Context) error {
		result, err := oq.GetDB().Exec(
			txCtx,
			markOutboundSendCompletedQuery,
			oq.BridgeID,
			loginID,
			txnID,
			remoteID,
			terminalAt.UnixMilli(),
			OutboundCompletedReceiptUnixMilli,
		)
		changed, err := rowWasChanged(result, err)
		if err != nil {
			return err
		}
		if !changed {
			existing, getErr := oq.Get(txCtx, loginID, txnID)
			if getErr != nil {
				return getErr
			}
			if existing == nil || existing.State != OutboundSendCompleted || existing.RemoteMessageID != remoteID {
				return ErrOutboundSendNotPending
			}
		}
		return oq.ensureTerminalReceipt(txCtx, loginID, txnID)
	})
}

func (oq *OutboundSendQuery) TerminalizeConversation(
	ctx context.Context,
	loginID networkid.UserLoginID,
	conversationID string,
	terminalAt time.Time,
	nextAttemptAt time.Time,
) ([]networkid.TransactionID, error) {
	if strings.TrimSpace(string(loginID)) == "" {
		return nil, fmt.Errorf("user login ID is required to terminalize outbound sends")
	}
	if strings.TrimSpace(conversationID) == "" {
		return nil, fmt.Errorf("conversation ID is required to terminalize outbound sends")
	}
	if terminalAt.IsZero() {
		return nil, fmt.Errorf("outbound terminal time is required")
	}
	if !nextAttemptAt.After(terminalAt) {
		return nil, fmt.Errorf("outbound terminal notification must be scheduled after the terminal time")
	}
	var transactionIDs []networkid.TransactionID
	err := oq.GetDB().DoTxn(ctx, nil, func(txCtx context.Context) error {
		rows, queryErr := oq.GetDB().Query(
			txCtx,
			terminalizeOutboundConversationQuery,
			oq.BridgeID,
			loginID,
			conversationID,
			terminalAt.UnixMilli(),
			nextAttemptAt.UnixMilli(),
		)
		transactionIDs, queryErr = dbutil.NewRowIterWithError(
			rows,
			dbutil.ScanSingleColumn[networkid.TransactionID],
			queryErr,
		).AsList()
		if queryErr != nil {
			return queryErr
		}
		for _, transactionID := range transactionIDs {
			if receiptErr := oq.ensureTerminalReceipt(txCtx, loginID, transactionID); receiptErr != nil {
				return receiptErr
			}
		}
		return nil
	})
	return transactionIDs, err
}

func (oq *OutboundSendQuery) ScheduleRetry(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	nextAttemptAt time.Time,
) error {
	now := time.Now()
	if err := validateNextAttempt(nextAttemptAt, now); err != nil {
		return fmt.Errorf("invalid outbound retry schedule: %w", err)
	}
	result, err := oq.GetDB().Exec(
		ctx,
		scheduleOutboundSendRetryQuery,
		oq.BridgeID,
		loginID,
		txnID,
		nextAttemptAt.UnixMilli(),
		now.UnixMilli(),
	)
	return requirePendingOutboundSend(result, err)
}

func (oq *OutboundSendQuery) ClaimRemoteMessage(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
	conversationID string,
	remoteID networkid.MessageID,
	nextAttemptAt time.Time,
) (bool, error) {
	if strings.TrimSpace(conversationID) == "" {
		return false, fmt.Errorf("conversation ID is required to claim an outbound remote message")
	}
	if strings.TrimSpace(string(remoteID)) == "" {
		return false, fmt.Errorf("remote message ID is required to claim an outbound send")
	}
	now := time.Now()
	if err := validateNextAttempt(nextAttemptAt, now); err != nil {
		return false, fmt.Errorf("invalid resolved-send schedule: %w", err)
	}
	result, err := oq.GetDB().Exec(
		ctx,
		claimOutboundRemoteMessageQuery,
		oq.BridgeID,
		loginID,
		txnID,
		remoteID,
		conversationID,
		nextAttemptAt.UnixMilli(),
		now.UnixMilli(),
	)
	return rowWasChanged(result, err)
}

func (oq *OutboundSendQuery) DeletePrepared(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
) (bool, error) {
	result, err := oq.GetDB().Exec(ctx, deletePreparedOutboundSendQuery, oq.BridgeID, loginID, txnID)
	return rowWasChanged(result, err)
}

func (oq *OutboundSendQuery) DeleteNotSubmitted(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
) (bool, error) {
	result, err := oq.GetDB().Exec(ctx, deleteNotSubmittedOutboundSendQuery, oq.BridgeID, loginID, txnID)
	return rowWasChanged(result, err)
}

func (oq *OutboundSendQuery) DeleteUnconfirmed(
	ctx context.Context,
	loginID networkid.UserLoginID,
	txnID networkid.TransactionID,
) (bool, error) {
	result, err := oq.GetDB().Exec(ctx, deleteUnconfirmedOutboundSendQuery, oq.BridgeID, loginID, txnID)
	return rowWasChanged(result, err)
}

func (send *OutboundSend) Scan(row dbutil.Scannable) (*OutboundSend, error) {
	var contentHash, baselineJSON string
	var terminalTS, statusNotifiedTS, statusClaimExpiresTS, nextAttemptTS, sendStartedTS, createdTS, updatedTS int64
	err := row.Scan(
		&send.BridgeID,
		&send.UserLoginID,
		&send.TransactionID,
		&send.PortalKey.ID,
		&send.PortalKey.Receiver,
		&send.ConversationID,
		&send.BindingConversationID,
		&send.MatrixRoomID,
		&send.MatrixEventID,
		&send.MatrixSenderID,
		&send.SenderID,
		&send.InputTransactionID,
		&send.MessageType,
		&contentHash,
		&send.SourceMediaDigest,
		&baselineJSON,
		&send.RemoteMessageID,
		&send.State,
		&terminalTS,
		&statusNotifiedTS,
		&send.StatusClaimToken,
		&statusClaimExpiresTS,
		&send.AttemptCount,
		&nextAttemptTS,
		&sendStartedTS,
		&createdTS,
		&updatedTS,
	)
	if err != nil {
		return nil, err
	}
	if err = validateOutboundSendState(send.State); err != nil {
		return nil, err
	}
	if err = validateOutboundMessageType(send.MessageType); err != nil {
		return nil, err
	}
	decodedHash, err := hex.DecodeString(contentHash)
	if err != nil || len(decodedHash) != sha256.Size {
		return nil, fmt.Errorf("invalid outbound content hash in database")
	}
	copy(send.ContentHash[:], decodedHash)
	if send.SourceMediaDigest != "" && !isLowerHexDigest(send.SourceMediaDigest, md5DigestHexLength) {
		return nil, fmt.Errorf("invalid outbound source media digest in database")
	}
	if err = json.Unmarshal([]byte(baselineJSON), &send.BaselineMessageIDs); err != nil {
		return nil, fmt.Errorf("decode outbound baseline message IDs: %w", err)
	}
	if send.BaselineMessageIDs == nil {
		send.BaselineMessageIDs = []networkid.MessageID{}
	}
	for _, messageID := range send.BaselineMessageIDs {
		if strings.TrimSpace(string(messageID)) == "" {
			return nil, fmt.Errorf("outbound baseline contains an empty message ID")
		}
	}
	if err = validateOutboundSendIdentity(send); err != nil {
		return nil, fmt.Errorf("invalid outbound send in database: %w", err)
	}
	if send.AttemptCount < 0 {
		return nil, fmt.Errorf("invalid negative outbound attempt count in database")
	}
	send.NextAttemptAt = timeFromMillis(nextAttemptTS)
	send.TerminalAt = timeFromMillis(terminalTS)
	send.StatusNotifiedAt = timeFromMillis(statusNotifiedTS)
	send.StatusClaimExpiresAt = timeFromMillis(statusClaimExpiresTS)
	send.SendStartedAt = timeFromMillis(sendStartedTS)
	send.CreatedAt = timeFromMillis(createdTS)
	send.UpdatedAt = timeFromMillis(updatedTS)
	if send.NextAttemptAt.IsZero() || send.SendStartedAt.IsZero() || send.CreatedAt.IsZero() || send.UpdatedAt.IsZero() {
		return nil, fmt.Errorf("invalid outbound send timestamps in database")
	}
	if (send.StatusClaimToken == "") != send.StatusClaimExpiresAt.IsZero() {
		return nil, fmt.Errorf("invalid outbound status claim in database")
	}
	statusEligible := !send.TerminalAt.IsZero() &&
		(send.State == OutboundSendNotSubmitted || send.State == OutboundSendUnconfirmed)
	if (!send.StatusNotifiedAt.IsZero() || send.StatusClaimToken != "") && !statusEligible {
		return nil, fmt.Errorf("invalid outbound status state in database")
	}
	if !send.StatusNotifiedAt.IsZero() && send.StatusClaimToken != "" {
		return nil, fmt.Errorf("invalid finished outbound status claim in database")
	}
	if !send.TerminalAt.IsZero() {
		validFailure := (send.State == OutboundSendNotSubmitted || send.State == OutboundSendUnconfirmed) &&
			send.RemoteMessageID == ""
		validCompletion := send.State == OutboundSendCompleted && send.RemoteMessageID != "" &&
			send.ConversationID != "" && send.BindingConversationID == ""
		if (!validFailure && !validCompletion) || !send.NextAttemptAt.After(send.TerminalAt) {
			return nil, fmt.Errorf("invalid terminal outbound send in database")
		}
		return send, nil
	}
	switch send.State {
	case OutboundSendCompleted, OutboundSendNotSubmitted, OutboundSendUnconfirmed:
		return nil, fmt.Errorf("invalid nonterminal terminal-state outbound send in database")
	case OutboundSendResolved:
		if send.RemoteMessageID == "" || send.ConversationID == "" || send.BindingConversationID != "" {
			return nil, fmt.Errorf("invalid unresolved identity for resolved outbound send in database")
		}
	case OutboundSendAwaitingEcho:
		if send.RemoteMessageID != "" || send.ConversationID == "" || send.BindingConversationID != "" {
			return nil, fmt.Errorf("invalid awaiting-echo outbound send in database")
		}
	default:
		if send.RemoteMessageID != "" {
			return nil, fmt.Errorf("invalid remote message ID on pending outbound send in database")
		}
	}
	if send.ConversationID != "" && send.BindingConversationID != "" {
		return nil, fmt.Errorf("invalid simultaneous final and staged outbound conversation bindings")
	}
	return send, nil
}

func (send *OutboundSend) IsTerminal() bool {
	return send != nil && !send.TerminalAt.IsZero()
}

func validateOutboundSendIdentity(send *OutboundSend) error {
	// An empty bridge ID is the canonical identity for a standalone mxmain bridge.
	// Query scoping and reentry validation still require the stored IDs to match.
	switch {
	case strings.TrimSpace(string(send.UserLoginID)) == "":
		return fmt.Errorf("outbound send user login ID is required")
	case strings.TrimSpace(string(send.TransactionID)) == "":
		return fmt.Errorf("outbound send transaction ID is required")
	case send.PortalKey.IsEmpty():
		return fmt.Errorf("outbound send portal key is required")
	case send.ConversationID != "" && string(send.PortalKey.ID) != send.ConversationID:
		return fmt.Errorf("outbound send portal ID must match its conversation ID")
	case strings.TrimSpace(string(send.MatrixRoomID)) == "":
		return fmt.Errorf("outbound send Matrix room ID is required")
	case strings.TrimSpace(string(send.MatrixEventID)) == "":
		return fmt.Errorf("outbound send Matrix event ID is required")
	case strings.TrimSpace(string(send.MatrixSenderID)) == "":
		return fmt.Errorf("outbound send Matrix sender ID is required")
	case strings.TrimSpace(string(send.SenderID)) == "":
		return fmt.Errorf("outbound send remote sender ID is required")
	case send.SourceMediaDigest != "" && !isLowerHexDigest(send.SourceMediaDigest, md5DigestHexLength):
		return fmt.Errorf("outbound send source media fingerprint is invalid")
	}
	return validateOutboundMessageType(send.MessageType)
}

func validateOutboundSendReentry(existing, candidate *OutboundSend) error {
	var changedField string
	switch {
	case existing.BridgeID != candidate.BridgeID:
		changedField = "bridge ID"
	case existing.UserLoginID != candidate.UserLoginID:
		changedField = "user login ID"
	case existing.TransactionID != candidate.TransactionID:
		changedField = "transaction ID"
	case existing.MatrixRoomID != candidate.MatrixRoomID:
		changedField = "Matrix room ID"
	case existing.MatrixEventID != candidate.MatrixEventID:
		changedField = "Matrix event ID"
	case existing.MatrixSenderID != candidate.MatrixSenderID:
		changedField = "Matrix sender ID"
	case existing.SenderID != candidate.SenderID:
		changedField = "remote sender ID"
	case existing.InputTransactionID != candidate.InputTransactionID:
		changedField = "Matrix input transaction ID"
	case existing.MessageType != candidate.MessageType:
		changedField = "message type"
	case existing.ContentHash != candidate.ContentHash:
		changedField = "content fingerprint"
	case existing.SourceMediaDigest != candidate.SourceMediaDigest:
		changedField = "source media fingerprint"
	case existing.ConversationID != "" && candidate.ConversationID != "" &&
		existing.ConversationID != candidate.ConversationID:
		changedField = "conversation binding"
	case existing.ConversationID == candidate.ConversationID && existing.PortalKey != candidate.PortalKey &&
		!outboundPortalReentryMatchesStagedBinding(existing, candidate):
		changedField = "portal binding"
	}
	if changedField != "" {
		return fmt.Errorf("%w: %s", ErrOutboundSendIdentityChanged, changedField)
	}
	return nil
}

func outboundPortalReentryMatchesStagedBinding(existing, candidate *OutboundSend) bool {
	return existing.ConversationID == "" && candidate.ConversationID == "" &&
		existing.BindingConversationID != "" &&
		candidate.PortalKey.ID == networkid.PortalID(existing.BindingConversationID) &&
		candidate.PortalKey.Receiver == existing.PortalKey.Receiver
}

func validateNextAttempt(nextAttemptAt, now time.Time) error {
	if nextAttemptAt.IsZero() {
		return fmt.Errorf("next attempt time is required")
	}
	if nextAttemptAt.UnixMilli() <= now.UnixMilli() {
		return fmt.Errorf("next attempt time must be in the future")
	}
	return nil
}

func validateOutboundSendState(state OutboundSendState) error {
	switch state {
	case OutboundSendPrepared, OutboundSendSubmitting, OutboundSendAwaitingEcho, OutboundSendUncertain,
		OutboundSendResolved, OutboundSendCompleted, OutboundSendNotSubmitted, OutboundSendUnconfirmed:
		return nil
	default:
		return fmt.Errorf("invalid outbound send state %q", state)
	}
}

func validateOutboundMessageType(messageType OutboundMessageType) error {
	switch messageType {
	case OutboundMessageText, OutboundMessageImage, OutboundMessagePostRef:
		return nil
	default:
		return fmt.Errorf("invalid outbound message type %q", messageType)
	}
}

func marshalBaselineMessageIDs(messageIDs []networkid.MessageID) (string, error) {
	if messageIDs == nil {
		messageIDs = []networkid.MessageID{}
	}
	for _, messageID := range messageIDs {
		if strings.TrimSpace(string(messageID)) == "" {
			return "", fmt.Errorf("outbound baseline contains an empty message ID")
		}
		for _, char := range messageID {
			if unicode.IsControl(char) {
				return "", fmt.Errorf("outbound baseline contains an invalid message ID")
			}
		}
	}
	encoded, err := json.Marshal(messageIDs)
	if err != nil {
		return "", fmt.Errorf("encode outbound baseline message IDs: %w", err)
	}
	return string(encoded), nil
}

func requirePendingOutboundSend(result sql.Result, err error) error {
	changed, err := rowWasChanged(result, err)
	if err != nil {
		return err
	}
	if !changed {
		return ErrOutboundSendNotPending
	}
	return nil
}
