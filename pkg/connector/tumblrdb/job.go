package tumblrdb

import (
	"context"
	"database/sql"
	"time"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

type JobErrorCode string

const (
	JobErrorUnknown         JobErrorCode = "unknown"
	JobErrorAuth            JobErrorCode = "auth"
	JobErrorNetwork         JobErrorCode = "network"
	JobErrorRateLimited     JobErrorCode = "rate_limited"
	JobErrorRemote          JobErrorCode = "remote"
	JobErrorInvalidResponse JobErrorCode = "invalid_response"
	JobErrorQueue           JobErrorCode = "queue"
	JobErrorDatabase        JobErrorCode = "database"
)

type ConversationJobQuery struct {
	BridgeID networkid.BridgeID
	*dbutil.QueryHelper[*ConversationJob]
}

type ConversationJob struct {
	BridgeID       networkid.BridgeID
	UserLoginID    networkid.UserLoginID
	ConversationID string
	Revision       int64
	AttemptCount   int
	NextAttemptAt  time.Time
	LastErrorCode  JobErrorCode
	DeleteRoomID   id.RoomID
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

const (
	ensureConversationJobQuery = `
		INSERT INTO tumblr_conversation_sync_job (
			bridge_id, user_login_id, conversation_id, revision, next_attempt_ts, created_ts, updated_ts
		) VALUES ($1, $2, $3, 1, 0, $4, $4)
		ON CONFLICT (bridge_id, user_login_id, conversation_id) DO NOTHING
	`
	putConversationJobQuery = `
		INSERT INTO tumblr_conversation_sync_job (
			bridge_id, user_login_id, conversation_id, revision, next_attempt_ts, created_ts, updated_ts
		) VALUES ($1, $2, $3, 1, 0, $4, $4)
		ON CONFLICT (bridge_id, user_login_id, conversation_id)
		DO UPDATE SET
			revision=tumblr_conversation_sync_job.revision+1,
			attempt_count=0,
			next_attempt_ts=0,
			last_error_code='',
			updated_ts=excluded.updated_ts
	`
	putLiveConversationJobQuery = `
		INSERT INTO tumblr_conversation_sync_job (
			bridge_id, user_login_id, conversation_id, revision, next_attempt_ts, created_ts, updated_ts
		) VALUES ($1, $2, $3, 1, 0, $4, $4)
		ON CONFLICT (bridge_id, user_login_id, conversation_id)
		DO UPDATE SET
			revision=tumblr_conversation_sync_job.revision+1,
			attempt_count=0,
			next_attempt_ts=0,
			last_error_code='',
			delete_room_id='',
			updated_ts=excluded.updated_ts
	`
	getNextDueConversationJobQuery = `
		SELECT bridge_id, user_login_id, conversation_id, revision, attempt_count, next_attempt_ts,
		       last_error_code, delete_room_id, created_ts, updated_ts
		FROM tumblr_conversation_sync_job
		WHERE bridge_id=$1 AND user_login_id=$2 AND next_attempt_ts<=$3
		ORDER BY next_attempt_ts, updated_ts
		LIMIT 1
	`
	getConversationJobQuery = `
		SELECT bridge_id, user_login_id, conversation_id, revision, attempt_count, next_attempt_ts,
		       last_error_code, delete_room_id, created_ts, updated_ts
		FROM tumblr_conversation_sync_job
		WHERE bridge_id=$1 AND user_login_id=$2 AND conversation_id=$3
	`
	setConversationJobDeleteRoomQuery = `
		UPDATE tumblr_conversation_sync_job
		SET delete_room_id=$5, updated_ts=$6
		WHERE bridge_id=$1 AND user_login_id=$2 AND conversation_id=$3 AND revision=$4
	`
	scheduleConversationJobDeleteRoomQuery = `
		INSERT INTO tumblr_conversation_sync_job (
			bridge_id, user_login_id, conversation_id, revision, next_attempt_ts,
			delete_room_id, created_ts, updated_ts
		) VALUES ($1, $2, $3, 1, $4, $5, $6, $6)
		ON CONFLICT (bridge_id, user_login_id, conversation_id)
		DO UPDATE SET
			revision=tumblr_conversation_sync_job.revision+1,
			attempt_count=0,
			next_attempt_ts=excluded.next_attempt_ts,
			last_error_code='',
			delete_room_id=excluded.delete_room_id,
			updated_ts=excluded.updated_ts
	`
	markConversationJobRetryQuery = `
		UPDATE tumblr_conversation_sync_job
		SET revision=revision+1, attempt_count=attempt_count+1, next_attempt_ts=$5,
			last_error_code=$6, updated_ts=$7
		WHERE bridge_id=$1 AND user_login_id=$2 AND conversation_id=$3 AND revision=$4
	`
	deleteConversationJobQuery = `
		DELETE FROM tumblr_conversation_sync_job
		WHERE bridge_id=$1 AND user_login_id=$2 AND conversation_id=$3 AND revision=$4
	`
	deleteAllConversationJobsQuery = `
		DELETE FROM tumblr_conversation_sync_job
		WHERE bridge_id=$1 AND user_login_id=$2
	`
	countConversationJobsQuery = `
		SELECT COUNT(*) FROM tumblr_conversation_sync_job
		WHERE bridge_id=$1 AND user_login_id=$2
	`
)

// Ensure persists a crash-recovery continuation without superseding newer
// live evidence or an already scheduled room deletion.
func (jq *ConversationJobQuery) Ensure(ctx context.Context, loginID networkid.UserLoginID, conversationID string) error {
	now := time.Now()
	return jq.Exec(ctx, ensureConversationJobQuery, jq.BridgeID, loginID, conversationID, now.UnixMilli())
}

func (jq *ConversationJobQuery) Put(ctx context.Context, loginID networkid.UserLoginID, conversationID string) error {
	now := time.Now()
	return jq.Exec(ctx, putConversationJobQuery, jq.BridgeID, loginID, conversationID, now.UnixMilli())
}

// PutLiveConversation records direct evidence that the conversation exists.
// In addition to waking sync, it cancels any persisted deletion continuation.
func (jq *ConversationJobQuery) PutLiveConversation(
	ctx context.Context,
	loginID networkid.UserLoginID,
	conversationID string,
) error {
	now := time.Now()
	return jq.Exec(ctx, putLiveConversationJobQuery, jq.BridgeID, loginID, conversationID, now.UnixMilli())
}

func (jq *ConversationJobQuery) GetNextDue(ctx context.Context, loginID networkid.UserLoginID, now time.Time) (*ConversationJob, error) {
	return jq.QueryOne(ctx, getNextDueConversationJobQuery, jq.BridgeID, loginID, now.UnixMilli())
}

func (jq *ConversationJobQuery) Get(
	ctx context.Context,
	loginID networkid.UserLoginID,
	conversationID string,
) (*ConversationJob, error) {
	return jq.QueryOne(ctx, getConversationJobQuery, jq.BridgeID, loginID, conversationID)
}

func (jq *ConversationJobQuery) SetDeleteRoom(
	ctx context.Context,
	loginID networkid.UserLoginID,
	conversationID string,
	revision int64,
	roomID id.RoomID,
) (bool, error) {
	if roomID == "" {
		return false, nil
	}
	now := time.Now()
	result, err := jq.GetDB().Exec(
		ctx,
		setConversationJobDeleteRoomQuery,
		jq.BridgeID,
		loginID,
		conversationID,
		revision,
		roomID,
		now.UnixMilli(),
	)
	return rowWasChanged(result, err)
}

// ScheduleDeleteRoom supersedes any previously loaded sync job and delays the
// crash-recovery continuation until notBefore. This gives bridgev2's
// synchronous post-handler room deletion a chance to finish without racing a
// second cleanup worker.
func (jq *ConversationJobQuery) ScheduleDeleteRoom(
	ctx context.Context,
	loginID networkid.UserLoginID,
	conversationID string,
	roomID id.RoomID,
	notBefore time.Time,
) error {
	if roomID == "" {
		return nil
	}
	now := time.Now()
	if notBefore.Before(now) {
		notBefore = now
	}
	return jq.Exec(
		ctx,
		scheduleConversationJobDeleteRoomQuery,
		jq.BridgeID,
		loginID,
		conversationID,
		notBefore.UnixMilli(),
		roomID,
		now.UnixMilli(),
	)
}

func (jq *ConversationJobQuery) MarkRetry(
	ctx context.Context,
	loginID networkid.UserLoginID,
	conversationID string,
	revision int64,
	nextAttemptAt time.Time,
	errorCode JobErrorCode,
) (bool, error) {
	now := time.Now()
	result, err := jq.GetDB().Exec(
		ctx,
		markConversationJobRetryQuery,
		jq.BridgeID,
		loginID,
		conversationID,
		revision,
		nextAttemptAt.UnixMilli(),
		normalizeJobErrorCode(errorCode),
		now.UnixMilli(),
	)
	return rowWasChanged(result, err)
}

func (jq *ConversationJobQuery) Delete(
	ctx context.Context,
	loginID networkid.UserLoginID,
	conversationID string,
	revision int64,
) (bool, error) {
	result, err := jq.GetDB().Exec(ctx, deleteConversationJobQuery, jq.BridgeID, loginID, conversationID, revision)
	return rowWasChanged(result, err)
}

func (jq *ConversationJobQuery) DeleteAll(ctx context.Context, loginID networkid.UserLoginID) error {
	return jq.Exec(ctx, deleteAllConversationJobsQuery, jq.BridgeID, loginID)
}

func (jq *ConversationJobQuery) Count(ctx context.Context, loginID networkid.UserLoginID) (int, error) {
	var count int
	err := jq.GetDB().QueryRow(ctx, countConversationJobsQuery, jq.BridgeID, loginID).Scan(&count)
	return count, err
}

func (job *ConversationJob) Scan(row dbutil.Scannable) (*ConversationJob, error) {
	var nextAttemptTS, createdTS, updatedTS int64
	err := row.Scan(
		&job.BridgeID,
		&job.UserLoginID,
		&job.ConversationID,
		&job.Revision,
		&job.AttemptCount,
		&nextAttemptTS,
		&job.LastErrorCode,
		&job.DeleteRoomID,
		&createdTS,
		&updatedTS,
	)
	if err != nil {
		return nil, err
	}
	job.NextAttemptAt = timeFromMillis(nextAttemptTS)
	job.CreatedAt = timeFromMillis(createdTS)
	job.UpdatedAt = timeFromMillis(updatedTS)
	return job, nil
}

func rowWasChanged(result sql.Result, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	rowsAffected, err := result.RowsAffected()
	return rowsAffected > 0, err
}

func normalizeJobErrorCode(code JobErrorCode) JobErrorCode {
	switch code {
	case "",
		JobErrorUnknown,
		JobErrorAuth,
		JobErrorNetwork,
		JobErrorRateLimited,
		JobErrorRemote,
		JobErrorInvalidResponse,
		JobErrorQueue,
		JobErrorDatabase:
		return code
	default:
		return JobErrorUnknown
	}
}

func timeFromMillis(timestamp int64) time.Time {
	if timestamp <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(timestamp)
}
