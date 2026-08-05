package tumblrdb

import (
	"context"
	_ "embed"
	"fmt"
)

//go:embed schema.sql
var freshInstallSchema string

var schemaValidationQueries = []string{
	`SELECT bridge_id, user_login_id, conversation_id, revision, attempt_count,
		next_attempt_ts, last_error_code, delete_room_id, created_ts, updated_ts
	 FROM tumblr_conversation_sync_job WHERE 1=0`,
	`SELECT bridge_id, user_login_id, transaction_id, portal_id, portal_receiver,
		conversation_id, binding_conversation_id, matrix_room_id, matrix_event_id,
		matrix_sender_id, sender_id, input_transaction_id, message_type, content_hash,
		source_media_digest, baseline_ids_json, remote_message_id, state, terminal_ts, status_notified_ts,
		status_claim_token, status_claim_expires_ts, attempt_count, next_attempt_ts,
		send_started_ts, created_ts, updated_ts
	 FROM tumblr_outbound_send WHERE 1=0`,
	`SELECT bridge_id, user_login_id, last_remote_watermark_ts, last_scan_success_ts
	 FROM tumblr_sync_state WHERE 1=0`,
	`SELECT bridge_id, user_login_id, conversation_id, completed_head_message_id, updated_ts
	 FROM tumblr_conversation_sync WHERE 1=0`,
	`SELECT bridge_id, matrix_event_id, user_login_id, transaction_id, terminal_ts
	 FROM tumblr_outbound_receipt WHERE 1=0`,
}

// Initialize creates the complete Tumblr-owned schema in one transaction. The
// bridge has no historical Tumblr schema to upgrade, so startup deliberately
// validates the current shape instead of maintaining a version chain.
func (db *Database) Initialize(ctx context.Context) error {
	if db == nil || db.Database == nil {
		return fmt.Errorf("tumblr database is unavailable")
	}
	return db.DoTxn(ctx, nil, func(ctx context.Context) error {
		if _, err := db.Exec(ctx, freshInstallSchema); err != nil {
			return fmt.Errorf("create fresh Tumblr schema: %w", err)
		}
		for _, query := range schemaValidationQueries {
			rows, err := db.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("validate fresh Tumblr schema: %w", err)
			}
			if err = rows.Close(); err != nil {
				return fmt.Errorf("finish validating fresh Tumblr schema: %w", err)
			}
		}
		return nil
	})
}
