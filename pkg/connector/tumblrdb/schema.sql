-- Fresh-install Tumblr bridge schema.
-- Startup creates the complete current schema atomically on an empty database.

CREATE TABLE IF NOT EXISTS tumblr_conversation_sync_job (
    bridge_id       TEXT    NOT NULL,
    user_login_id   TEXT    NOT NULL,
    conversation_id TEXT    NOT NULL,
    revision        BIGINT  NOT NULL DEFAULT 1,
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    next_attempt_ts BIGINT  NOT NULL DEFAULT 0,
    last_error_code TEXT    NOT NULL DEFAULT '',
    delete_room_id  TEXT    NOT NULL DEFAULT '',
    created_ts      BIGINT  NOT NULL,
    updated_ts      BIGINT  NOT NULL,

    PRIMARY KEY (bridge_id, user_login_id, conversation_id),
    CONSTRAINT tumblr_conversation_sync_job_user_login_fkey
        FOREIGN KEY (bridge_id, user_login_id)
        REFERENCES user_login (bridge_id, id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS tumblr_conversation_sync_job_due_idx
    ON tumblr_conversation_sync_job (bridge_id, user_login_id, next_attempt_ts);

-- Outbound claims intentionally outlive user_login rows. A disconnect must not
-- erase a Tumblr POST whose remote outcome still needs reconciliation.
CREATE TABLE IF NOT EXISTS tumblr_outbound_send (
    bridge_id             TEXT    NOT NULL,
    user_login_id         TEXT    NOT NULL,
    transaction_id        TEXT    NOT NULL,
    portal_id             TEXT    NOT NULL,
    portal_receiver       TEXT    NOT NULL,
    conversation_id       TEXT    NOT NULL,
    binding_conversation_id TEXT  NOT NULL DEFAULT '',
    matrix_room_id        TEXT    NOT NULL,
    matrix_event_id       TEXT    NOT NULL,
    matrix_sender_id      TEXT    NOT NULL,
    sender_id             TEXT    NOT NULL,
    input_transaction_id   TEXT    NOT NULL,
    message_type          TEXT    NOT NULL,
    content_hash          TEXT    NOT NULL,
    source_media_digest   TEXT    NOT NULL DEFAULT '',
    baseline_ids_json     TEXT    NOT NULL DEFAULT '[]',
    remote_message_id     TEXT    NOT NULL DEFAULT '',
    state                 TEXT    NOT NULL DEFAULT 'prepared',
    terminal_ts           BIGINT  NOT NULL DEFAULT 0,
    status_notified_ts    BIGINT  NOT NULL DEFAULT 0,
    status_claim_token    TEXT    NOT NULL DEFAULT '',
    status_claim_expires_ts BIGINT NOT NULL DEFAULT 0,
    attempt_count         INTEGER NOT NULL DEFAULT 0,
    next_attempt_ts       BIGINT  NOT NULL,
    send_started_ts       BIGINT  NOT NULL,
    created_ts            BIGINT  NOT NULL,
    updated_ts            BIGINT  NOT NULL,

    PRIMARY KEY (bridge_id, user_login_id, transaction_id),
    CONSTRAINT tumblr_outbound_send_state_check
        CHECK (state IN ('prepared', 'submitting', 'awaiting_echo', 'uncertain', 'resolved', 'completed', 'not_submitted', 'unconfirmed')),
    CONSTRAINT tumblr_outbound_send_message_type_check
        CHECK (message_type IN ('TEXT', 'IMAGE', 'POSTREF')),
    CONSTRAINT tumblr_outbound_send_content_hash_check
        CHECK (length(content_hash) = 64),
    CONSTRAINT tumblr_outbound_send_source_media_digest_check
        CHECK (source_media_digest = '' OR length(source_media_digest) = 32),
    CONSTRAINT tumblr_outbound_send_matrix_room_id_check
        CHECK (matrix_room_id <> ''),
    CONSTRAINT tumblr_outbound_send_binding_state_check
        CHECK (binding_conversation_id = '' OR conversation_id = ''),
    CONSTRAINT tumblr_outbound_send_attempt_count_check
        CHECK (attempt_count >= 0),
    CONSTRAINT tumblr_outbound_send_terminal_ts_check
        CHECK (terminal_ts >= 0),
    CONSTRAINT tumblr_outbound_send_status_notified_ts_check
        CHECK (status_notified_ts >= 0),
    CONSTRAINT tumblr_outbound_send_status_claim_check
        CHECK (
            (status_claim_token = '' AND status_claim_expires_ts = 0)
            OR (
                status_claim_token <> ''
                AND status_claim_expires_ts > 0
                AND terminal_ts > 0
                AND state IN ('not_submitted', 'unconfirmed')
            )
        ),
    CONSTRAINT tumblr_outbound_send_status_finished_check
        CHECK (
            status_notified_ts = 0
            OR (
                status_claim_token = ''
                AND terminal_ts > 0
                AND state IN ('not_submitted', 'unconfirmed')
            )
        ),
    CONSTRAINT tumblr_outbound_send_terminal_state_check
        CHECK (
            terminal_ts = 0
            OR (
                next_attempt_ts > terminal_ts
                AND (
                    (state IN ('not_submitted', 'unconfirmed') AND remote_message_id = '')
                    OR (state = 'completed' AND remote_message_id <> '')
                )
            )
        ),
    CONSTRAINT tumblr_outbound_send_state_identity_check
        CHECK (
            (state IN ('resolved', 'completed') AND remote_message_id <> '')
            OR (state NOT IN ('resolved', 'completed') AND remote_message_id = '')
        ),
    CONSTRAINT tumblr_outbound_send_bound_state_check
        CHECK (state NOT IN ('awaiting_echo', 'resolved', 'completed') OR conversation_id <> ''),
    CONSTRAINT tumblr_outbound_send_next_attempt_check
        CHECK (next_attempt_ts > 0),
    CONSTRAINT tumblr_outbound_send_timestamps_check
        CHECK (send_started_ts > 0 AND created_ts > 0 AND updated_ts > 0)
);

CREATE INDEX IF NOT EXISTS tumblr_outbound_send_due_idx
    ON tumblr_outbound_send (bridge_id, user_login_id, next_attempt_ts);

CREATE INDEX IF NOT EXISTS tumblr_outbound_send_match_idx
    ON tumblr_outbound_send (
        bridge_id, user_login_id, conversation_id, state, send_started_ts
    );

CREATE UNIQUE INDEX IF NOT EXISTS tumblr_outbound_send_remote_idx
    ON tumblr_outbound_send (
        bridge_id, user_login_id, conversation_id, remote_message_id
    )
    WHERE remote_message_id <> '';

CREATE UNIQUE INDEX IF NOT EXISTS tumblr_outbound_send_matrix_event_idx
    ON tumblr_outbound_send (bridge_id, matrix_event_id);

-- Permanent, content-free idempotency receipts. Full failure rows may expire,
-- but an exact Matrix event can never become eligible for another Tumblr POST.
CREATE TABLE IF NOT EXISTS tumblr_outbound_receipt (
    bridge_id       TEXT   NOT NULL,
    matrix_event_id TEXT   NOT NULL,
    user_login_id   TEXT   NOT NULL,
    transaction_id  TEXT   NOT NULL,
    terminal_ts     BIGINT NOT NULL,

    PRIMARY KEY (bridge_id, matrix_event_id),
    CONSTRAINT tumblr_outbound_receipt_transaction_unique
        UNIQUE (bridge_id, user_login_id, transaction_id),
    CONSTRAINT tumblr_outbound_receipt_terminal_ts_check
        CHECK (terminal_ts > 0)
);

CREATE TABLE IF NOT EXISTS tumblr_sync_state (
    bridge_id                TEXT   NOT NULL,
    user_login_id            TEXT   NOT NULL,
    last_remote_watermark_ts BIGINT NOT NULL DEFAULT 0,
    last_scan_success_ts     BIGINT NOT NULL DEFAULT 0,

    PRIMARY KEY (bridge_id, user_login_id),
    CONSTRAINT tumblr_sync_state_user_login_fkey
        FOREIGN KEY (bridge_id, user_login_id)
        REFERENCES user_login (bridge_id, id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tumblr_conversation_sync (
    bridge_id                 TEXT   NOT NULL,
    user_login_id             TEXT   NOT NULL,
    conversation_id           TEXT   NOT NULL,
    completed_head_message_id TEXT   NOT NULL,
    updated_ts                BIGINT NOT NULL,

    PRIMARY KEY (bridge_id, user_login_id, conversation_id),
    CONSTRAINT tumblr_conversation_sync_head_message_id_check
        CHECK (completed_head_message_id <> ''),
    CONSTRAINT tumblr_conversation_sync_updated_ts_check
        CHECK (updated_ts > 0),
    CONSTRAINT tumblr_conversation_sync_user_login_fkey
        FOREIGN KEY (bridge_id, user_login_id)
        REFERENCES user_login (bridge_id, id) ON UPDATE CASCADE ON DELETE CASCADE
);
