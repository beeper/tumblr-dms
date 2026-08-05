package tumblrdb

import (
	"context"
	"time"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

type SyncStateQuery struct {
	BridgeID networkid.BridgeID
	*dbutil.QueryHelper[*SyncState]
}

type SyncState struct {
	BridgeID            networkid.BridgeID
	UserLoginID         networkid.UserLoginID
	LastRemoteWatermark int64
	LastScanSuccessAt   time.Time
}

const (
	getSyncStateQuery = `
		SELECT bridge_id, user_login_id, last_remote_watermark_ts, last_scan_success_ts
		FROM tumblr_sync_state
		WHERE bridge_id=$1 AND user_login_id=$2
	`
	setScanSuccessQuery = `
		INSERT INTO tumblr_sync_state (
			bridge_id, user_login_id, last_remote_watermark_ts, last_scan_success_ts
		) VALUES ($1, $2, $3, $4)
		ON CONFLICT (bridge_id, user_login_id)
		DO UPDATE SET
			last_remote_watermark_ts=CASE
				WHEN excluded.last_remote_watermark_ts > tumblr_sync_state.last_remote_watermark_ts
				THEN excluded.last_remote_watermark_ts
				ELSE tumblr_sync_state.last_remote_watermark_ts
			END,
			last_scan_success_ts=excluded.last_scan_success_ts
	`
)

func (sq *SyncStateQuery) Get(ctx context.Context, loginID networkid.UserLoginID) (*SyncState, error) {
	return sq.QueryOne(ctx, getSyncStateQuery, sq.BridgeID, loginID)
}

func (sq *SyncStateQuery) SetScanSuccess(
	ctx context.Context,
	loginID networkid.UserLoginID,
	remoteWatermark int64,
	at time.Time,
) error {
	return sq.Exec(ctx, setScanSuccessQuery, sq.BridgeID, loginID, remoteWatermark, at.UnixMilli())
}

func (state *SyncState) Scan(row dbutil.Scannable) (*SyncState, error) {
	var lastScanSuccessTS int64
	err := row.Scan(
		&state.BridgeID,
		&state.UserLoginID,
		&state.LastRemoteWatermark,
		&lastScanSuccessTS,
	)
	if err != nil {
		return nil, err
	}
	state.LastScanSuccessAt = timeFromMillis(lastScanSuccessTS)
	return state, nil
}
