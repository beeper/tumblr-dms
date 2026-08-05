package tumblrdb

import (
	"sync"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

type Database struct {
	*dbutil.Database
	Jobs             *ConversationJobQuery
	Outbound         *OutboundSendQuery
	SyncState        *SyncStateQuery
	ConversationSync *ConversationSyncQuery

	postgresCoordinationSlotsOnce sync.Once
	postgresCoordinationSlots     chan struct{}
}

func New(bridgeID networkid.BridgeID, db *dbutil.Database) *Database {
	return &Database{
		Database: db,
		Jobs: &ConversationJobQuery{
			BridgeID: bridgeID,
			QueryHelper: dbutil.MakeQueryHelper(db, func(_ *dbutil.QueryHelper[*ConversationJob]) *ConversationJob {
				return &ConversationJob{}
			}),
		},
		Outbound: &OutboundSendQuery{
			BridgeID: bridgeID,
			QueryHelper: dbutil.MakeQueryHelper(db, func(_ *dbutil.QueryHelper[*OutboundSend]) *OutboundSend {
				return &OutboundSend{}
			}),
		},
		SyncState: &SyncStateQuery{
			BridgeID: bridgeID,
			QueryHelper: dbutil.MakeQueryHelper(db, func(_ *dbutil.QueryHelper[*SyncState]) *SyncState {
				return &SyncState{}
			}),
		},
		ConversationSync: &ConversationSyncQuery{
			BridgeID: bridgeID,
			QueryHelper: dbutil.MakeQueryHelper(db, func(_ *dbutil.QueryHelper[*ConversationSync]) *ConversationSync {
				return &ConversationSync{}
			}),
		},
	}
}
