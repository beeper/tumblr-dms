package tumblrdb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

const (
	portalReIDLockNamespace         = "github.com/beeper/tumblr-dms/portal-reid"
	outboundSubmissionLockNamespace = "github.com/beeper/tumblr-dms/outbound-submission"
	portalReIDLockPollDelay         = 50 * time.Millisecond
	portalReIDUnlockTimeout         = 5 * time.Second
)

type postgresAdvisoryLockSession struct {
	conn      *sql.Conn
	queryLock sync.Mutex
}

type postgresAdvisoryLockSessionContextKey struct{}

// AcquirePortalReIDLock serializes portal ReID operations across bridge
// processes that share this database. The caller must keep its process-local
// portal graph mutex held too, and must call the returned release function.
func (db *Database) AcquirePortalReIDLock(ctx context.Context) (func(), error) {
	if db == nil || db.Database == nil || db.RawDB == nil {
		return nil, fmt.Errorf("tumblr database is unavailable")
	}
	bridgeID, err := db.portalReIDBridgeID()
	if err != nil {
		return nil, err
	}

	switch db.Dialect {
	case dbutil.Postgres:
		if session, ok := ctx.Value(postgresAdvisoryLockSessionContextKey{}).(*postgresAdvisoryLockSession); ok && session != nil {
			return db.acquirePostgresAdvisoryLockOnSession(
				ctx,
				session,
				portalReIDAdvisoryLockKey(bridgeID),
				"portal mutation",
			)
		}
		return db.acquirePostgresPortalReIDLock(ctx, bridgeID)
	case dbutil.SQLite:
		return db.acquireSQLitePortalReIDLock(ctx, bridgeID)
	default:
		return nil, fmt.Errorf("acquire Tumblr portal ReID lock: unsupported database dialect %q", db.Dialect.String())
	}
}

// AcquireOutboundSubmissionLock fences one login's Tumblr submissions across
// bridge processes. The returned context carries the reserved PostgreSQL
// session so a nested portal lock can reuse it instead of exhausting the pool.
// The caller must also hold the connector's process-local lock for this login
// and must call the returned release function.
func (db *Database) AcquireOutboundSubmissionLock(
	ctx context.Context,
	loginID networkid.UserLoginID,
) (context.Context, func(), error) {
	if db == nil || db.Database == nil || db.RawDB == nil {
		return ctx, nil, fmt.Errorf("tumblr database is unavailable")
	}
	if strings.TrimSpace(string(loginID)) == "" {
		return ctx, nil, fmt.Errorf("tumblr outbound submission login is unavailable")
	}
	bridgeID, err := db.portalReIDBridgeID()
	if err != nil {
		return ctx, nil, err
	}

	switch db.Dialect {
	case dbutil.Postgres:
		return db.acquirePostgresAdvisoryLockSession(
			ctx,
			outboundSubmissionAdvisoryLockKey(bridgeID, loginID),
			"outbound submission",
		)
	case dbutil.SQLite:
		release, lockErr := db.acquireSQLiteOutboundSubmissionLock(ctx, bridgeID, loginID)
		return ctx, release, lockErr
	default:
		return ctx, nil, fmt.Errorf("acquire Tumblr outbound submission lock: unsupported database dialect %q", db.Dialect.String())
	}
}

func (db *Database) portalReIDBridgeID() (string, error) {
	var bridgeID string
	hasBridgeID := false
	setBridgeID := func(candidate string) error {
		if !hasBridgeID {
			bridgeID = candidate
			hasBridgeID = true
			return nil
		} else if bridgeID != candidate {
			return fmt.Errorf("tumblr database query helpers have inconsistent bridge IDs")
		}
		return nil
	}

	if db.Jobs != nil {
		if err := setBridgeID(string(db.Jobs.BridgeID)); err != nil {
			return "", err
		}
	}
	if db.Outbound != nil {
		if err := setBridgeID(string(db.Outbound.BridgeID)); err != nil {
			return "", err
		}
	}
	if db.SyncState != nil {
		if err := setBridgeID(string(db.SyncState.BridgeID)); err != nil {
			return "", err
		}
	}
	if db.ConversationSync != nil {
		if err := setBridgeID(string(db.ConversationSync.BridgeID)); err != nil {
			return "", err
		}
	}
	if !hasBridgeID {
		return "", fmt.Errorf("tumblr database query helpers are unavailable")
	}
	// Standalone mxmain bridges intentionally use an empty bridge ID. It is
	// still a stable coordination identity: the lock namespaces are specific to
	// Tumblr, and SQLite additionally includes the canonical database path.
	return bridgeID, nil
}

func (db *Database) acquirePostgresPortalReIDLock(ctx context.Context, bridgeID string) (func(), error) {
	_, release, err := db.acquirePostgresAdvisoryLockSession(
		ctx,
		portalReIDAdvisoryLockKey(bridgeID),
		"portal mutation",
	)
	return release, err
}

func (db *Database) acquirePostgresAdvisoryLockSession(
	ctx context.Context,
	lockKey int64,
	description string,
) (context.Context, func(), error) {
	maxConnections := db.RawDB.Stats().MaxOpenConnections
	if maxConnections == 1 {
		return ctx, nil, fmt.Errorf(
			"acquire PostgreSQL Tumblr %s lock: database pool needs at least two connections",
			description,
		)
	}
	releaseSlot, err := db.acquirePostgresCoordinationSlot(ctx)
	if err != nil {
		return ctx, nil, err
	}
	conn, err := db.RawDB.Conn(ctx)
	if err != nil {
		releaseSlot()
		return ctx, nil, fmt.Errorf("reserve PostgreSQL connection for Tumblr %s lock: %w", description, err)
	}
	session := &postgresAdvisoryLockSession{conn: conn}
	releaseLock, err := db.acquirePostgresAdvisoryLockOnSession(ctx, session, lockKey, description)
	if err != nil {
		_ = conn.Close()
		releaseSlot()
		return ctx, nil, err
	}
	var releaseOnce sync.Once
	return context.WithValue(ctx, postgresAdvisoryLockSessionContextKey{}, session), func() {
		releaseOnce.Do(func() {
			releaseLock()
			_ = conn.Close()
			releaseSlot()
		})
	}, nil
}

func (db *Database) acquirePostgresAdvisoryLockOnSession(
	ctx context.Context,
	session *postgresAdvisoryLockSession,
	lockKey int64,
	description string,
) (func(), error) {
	if session == nil || session.conn == nil {
		return nil, fmt.Errorf("postgresql Tumblr %s lock session is unavailable", description)
	}
	for {
		var acquired bool
		session.queryLock.Lock()
		err := session.conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, lockKey).Scan(&acquired)
		session.queryLock.Unlock()
		if err != nil {
			return nil, fmt.Errorf("acquire PostgreSQL Tumblr %s lock: %w", description, err)
		}
		if acquired {
			var releaseOnce sync.Once
			return func() {
				releaseOnce.Do(func() {
					db.releasePostgresAdvisoryLock(ctx, session, lockKey, description)
				})
			}, nil
		}
		if err = waitForPortalReIDLock(ctx); err != nil {
			return nil, err
		}
	}
}

func (db *Database) acquirePostgresCoordinationSlot(ctx context.Context) (func(), error) {
	maxConnections := db.RawDB.Stats().MaxOpenConnections
	db.postgresCoordinationSlotsOnce.Do(func() {
		if maxConnections > 1 {
			db.postgresCoordinationSlots = make(chan struct{}, maxConnections-1)
		}
	})
	if db.postgresCoordinationSlots == nil {
		return func() {}, nil
	}
	select {
	case db.postgresCoordinationSlots <- struct{}{}:
		var releaseOnce sync.Once
		return func() {
			releaseOnce.Do(func() { <-db.postgresCoordinationSlots })
		}, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("wait for a PostgreSQL Tumblr coordination connection: %w", ctx.Err())
	}
}

func (db *Database) releasePostgresAdvisoryLock(
	logCtx context.Context,
	session *postgresAdvisoryLockSession,
	lockKey int64,
	description string,
) {
	ctx, cancel := context.WithTimeout(context.Background(), portalReIDUnlockTimeout)
	defer cancel()

	var unlocked bool
	session.queryLock.Lock()
	err := session.conn.QueryRowContext(ctx, `SELECT pg_advisory_unlock($1)`, lockKey).Scan(&unlocked)
	session.queryLock.Unlock()
	if err != nil || !unlocked {
		if err == nil {
			err = errors.New("postgresql session did not own the advisory lock")
		}
		zerolog.Ctx(logCtx).Warn().Err(err).Str("lock", description).
			Msg("Failed to release Tumblr advisory lock cleanly")
		// Never return a session with an uncertain advisory-lock state to the
		// pool. Marking it bad makes database/sql close the driver connection,
		// which also releases all session advisory locks.
		_ = session.conn.Raw(func(any) error { return driver.ErrBadConn })
	}
}

func (db *Database) acquireSQLiteOutboundSubmissionLock(
	ctx context.Context,
	bridgeID string,
	loginID networkid.UserLoginID,
) (func(), error) {
	mainPath, err := sqliteMainDatabasePath(ctx, db.RawDB)
	if err != nil {
		return nil, err
	}
	if mainPath == "" || mainPath == ":memory:" {
		return func() {}, nil
	}

	absolutePath, err := filepath.Abs(mainPath)
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite main database path for Tumblr outbound submission lock: %w", err)
	}
	canonicalPath, err := filepath.EvalSymlinks(absolutePath)
	if err != nil {
		return nil, fmt.Errorf("canonicalize SQLite main database path for Tumblr outbound submission lock: %w", err)
	}
	digest := sha256.Sum256([]byte(
		outboundSubmissionLockNamespace + "\x00" + canonicalPath + "\x00" + bridgeID + "\x00" + string(loginID),
	))
	lockPath := filepath.Join(
		filepath.Dir(canonicalPath),
		".tumblr-outbound-submission-"+hex.EncodeToString(digest[:])+".lock",
	)
	return acquirePortalReIDFileLock(ctx, lockPath)
}

func (db *Database) acquireSQLitePortalReIDLock(ctx context.Context, bridgeID string) (func(), error) {
	mainPath, err := sqliteMainDatabasePath(ctx, db.RawDB)
	if err != nil {
		return nil, err
	}
	if mainPath == "" || mainPath == ":memory:" {
		// In-memory SQLite databases cannot be shared by separate processes.
		return func() {}, nil
	}

	absolutePath, err := filepath.Abs(mainPath)
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite main database path for Tumblr portal ReID lock: %w", err)
	}
	canonicalPath, err := filepath.EvalSymlinks(absolutePath)
	if err != nil {
		return nil, fmt.Errorf("canonicalize SQLite main database path for Tumblr portal ReID lock: %w", err)
	}
	digest := sha256.Sum256([]byte(portalReIDLockNamespace + "\x00" + canonicalPath + "\x00" + bridgeID))
	lockPath := filepath.Join(
		filepath.Dir(canonicalPath),
		".tumblr-portal-reid-"+hex.EncodeToString(digest[:])+".lock",
	)
	return acquirePortalReIDFileLock(ctx, lockPath)
}

func sqliteMainDatabasePath(ctx context.Context, rawDB *sql.DB) (string, error) {
	conn, err := rawDB.Conn(ctx)
	if err != nil {
		return "", fmt.Errorf("reserve SQLite connection for Tumblr portal ReID lock: %w", err)
	}
	defer func() { _ = conn.Close() }()

	rows, err := conn.QueryContext(ctx, `PRAGMA database_list`)
	if err != nil {
		return "", fmt.Errorf("read SQLite database path for Tumblr portal ReID lock: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var sequence int
		var name, path string
		if err = rows.Scan(&sequence, &name, &path); err != nil {
			return "", fmt.Errorf("scan SQLite database path for Tumblr portal ReID lock: %w", err)
		}
		if strings.EqualFold(name, "main") {
			return path, nil
		}
	}
	if err = rows.Err(); err != nil {
		return "", fmt.Errorf("read SQLite database path for Tumblr portal ReID lock: %w", err)
	}
	return "", fmt.Errorf("SQLite main database is unavailable for Tumblr portal ReID lock")
}

func portalReIDAdvisoryLockKey(bridgeID string) int64 {
	digest := sha256.Sum256([]byte(portalReIDLockNamespace + "\x00" + bridgeID))
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

func outboundSubmissionAdvisoryLockKey(bridgeID string, loginID networkid.UserLoginID) int64 {
	digest := sha256.Sum256([]byte(outboundSubmissionLockNamespace + "\x00" + bridgeID + "\x00" + string(loginID)))
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

func waitForPortalReIDLock(ctx context.Context) error {
	timer := time.NewTimer(portalReIDLockPollDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for Tumblr portal ReID lock: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}
