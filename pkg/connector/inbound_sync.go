package connector

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/ifixrobots/tumblr-dms/pkg/connector/tumblrdb"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblrid"
)

const (
	tumblrReconciliationInterval         = 5 * time.Minute
	tumblrDeletionReconciliationInterval = 5 * time.Minute
	tumblrDeletionReconciliationStart    = 30 * time.Second
	tumblrConversationHeadProbeDefault   = 30 * time.Second
	tumblrConversationHeadProbeIdle      = time.Minute
	tumblrConversationHeadProbeHotWindow = 5 * time.Minute
	tumblrJobPollInterval                = 2 * time.Second
	tumblrJobRetryBase                   = 2 * time.Second
	tumblrJobRetryMax                    = 5 * time.Minute
)

var errTumblrConversationJobSuperseded = errors.New("tumblr conversation job was superseded")

func (tc *TumblrClient) ensureInboundSyncStarted(generation *connectionGeneration) {
	if generation == nil {
		return
	}
	generation.inboundOnce.Do(func() {
		tc.startInboundSync(generation)
	})
	tc.wakeInboundSync()
}

func (tc *TumblrClient) startInboundSync(generation *connectionGeneration) {
	if tc == nil || generation == nil || !tc.isCurrentGeneration(generation) ||
		tc.connector == nil || tc.connector.DB == nil || tc.userLogin == nil {
		return
	}
	tc.inboundLock.Lock()
	if tc.inboundGeneration == generation.id {
		tc.inboundLock.Unlock()
		return
	}
	tc.inboundGeneration = generation.id
	tc.inboundWake = make(chan struct{}, 1)
	wake := tc.inboundWake
	generation.wg.Add(1)
	tc.inboundLock.Unlock()
	go tc.inboundSyncLoop(generation, wake)
}

func (tc *TumblrClient) wakeInboundSync() {
	if tc == nil {
		return
	}
	tc.inboundLock.Lock()
	wake := tc.inboundWake
	tc.inboundLock.Unlock()
	if wake == nil {
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (tc *TumblrClient) enqueueConversationSync(ctx context.Context, conversationID string) error {
	if !validRemoteID(conversationID) {
		return fmt.Errorf("tumblr conversation ID is invalid")
	}
	if tc == nil || tc.connector == nil || tc.connector.DB == nil || tc.userLogin == nil {
		return fmt.Errorf("tumblr durable sync database is unavailable")
	}
	if err := tc.putLiveConversationJob(ctx, conversationID); err != nil {
		return fmt.Errorf("failed to persist Tumblr conversation sync job: %w", err)
	}
	tc.wakeInboundSync()
	return nil
}

// putLiveConversationJob serializes new remote evidence with delete handling.
// A push or reconciliation fetch must not clear or replace a deletion job while
// another process is using that job to mutate the portal graph.
func (tc *TumblrClient) putLiveConversationJob(ctx context.Context, conversationID string) error {
	lockCtx, releaseSubmission, err := tc.acquireOutboundSubmissionLock(ctx)
	if err != nil {
		return err
	}
	defer releaseSubmission()
	return tc.connector.DB.Jobs.PutLiveConversation(lockCtx, tc.userLogin.ID, conversationID)
}

func (tc *TumblrClient) inboundSyncLoop(generation *connectionGeneration, wake <-chan struct{}) {
	defer generation.wg.Done()
	defer func() {
		tc.inboundLock.Lock()
		if tc.inboundGeneration == generation.id {
			tc.inboundGeneration = 0
			tc.inboundWake = nil
		}
		tc.inboundLock.Unlock()
	}()
	ctx := generation.ctx
	if log := tc.log(); log != nil {
		log.Info().Msg("Started durable Tumblr inbound sync")
		defer log.Info().Msg("Stopped durable Tumblr inbound sync")
	}

	poll := time.NewTicker(tumblrJobPollInterval)
	defer poll.Stop()
	reconcile := time.NewTimer(0)
	defer reconcile.Stop()
	headProbe := time.NewTimer(tumblrConversationHeadProbeDefault)
	defer headProbe.Stop()
	observedHeads := make(map[string]string)
	headProbeHotUntil := time.Time{}
	deletionReconcile := time.NewTimer(jitterDuration(
		tumblrDeletionReconciliationStart,
		tumblrDeletionReconciliationStart,
	))
	defer deletionReconcile.Stop()
	for {
		if err := tc.processDueConversationJobs(ctx, 0); err != nil && ctx.Err() == nil {
			if log := tc.log(); log != nil {
				log.Warn().Err(err).Msg("Durable Tumblr inbound sync pass failed")
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-poll.C:
		case <-headProbe.C:
			latestModifiedTS, changed, err := tc.probeChangedConversationHeads(ctx, observedHeads)
			now := time.Now()
			nextProbe := tumblrConversationHeadProbeIdle
			if err != nil {
				headProbeHotUntil = time.Time{}
				if ctx.Err() == nil {
					if log := tc.log(); log != nil {
						log.Warn().Err(err).Msg("Tumblr conversation head probe failed")
					}
				}
			} else {
				if changed {
					headProbeHotUntil = now.Add(tumblrConversationHeadProbeHotWindow)
				}
				if now.Before(headProbeHotUntil) {
					nextProbe = tumblrConversationHeadProbeDelay(now, latestModifiedTS)
					if nextProbe >= tumblrConversationHeadProbeIdle {
						headProbeHotUntil = time.Time{}
					}
				}
			}
			headProbe.Reset(nextProbe)
		case <-reconcile.C:
			if err := tc.reconcileConversationHead(ctx); err != nil && ctx.Err() == nil {
				if log := tc.log(); log != nil {
					log.Warn().Err(err).Msg("Tumblr conversation reconciliation failed")
				}
			}
			reconcile.Reset(jitterDuration(tumblrReconciliationInterval, tumblrReconciliationInterval/5))
		case <-deletionReconcile.C:
			if err := tc.reconcileConversationDeletions(ctx); err != nil && ctx.Err() == nil {
				if log := tc.log(); log != nil {
					log.Warn().Err(err).Msg("Tumblr conversation deletion reconciliation failed")
				}
			}
			clear(observedHeads)
			deletionReconcile.Reset(jitterDuration(
				tumblrDeletionReconciliationInterval,
				tumblrDeletionReconciliationInterval/5,
			))
		}
	}
}

func tumblrConversationHeadProbeDelay(now time.Time, latestModifiedTS int64) time.Duration {
	if latestModifiedTS <= 0 {
		return tumblrConversationHeadProbeDefault
	}
	age := now.Sub(time.Unix(latestModifiedTS, 0))
	switch {
	case age < 30*time.Second:
		return 2 * time.Second
	case age < time.Minute:
		return 5 * time.Second
	case age < 2*time.Minute:
		return 10 * time.Second
	case age < 5*time.Minute:
		return 30 * time.Second
	default:
		return time.Minute
	}
}

func (tc *TumblrClient) probeChangedConversationHeads(ctx context.Context, observedHeads map[string]string) (int64, bool, error) {
	if err := tc.requireLoggedIn(); err != nil {
		return 0, false, err
	}
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return 0, false, err
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return 0, false, err
	}
	// The official client polls one server-sized first page without a limit.
	resp, err := client.ListConversations(ctx, meta.SelectedBlogUUID, 0)
	if err != nil {
		return 0, false, tc.handleRemoteError(err)
	}

	latestModifiedTS := int64(0)
	queued := false
	nextObservedHeads := make(map[string]string, len(resp.Conversations))
	for _, conversation := range resp.Conversations {
		latestModifiedTS = max(latestModifiedTS, conversation.LastModifiedTimestamp)
		if !validRemoteID(conversation.ID) {
			continue
		}
		headMessageID := conversationPageHeadMessageID(conversation.Messages.Data)
		if !validRemoteID(headMessageID) {
			continue
		}
		if observedHeads[conversation.ID] == headMessageID {
			nextObservedHeads[conversation.ID] = headMessageID
			continue
		}
		changed, queueErr := tc.queueChangedConversationHead(ctx, conversation.ID, headMessageID)
		if queueErr != nil {
			return latestModifiedTS, queued, queueErr
		}
		nextObservedHeads[conversation.ID] = headMessageID
		if changed {
			queued = true
		}
	}
	if queued {
		tc.wakeInboundSync()
	}
	clear(observedHeads)
	for conversationID, headMessageID := range nextObservedHeads {
		observedHeads[conversationID] = headMessageID
	}
	return latestModifiedTS, queued, nil
}

// queueChangedConversationHead checks the completed boundary and any existing
// durable work under the same submission fence used by the sync worker. This
// keeps the fast probe from superseding an in-flight job on every poll.
func (tc *TumblrClient) queueChangedConversationHead(ctx context.Context, conversationID, headMessageID string) (bool, error) {
	lockCtx, releaseSubmission, err := tc.acquireOutboundSubmissionLock(ctx)
	if err != nil {
		return false, err
	}
	defer releaseSubmission()

	state, err := tc.connector.DB.ConversationSync.Get(lockCtx, tc.userLogin.ID, conversationID)
	if err != nil {
		return false, err
	}
	job, err := tc.connector.DB.Jobs.Get(lockCtx, tc.userLogin.ID, conversationID)
	if err != nil {
		return false, err
	}
	if job != nil && job.DeleteRoomID != "" {
		return true, tc.connector.DB.Jobs.PutLiveConversation(lockCtx, tc.userLogin.ID, conversationID)
	}
	if job != nil {
		return false, nil
	}
	if state != nil && state.CompletedHeadMessageID == headMessageID {
		return false, nil
	}
	return true, tc.connector.DB.Jobs.PutLiveConversation(lockCtx, tc.userLogin.ID, conversationID)
}

func (tc *TumblrClient) processDueConversationJobs(ctx context.Context, limit int) error {
	if tc == nil || tc.connector == nil || tc.connector.DB == nil || tc.userLogin == nil {
		return fmt.Errorf("tumblr durable sync database is unavailable")
	}
	if !tc.inboundProcessLock.TryLock() {
		// The job is already durable. The active owner will pick it up on this
		// pass, so a canceled generation never waits behind unrelated remote I/O.
		return nil
	}
	defer tc.inboundProcessLock.Unlock()

	processed := 0
	for ctx.Err() == nil && (limit <= 0 || processed < limit) {
		job, err := tc.connector.DB.Jobs.GetNextDue(ctx, tc.userLogin.ID, time.Now())
		if err != nil {
			return err
		}
		if job == nil {
			return nil
		}
		processed++
		if err = tc.processSelectedConversationJob(ctx, job); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (tc *TumblrClient) processSelectedConversationJob(
	ctx context.Context,
	selected *tumblrdb.ConversationJob,
) error {
	if selected == nil {
		return nil
	}
	lockCtx, releaseSubmission, err := tc.acquireOutboundSubmissionLock(ctx)
	if err != nil {
		return err
	}
	defer releaseSubmission()

	// A second process may have selected the same row before this process won
	// the submission fence. Only the exact still-due revision may be handled or
	// acknowledged by this owner.
	job, err := tc.connector.DB.Jobs.Get(lockCtx, tc.userLogin.ID, selected.ConversationID)
	if err != nil {
		return err
	}
	if job == nil || job.Revision != selected.Revision || job.NextAttemptAt.After(time.Now()) {
		tc.wakeInboundSync()
		return nil
	}

	if job.DeleteRoomID != "" {
		err = tc.resumeRemoteConversationDeleteWithSubmissionLock(lockCtx, job.ConversationID, job.Revision)
	} else {
		err = tc.syncConversationByIDWithSubmissionLock(lockCtx, job.ConversationID, job.Revision)
	}
	if errors.Is(err, errTumblrConversationJobSuperseded) {
		tc.wakeInboundSync()
		return nil
	}
	if err != nil {
		nextAttempt := time.Now().Add(tumblrConversationJobBackoff(job.AttemptCount + 1))
		changed, markErr := tc.connector.DB.Jobs.MarkRetry(
			lockCtx,
			tc.userLogin.ID,
			job.ConversationID,
			job.Revision,
			nextAttempt,
			classifyConversationJobError(err),
		)
		if markErr != nil {
			return errors.Join(err, markErr)
		}
		if log := tc.log(); log != nil {
			log.Warn().Err(err).
				Str("conversation_id_hash", logIdentifierHash(job.ConversationID)).
				Int("attempt", job.AttemptCount+1).
				Dur("retry_after", time.Until(nextAttempt)).
				Bool("job_superseded", !changed).
				Msg("Tumblr conversation sync will retry")
		}
		if tumblr.IsAuthError(err) {
			return err
		}
		return nil
	}
	changed, err := tc.connector.DB.Jobs.Delete(
		lockCtx,
		tc.userLogin.ID,
		job.ConversationID,
		job.Revision,
	)
	if err != nil {
		return err
	}
	if !changed {
		tc.wakeInboundSync()
	}
	return nil
}

func (tc *TumblrClient) reconcileConversationHead(ctx context.Context) error {
	if err := tc.requireLoggedIn(); err != nil {
		return err
	}
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return err
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return err
	}
	state, err := tc.connector.DB.SyncState.Get(ctx, tc.userLogin.ID)
	if err != nil {
		return err
	}
	previousWatermark := int64(0)
	if state != nil {
		previousWatermark = state.LastRemoteWatermark
	}
	limit := tc.connector.Config.ConversationSyncBatchLimit()
	before := ""
	seenCursors := make(map[string]struct{})
	newWatermark := previousWatermark
	for page := 0; page < maxConversationListPages; page++ {
		resp, fetchErr := client.ListConversationsBefore(ctx, meta.SelectedBlogUUID, limit, before)
		if fetchErr != nil {
			return tc.handleRemoteError(fetchErr)
		}
		pageStrictlyOlderThanWatermark := previousWatermark > 0 && len(resp.Conversations) > 0
		for _, conversation := range resp.Conversations {
			if conversation.LastModifiedTimestamp <= 0 || conversation.LastModifiedTimestamp >= previousWatermark {
				pageStrictlyOlderThanWatermark = false
			}
			if !validRemoteID(conversation.ID) {
				continue
			}
			if conversation.LastModifiedTimestamp > newWatermark {
				newWatermark = conversation.LastModifiedTimestamp
			}
			if err = tc.putLiveConversationJob(ctx, conversation.ID); err != nil {
				return err
			}
		}
		nextBefore, cursorErr := resp.NextBefore()
		if cursorErr != nil {
			return cursorErr
		}
		nextBefore = strings.TrimSpace(nextBefore)
		if nextBefore == "" || pageStrictlyOlderThanWatermark {
			if err = tc.connector.DB.SyncState.SetScanSuccess(ctx, tc.userLogin.ID, newWatermark, time.Now()); err != nil {
				return err
			}
			tc.wakeInboundSync()
			return nil
		}
		if _, duplicate := seenCursors[nextBefore]; duplicate {
			return fmt.Errorf("tumblr conversation reconciliation cursor repeated")
		}
		seenCursors[nextBefore] = struct{}{}
		before = nextBefore
	}
	return fmt.Errorf("tumblr conversation reconciliation exceeded %d pages", maxConversationListPages)
}

func (tc *TumblrClient) reconcileConversationDeletions(ctx context.Context) error {
	if err := tc.requireLoggedIn(); err != nil {
		return err
	}
	if tc == nil || tc.connector == nil || tc.connector.Bridge == nil || tc.connector.Bridge.DB == nil ||
		tc.connector.Bridge.DB.UserPortal == nil || tc.connector.DB == nil || tc.userLogin == nil || tc.userLogin.UserLogin == nil {
		return fmt.Errorf("tumblr deletion reconciliation is unavailable")
	}
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return err
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return err
	}

	limit := tc.connector.Config.ConversationSyncBatchLimit()
	before := ""
	seenCursors := make(map[string]struct{})
	remoteConversationIDs := make(map[string]struct{})
	for page := 0; page < maxConversationListPages; page++ {
		resp, fetchErr := client.ListConversationsBefore(ctx, meta.SelectedBlogUUID, limit, before)
		if fetchErr != nil {
			return tc.handleRemoteError(fetchErr)
		}
		if resp == nil {
			return fmt.Errorf("tumblr conversation list response is missing")
		}
		for _, conversation := range resp.Conversations {
			if validRemoteID(conversation.ID) {
				remoteConversationIDs[conversation.ID] = struct{}{}
			}
		}
		nextBefore, cursorErr := resp.NextBefore()
		if cursorErr != nil {
			return cursorErr
		}
		nextBefore = strings.TrimSpace(nextBefore)
		if nextBefore == "" {
			return tc.enqueueMissingConversationChecks(ctx, remoteConversationIDs)
		}
		if _, duplicate := seenCursors[nextBefore]; duplicate {
			return fmt.Errorf("tumblr conversation deletion reconciliation cursor repeated")
		}
		seenCursors[nextBefore] = struct{}{}
		before = nextBefore
	}
	return fmt.Errorf("tumblr conversation deletion reconciliation exceeded %d pages", maxConversationListPages)
}

func (tc *TumblrClient) enqueueMissingConversationChecks(ctx context.Context, remoteConversationIDs map[string]struct{}) error {
	userPortals, err := tc.connector.Bridge.DB.UserPortal.GetAllForLogin(ctx, tc.userLogin.UserLogin)
	if err != nil {
		return fmt.Errorf("failed to load Tumblr portals for deletion reconciliation: %w", err)
	}
	queued := 0
	for _, userPortal := range userPortals {
		if userPortal == nil {
			continue
		}
		conversationID := tumblrid.ParsePortalID(userPortal.Portal.ID)
		if !validRemoteID(conversationID) || isPendingDMPortalID(conversationID) {
			continue
		}
		if userPortal.Portal != tc.portalKey(conversationID) {
			continue
		}
		if _, present := remoteConversationIDs[conversationID]; present {
			continue
		}
		// List absence only selects a candidate. The durable job must confirm a
		// direct 404 before it can remove the local room.
		if err = tc.connector.DB.Jobs.Put(ctx, tc.userLogin.ID, conversationID); err != nil {
			return fmt.Errorf("failed to persist Tumblr deletion verification job: %w", err)
		}
		queued++
	}
	if queued > 0 {
		tc.wakeInboundSync()
		if log := tc.log(); log != nil {
			log.Info().Int("conversation_count", queued).
				Msg("Queued Tumblr conversations missing from the complete list for direct verification")
		}
	}
	return nil
}

func tumblrConversationJobBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	backoff := tumblrJobRetryBase
	for i := 1; i < attempt && backoff < tumblrJobRetryMax; i++ {
		backoff *= 2
		if backoff > tumblrJobRetryMax {
			backoff = tumblrJobRetryMax
		}
	}
	return jitterDuration(backoff, backoff/2)
}

func jitterDuration(base, spread time.Duration) time.Duration {
	if spread <= 0 {
		return base
	}
	return base - spread + time.Duration(rand.Int64N(int64(spread*2)+1))
}

func classifyConversationJobError(err error) tumblrdb.JobErrorCode {
	if err == nil {
		return ""
	}
	var deferred *outboundReconciliationDeferredError
	if errors.As(err, &deferred) {
		return tumblrdb.JobErrorQueue
	}
	if tumblr.IsAuthError(err) {
		return tumblrdb.JobErrorAuth
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return tumblrdb.JobErrorNetwork
	}
	var remoteErr *tumblr.Error
	if errors.As(err, &remoteErr) {
		switch {
		case remoteErr.StatusCode == http.StatusTooManyRequests:
			return tumblrdb.JobErrorRateLimited
		case remoteErr.StatusCode >= http.StatusInternalServerError:
			return tumblrdb.JobErrorRemote
		default:
			return tumblrdb.JobErrorInvalidResponse
		}
	}
	return tumblrdb.JobErrorUnknown
}
