package connector

import (
	"context"
	"sync"
	"sync/atomic"

	"maunium.net/go/mautrix/bridgev2/status"
)

// connectionGeneration owns every goroutine started for one Connect call.
// Replacing or disconnecting a generation cancels one context and waits for
// all session, push, and inbound workers without holding an ownership lock.
type connectionGeneration struct {
	id     uint64
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	inboundOnce   sync.Once
	retiringClean atomic.Bool
}

func (tc *TumblrClient) isActiveClient() bool {
	if tc == nil {
		return false
	}
	tc.ownershipLock.Lock()
	active := !tc.retired
	tc.ownershipLock.Unlock()
	return active
}

func (tc *TumblrClient) beginOwnedOperation() bool {
	if tc == nil {
		return false
	}
	tc.ownershipLock.Lock()
	defer tc.ownershipLock.Unlock()
	if tc.retired {
		return false
	}
	tc.ownershipWG.Add(1)
	return true
}

func (tc *TumblrClient) endOwnedOperation() {
	tc.ownershipWG.Done()
}

// retireForReplacement rejects new old-client work and waits for any owned
// operation already in flight. Once it returns, the client can no longer save
// login metadata, publish bridge state, or start new work.
func (tc *TumblrClient) retireForReplacement() {
	if tc == nil {
		return
	}
	tc.ownershipLock.Lock()
	tc.retired = true
	tc.ownershipLock.Unlock()
	tc.ownershipWG.Wait()
	tc.setLoggedIn(false)
}

func (tc *TumblrClient) reactivateAfterReplacementFailure() {
	if tc == nil {
		return
	}
	tc.ownershipLock.Lock()
	tc.retired = false
	tc.ownershipLock.Unlock()
}

func (tc *TumblrClient) replaceConnectionGeneration() *connectionGeneration {
	baseCtx := context.Background()
	if tc.userLogin != nil && tc.userLogin.Bridge != nil && tc.userLogin.Bridge.BackgroundCtx != nil {
		baseCtx = tc.userLogin.Bridge.BackgroundCtx
	}
	ctx, cancel := context.WithCancel(baseCtx)
	previous, generationID, changed := tc.beginGenerationChange()

	generation := &connectionGeneration{
		id:     generationID,
		ctx:    ctx,
		cancel: cancel,
	}
	// The orchestration worker is reserved before publishing the generation so
	// Disconnect can never observe a zero counter and return before it starts.
	generation.wg.Add(1)
	stopConnectionGeneration(previous)

	tc.generationLock.Lock()
	tc.generation = generation
	tc.generationChanging = false
	close(changed)
	tc.generationLock.Unlock()
	return generation
}

func (tc *TumblrClient) stopConnectionGeneration() {
	if tc == nil {
		return
	}
	generation, _, changed := tc.beginGenerationChange()
	stopConnectionGeneration(generation)
	tc.generationLock.Lock()
	tc.generationChanging = false
	close(changed)
	tc.generationLock.Unlock()
}

func (tc *TumblrClient) beginGenerationChange() (*connectionGeneration, uint64, chan struct{}) {
	for {
		tc.generationLock.Lock()
		if tc.generationChanging {
			changed := tc.generationChanged
			tc.generationLock.Unlock()
			<-changed
			continue
		}
		tc.generationChanging = true
		tc.generationChanged = make(chan struct{})
		tc.generationID++
		generationID := tc.generationID
		generation := tc.generation
		tc.generation = nil
		changed := tc.generationChanged
		tc.generationLock.Unlock()
		return generation, generationID, changed
	}
}

func stopConnectionGeneration(generation *connectionGeneration) {
	if generation == nil {
		return
	}
	generation.cancel()
	generation.wg.Wait()
}

func (tc *TumblrClient) isCurrentGeneration(generation *connectionGeneration) bool {
	if tc == nil || generation == nil || !tc.isActiveClient() {
		return false
	}
	tc.generationLock.Lock()
	current := tc.generation == generation
	tc.generationLock.Unlock()
	return current && generation.ctx.Err() == nil
}

func (tc *TumblrClient) failGeneration(generation *connectionGeneration, state status.BridgeState) {
	if generation == nil {
		return
	}
	tc.generationLock.Lock()
	isCurrent := tc.generation == generation && generation.ctx.Err() == nil
	tc.generationLock.Unlock()
	if !isCurrent {
		return
	}
	tc.setLoggedIn(false)
	generation.cancel()
	tc.sendBridgeState(state)
}

func (tc *TumblrClient) sendGenerationState(generation *connectionGeneration, state status.BridgeState) {
	if tc.isCurrentGeneration(generation) {
		tc.sendBridgeState(state)
	}
}
