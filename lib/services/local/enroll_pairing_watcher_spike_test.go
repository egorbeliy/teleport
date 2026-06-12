// Teleport
// Copyright (C) 2026 Gravitational, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// This file is a throwaway spike. It sketches how the public Device Trust
// RPC will consume EnrollPairing events via the standard event stream
// (RFD 153) — no custom watcher needed. Delete when the real public-RPC
// issue lands.

package local_test

import (
	"context"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	devicepb "github.com/gravitational/teleport/api/gen/proto/go/teleport/devicetrust/v1"
	"github.com/gravitational/teleport/api/types"
	"github.com/gravitational/teleport/lib/backend"
	"github.com/gravitational/teleport/lib/backend/memory"
	"github.com/gravitational/teleport/lib/services"
	"github.com/gravitational/teleport/lib/services/local"
)

// TestEnrollPairingEventStream walks an EnrollPairing through its state
// machine while a goroutine modeling the public Device Trust RPC handler
// blocks on the standard event stream waiting for APPROVED.
func TestEnrollPairingEventStream(t *testing.T) {
	t.Parallel()
	const user = "alice"

	bk, err := memory.New(memory.Config{
		Context: t.Context(),
		Clock:   clockwork.NewFakeClock(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bk.Close() })

	service := local.NewEnrollPairingService(bk)
	events := local.NewEventsService(bk)

	// Authenticated side: Web UI opens the wizard, creates the pairing.
	created, err := service.CreateEnrollPairing(t.Context(), user)
	require.NoError(t, err)

	type result struct {
		pairing *devicepb.EnrollPairing
		err     error
	}
	approved := make(chan result, 1)
	ready := make(chan struct{})

	// Public RPC handler sketch. After token lookup + CAS to AWAITING_APPROVAL
	// (omitted here — secondary index lives in a follow-up issue), the
	// handler blocks for APPROVED via the standard event stream and returns
	// the resolved pairing. Token comes from the mobile app's request.
	go func() {
		p, err := awaitApprovedSketch(t.Context(), events, created.GetStatus().GetToken(), ready)
		approved <- result{p, err}
	}()

	// Wait for the watcher to be subscribed so the puts below don't race
	// the OpInit handshake.
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("watcher did not initialize in time")
	}

	// Drive the state machine. In production, AWAITING_APPROVAL is written
	// by the public RPC's CAS and APPROVED by ApproveEnrollPairing on the
	// authenticated side. The spike just bypasses the RPCs and writes the
	// states directly.
	awaitingApproval := proto.Clone(created).(*devicepb.EnrollPairing)
	awaitingApproval.GetStatus().SetState(devicepb.EnrollPairingState_ENROLL_PAIRING_STATE_AWAITING_APPROVAL)
	require.NoError(t, putForTest(t.Context(), bk, awaitingApproval))

	approvedPairing := proto.Clone(awaitingApproval).(*devicepb.EnrollPairing)
	approvedPairing.GetStatus().SetState(devicepb.EnrollPairingState_ENROLL_PAIRING_STATE_APPROVED)
	require.NoError(t, putForTest(t.Context(), bk, approvedPairing))

	select {
	case r := <-approved:
		require.NoError(t, r.err)
		require.Equal(t, user, r.pairing.GetMetadata().GetName())
		require.Equal(t,
			devicepb.EnrollPairingState_ENROLL_PAIRING_STATE_APPROVED,
			r.pairing.GetStatus().GetState())
	case <-time.After(2 * time.Second):
		t.Fatal("public RPC sketch did not observe APPROVED in time")
	}
}

// awaitApprovedSketch is what the public Device Trust RPC handler will look
// like in production. The mobile app's request supplies the pairing token;
// the handler looks the pairing up by sha256(token) (out of scope here),
// CASes the state to AWAITING_APPROVAL, then opens an event watcher scoped
// to this specific token and returns when the pairing transitions to
// APPROVED. Filtering by token (not metadata.name) guards against the
// pairing TTL-expiring and being recreated for the same user during the
// blocking wait — events for the fresh pairing have a different token and
// stay dropped at the parser.
func awaitApprovedSketch(ctx context.Context, events *local.EventsService, token string, ready chan<- struct{}) (*devicepb.EnrollPairing, error) {
	filter := types.EnrollPairingFilter{Token: token}
	w, err := events.NewWatcher(ctx, types.Watch{
		Kinds: []types.WatchKind{{
			Kind:   types.KindEnrollPairing,
			Filter: filter.IntoMap(),
		}},
	})
	if err != nil {
		return nil, err
	}
	defer w.Close()

	for {
		select {
		case event := <-w.Events():
			if event.Type == types.OpInit {
				close(ready)
				continue
			}
			if event.Type != types.OpPut {
				continue
			}
			pairing, err := types.ConvertResource[*devicepb.EnrollPairing](event.Resource)
			if err != nil {
				return nil, err
			}
			if pairing.GetStatus().GetState() == devicepb.EnrollPairingState_ENROLL_PAIRING_STATE_APPROVED {
				return pairing, nil
			}
		case <-w.Done():
			return nil, context.Canceled
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// TestEnrollPairingFilterPushdown asserts that the parser drops events for
// pairings whose token doesn't match the filter. Covers two scenarios:
//
//   - Different-user noise: a concurrent enrollment by another user must not
//     reach this RPC's watcher.
//   - Same-user replacement: the pairing the RPC is waiting on TTL-expires
//     and a fresh pairing replaces it for the same user. The replacement has
//     a new token, so its events must stay dropped — otherwise the RPC
//     resolves its wait on the wrong pairing. This is the case that
//     metadata.name-based filtering would get wrong.
func TestEnrollPairingFilterPushdown(t *testing.T) {
	t.Parallel()
	const watched = "alice"
	const other = "bob"

	bk, err := memory.New(memory.Config{
		Context: t.Context(),
		Clock:   clockwork.NewFakeClock(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bk.Close() })

	events := local.NewEventsService(bk)
	service := local.NewEnrollPairingService(bk)

	// The pairing the public RPC is acting on. The handler looks it up by
	// token first (out of scope), so the pre-subscribe put isn't missed.
	pinned, err := service.CreateEnrollPairing(t.Context(), watched)
	require.NoError(t, err)

	filter := types.EnrollPairingFilter{Token: pinned.GetStatus().GetToken()}
	w, err := events.NewWatcher(t.Context(), types.Watch{
		Kinds: []types.WatchKind{{
			Kind:   types.KindEnrollPairing,
			Filter: filter.IntoMap(),
		}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	select {
	case event := <-w.Events():
		require.Equal(t, types.OpInit, event.Type)
	case <-time.After(time.Second):
		t.Fatal("did not see OpInit")
	}

	// (1) Different-user noise.
	_, err = service.CreateEnrollPairing(t.Context(), other)
	require.NoError(t, err)

	// (2) Same-user replacement: delete the pinned pairing and create a
	// fresh one for the same user. The fresh pairing's token differs, so
	// its put must be dropped at the parser. Delete via raw backend.Delete
	// since the service doesn't expose one (it lives with the reaper issue).
	require.NoError(t, bk.Delete(t.Context(), backend.NewKey("devices", "enroll_pairing", watched)))
	replacement, err := service.CreateEnrollPairing(t.Context(), watched)
	require.NoError(t, err)
	require.NotEqual(t, pinned.GetStatus().GetToken(), replacement.GetStatus().GetToken(),
		"sanity: a recreated pairing must have a fresh token")

	// (3) Drive a put for the pinned token, modeling the public RPC's CAS
	// to AWAITING_APPROVAL. Use a shadow key because the canonical key
	// already holds the replacement; what we're testing is parser routing
	// by token, not the storage layout.
	awaiting := proto.Clone(pinned).(*devicepb.EnrollPairing)
	awaiting.GetStatus().SetState(devicepb.EnrollPairingState_ENROLL_PAIRING_STATE_AWAITING_APPROVAL)
	value, err := services.MarshalEnrollPairing(awaiting)
	require.NoError(t, err)
	_, err = bk.Put(t.Context(), backend.Item{
		Key:     backend.NewKey("devices", "enroll_pairing", "alice-shadow"),
		Value:   value,
		Expires: awaiting.GetMetadata().GetExpires().AsTime(),
	})
	require.NoError(t, err)

	// Drain any OpDelete events (parser can't apply token filter without a
	// value, so they pass through) until the pinned-token put arrives.
	deadline := time.After(time.Second)
WAIT_PUT:
	for {
		select {
		case event := <-w.Events():
			if event.Type == types.OpDelete {
				continue
			}
			require.Equal(t, types.OpPut, event.Type)
			got, err := types.ConvertResource[*devicepb.EnrollPairing](event.Resource)
			require.NoError(t, err)
			require.Equal(t, pinned.GetStatus().GetToken(), got.GetStatus().GetToken())
			require.Equal(t,
				devicepb.EnrollPairingState_ENROLL_PAIRING_STATE_AWAITING_APPROVAL,
				got.GetStatus().GetState())
			break WAIT_PUT
		case <-deadline:
			t.Fatal("did not see the pinned-token put event")
		}
	}

	// No further put events: bob's pairing and the replacement were both
	// dropped at the parser. Delete events for the original pinned pairing
	// pass through (parser can't apply token filter without a value), so
	// drain any that arrive and assert no put-shaped noise.
	stop := time.After(100 * time.Millisecond)
	for {
		select {
		case event := <-w.Events():
			if event.Type == types.OpDelete {
				continue
			}
			t.Fatalf("unexpected event after pinned-token put: %+v", event)
		case <-stop:
			return
		}
	}
}

func putForTest(ctx context.Context, bk backend.Backend, p *devicepb.EnrollPairing) error {
	value, err := services.MarshalEnrollPairing(p)
	if err != nil {
		return err
	}
	expires := p.GetMetadata().GetExpires()
	var exp time.Time
	if expires != nil {
		exp = expires.AsTime()
	} else {
		exp = bk.Clock().Now().Add(5 * time.Minute)
		p.GetMetadata().SetExpires(timestamppb.New(exp))
	}
	_, err = bk.Put(ctx, backend.Item{
		Key:     backend.NewKey("devices", "enroll_pairing", p.GetMetadata().GetName()),
		Value:   value,
		Expires: exp,
	})
	return err
}
