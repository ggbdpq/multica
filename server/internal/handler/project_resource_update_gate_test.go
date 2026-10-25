package handler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestProjectResourceUpdateGateIgnoresUnrelatedWaiters(t *testing.T) {
	if testPool == nil || dbfx == nil {
		t.Fatal("database-backed handler test fixture is required")
	}
	projectID := dbfx.Project(t, "unrelated resource gate regression")
	resourceID := dbfx.Insert(t, "project_resource", testutil.Cols{
		"project_id": projectID, "workspace_id": testWorkspaceID,
		"resource_type": "github_repo",
		"resource_ref":  `{"url":"https://github.com/example/repo","ref":"main"}`,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rollback := func(tx pgx.Tx) {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		if err := tx.Rollback(cleanupCtx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			t.Errorf("rollback gate regression transaction: %v", err)
		}
	}

	// Keep this backend reserved: it represents B before any resource read.
	idle, err := testPool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Release()
	idlePID := idle.Conn().PgConn().PID()
	holder, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(holder)
	params := db.GetProjectResourceInWorkspaceForUpdateParams{
		ID: parseUUID(resourceID), WorkspaceID: parseUUID(testWorkspaceID),
	}
	if _, err := db.New(holder).GetProjectResourceInWorkspaceForUpdate(ctx, params); err != nil {
		t.Fatal(err)
	}
	holderPID := holder.Conn().PgConn().PID()
	waiter, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waiterPID := waiter.Conn().PgConn().PID()
	waiterDone := make(chan struct{})
	var waiterErr error
	go func() {
		defer close(waiterDone)
		defer rollback(waiter)
		_, waiterErr = db.New(waiter).GetProjectResourceInWorkspaceForUpdate(ctx, params)
	}()
	defer func() {
		rollback(holder)
		select {
		case <-waiterDone:
			return
		case <-time.After(5 * time.Second):
			cancel()
		}
		select {
		case <-waiterDone:
		case <-time.After(5 * time.Second):
			t.Error("unrelated waiter did not stop after cancellation")
		}
	}()

	// Establish the decoy independently of the gate being tested.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := testPool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity WHERE pid = $1::int
			AND wait_event_type = 'Lock'
			AND query LIKE '%GetProjectResourceInWorkspaceForUpdate%'
			AND $2::int = ANY(pg_blocking_pids(pid))
		)`, int(waiterPID), int(holderPID)).Scan(&blocked); err != nil {
			t.Fatalf("observe unrelated waiter: %v", err)
		}
		if blocked {
			break
		}
		select {
		case <-waiterDone:
			t.Fatalf("unrelated waiter completed before blocking: %v", waiterErr)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}

	open := make(chan struct{})
	closed := make(chan struct{})
	close(closed)
	for _, tc := range []struct {
		name           string
		holder, waiter uint32
		done           <-chan struct{}
		want           bool
	}{
		{"unrelated_waiter", holderPID, idlePID, open, false},
		{"wrong_blocker", idlePID, waiterPID, open, false},
		{"owned_wait", holderPID, waiterPID, open, true},
		{"completed", holderPID, idlePID, closed, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ready, err := refRequestGated(ctx, tc.holder, tc.waiter, tc.done)
			if err != nil {
				t.Fatal(err)
			}
			if ready != tc.want {
				t.Errorf("gate ready = %v, want %v for holder %d / waiter %d", ready, tc.want, tc.holder, tc.waiter)
			}
		})
	}
}
