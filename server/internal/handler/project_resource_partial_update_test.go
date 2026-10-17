package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// Pin only the resource read to a known backend. Other queries still use the
// pool, as they do in production, including queries outside the transaction.
type resourceReadPauseDB struct {
	db.DBTX
	resourceDB      db.DBTX
	paused, release chan struct{}
}

func (d resourceReadPauseDB) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	if !strings.HasPrefix(query, "-- name: GetProjectResourceInWorkspace") {
		return d.DBTX.QueryRow(ctx, query, args...)
	}
	row := d.resourceDB.QueryRow(ctx, query, args...)
	if d.paused != nil {
		return &contextLockPauseRow{Row: row, paused: d.paused, release: d.release}
	}
	return row
}

type resourceReadPauseStarter struct {
	conn            *pgxpool.Conn
	paused, release chan struct{}
}

func (s resourceReadPauseStarter) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &resourceReadPauseTx{Tx: tx, paused: s.paused, release: s.release}, nil
}

type resourceReadPauseTx struct {
	pgx.Tx
	paused, release chan struct{}
}

func (tx *resourceReadPauseTx) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	return (resourceReadPauseDB{
		DBTX: tx.Tx, resourceDB: tx.Tx, paused: tx.paused, release: tx.release,
	}).QueryRow(ctx, query, args...)
}

// Wait for evidence that B has progressed while A is paused. With an unlocked
// read, B finishes first, forcing A to write its stale snapshot last. With a
// locked read, PostgreSQL must identify B as waiting on A before A is released.
// Merely observing B before its SELECT would still allow it to read after A.
func waitForResourceUpdateCompetitor(t *testing.T, ctx context.Context, holderPID, waiterPID uint32, done <-chan struct{}) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			t.Log("ref update completed while label update was paused")
			return
		default:
		}
		var blocked bool
		if err := testPool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE datname = current_database() AND pid = $2
				  AND state = 'active' AND wait_event_type = 'Lock'
				  AND wait_event IN ('transactionid', 'tuple')
				  AND $1::int = ANY(pg_blocking_pids(pid))
			)
		`, int(holderPID), int(waiterPID)).Scan(&blocked); err != nil {
			t.Fatalf("observe competing resource update: %v", err)
		}
		if blocked {
			t.Log("ref update is blocked by label update's PostgreSQL backend")
			return
		}
		select {
		case <-done:
			t.Log("ref update completed while label update was paused")
			return
		case <-ctx.Done():
			t.Fatalf("ref update neither completed nor blocked on label update: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

// A label-only request must preserve a racing ref update, including the
// local_directory execution mode and its legacy label mirror (#9238).
func TestUpdateProjectResourceConcurrentPartialUpdate(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Fatal("database-backed handler test fixture is required")
	}
	for _, resourceType := range []string{"github_repo", "local_directory"} {
		t.Run(resourceType, func(t *testing.T) {
			projectID := dbfx.Project(t, "racing project")
			initialRef := map[string]any{"url": "https://github.com/example/repo", "ref": "main"}
			nextRef := map[string]any{"url": "https://github.com/example/repo", "ref": "release"}
			wantRef := map[string]string{"url": "https://github.com/example/repo", "ref": "release"}
			if resourceType == "local_directory" {
				daemonID := "resource-update-" + projectID
				metadata, err := json.Marshal(map[string]any{
					"capabilities": []string{protocol.DaemonCapabilityLocalWorktreeV1},
				})
				if err != nil {
					t.Fatal(err)
				}
				dbfx.Runtime(t, "resource update worktree runtime", testutil.Cols{
					"daemon_id": daemonID, "runtime_mode": "local", "metadata": string(metadata),
				})
				initialRef = map[string]any{
					"local_path": "/tmp/resource-update-project", "daemon_id": daemonID,
					"execution_mode": "in_place", "label": "Old label",
				}
				// A mode dialog may carry the embedded label it originally read.
				nextRef = map[string]any{
					"local_path": "/tmp/resource-update-project", "daemon_id": daemonID,
					"execution_mode": "worktree", "label": "Old label",
				}
				wantRef = map[string]string{
					"local_path": "/tmp/resource-update-project", "daemon_id": daemonID,
					"execution_mode": "worktree", "label": "New label",
				}
			}
			refJSON, err := json.Marshal(initialRef)
			if err != nil {
				t.Fatal(err)
			}
			resourceID := dbfx.Insert(t, "project_resource", testutil.Cols{
				"project_id": projectID, "workspace_id": testWorkspaceID,
				"resource_type": resourceType, "resource_ref": string(refJSON),
				"label": "Old label", "position": 1,
			})

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			startedA, startedB := false, false
			connA, err := testPool.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if !startedA {
					connA.Release()
				}
			}()
			connB, err := testPool.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if !startedB {
					connB.Release()
				}
			}()
			pidA, pidB := connA.Conn().PgConn().PID(), connB.Conn().PgConn().PID()
			paused, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			resume := func() { releaseOnce.Do(func() { close(release) }) }
			hA, hB := *testHandler, *testHandler
			hA.Queries = db.New(resourceReadPauseDB{
				DBTX: testPool, resourceDB: connA, paused: paused, release: release,
			})
			hA.TxStarter = resourceReadPauseStarter{conn: connA, paused: paused, release: release}
			hB.Queries = db.New(resourceReadPauseDB{DBTX: testPool, resourceDB: connB})
			hB.TxStarter = connB

			path := "/api/projects/" + projectID + "/resources/" + resourceID
			reqA := withURLParams(newRequest("PUT", path, map[string]any{"label": "New label"}).WithContext(ctx),
				"id", projectID, "resourceId", resourceID)
			reqB := withURLParams(newRequest("PUT", path, map[string]any{"resource_ref": nextRef}).WithContext(ctx),
				"id", projectID, "resourceId", resourceID)
			doneA, doneB := make(chan struct{}), make(chan struct{})
			var responseA, responseB *testutil.Response
			defer func() {
				resume()
				cancel()
				for _, done := range []<-chan struct{}{doneA, doneB} {
					if done == doneB && !startedB {
						continue
					}
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("resource update did not stop after cancellation")
					}
				}
			}()
			startedA = true
			go func() {
				defer close(doneA)
				defer connA.Release()
				responseA = testutil.Call(t, hA.UpdateProjectResource, reqA)
			}()
			waitContextLockSignal(t, paused, "label update did not read resource")
			startedB = true
			go func() {
				defer close(doneB)
				defer connB.Release()
				responseB = testutil.Call(t, hB.UpdateProjectResource, reqB)
			}()
			waitForResourceUpdateCompetitor(t, ctx, pidA, pidB, doneB)
			resume()
			for _, done := range []<-chan struct{}{doneA, doneB} {
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatalf("resource updates did not complete: %v", ctx.Err())
				}
			}
			responseA.Want(http.StatusOK)
			responseB.Want(http.StatusOK)

			stored, err := testHandler.Queries.GetProjectResourceInWorkspace(ctx, db.GetProjectResourceInWorkspaceParams{
				ID: parseUUID(resourceID), WorkspaceID: parseUUID(testWorkspaceID),
			})
			if err != nil {
				t.Fatal(err)
			}
			var ref map[string]string
			if err := json.Unmarshal(stored.ResourceRef, &ref); err != nil {
				t.Fatalf("stored ref %s: %v", stored.ResourceRef, err)
			}
			for key, want := range wantRef {
				if ref[key] != want {
					t.Errorf("stored resource_ref.%s = %q, want %q", key, ref[key], want)
				}
			}
			if !stored.Label.Valid || stored.Label.String != "New label" {
				t.Errorf("stored label = %v, want New label", stored.Label)
			}
			if stored.Position != 1 {
				t.Errorf("stored position = %d, want 1", stored.Position)
			}
		})
	}
}
