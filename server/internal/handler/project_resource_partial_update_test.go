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

// Reserve the resource-read backend for the entire request, including the
// unlocked implementation: releasing it after Scan would allow PID reuse.
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
	if err != nil || s.paused == nil {
		return tx, err
	}
	return &contextLockTestTx{Tx: tx, starter: &contextLockTestStarter{
		pauseAfter: "GetProjectResourceInWorkspaceForUpdate",
		paused:     s.paused, release: s.release,
	}}, nil
}

type resourceUpdateRequest struct {
	started, done chan struct{}
	pid           uint32
	response      *testutil.Response
	err           error
}

func startResourceUpdate(t *testing.T, ctx context.Context, req *http.Request, paused, release chan struct{}) *resourceUpdateRequest {
	t.Helper()
	result := &resourceUpdateRequest{started: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(result.done)
		result.err = testPool.AcquireFunc(ctx, func(conn *pgxpool.Conn) error {
			result.pid = conn.Conn().PgConn().PID()
			close(result.started)
			h := *testHandler
			h.Queries = db.New(resourceReadPauseDB{
				DBTX: testPool, resourceDB: conn, paused: paused, release: release,
			})
			h.TxStarter = resourceReadPauseStarter{conn: conn, paused: paused, release: release}
			result.response = testutil.Call(t, h.UpdateProjectResource, req)
			return nil
		})
	}()
	return result
}

func resourceUpdatePID(t *testing.T, ctx context.Context, request *resourceUpdateRequest) uint32 {
	t.Helper()
	select {
	case <-request.started:
		return request.pid
	case <-request.done:
		if request.err != nil {
			t.Fatalf("acquire resource update connection: %v", request.err)
		}
		return request.pid
	case <-ctx.Done():
		t.Fatalf("resource update did not acquire connection: %v", ctx.Err())
		return 0
	}
}

// The request connections remain reserved while this is observed. A matching
// SQL name alone could refer to another resource, database, or test run.
func refRequestGated(ctx context.Context, holderPID, waiterPID uint32, done <-chan struct{}) (bool, error) {
	select {
	case <-done:
		return true, nil
	default:
	}
	var blocked bool
	err := testPool.QueryRow(ctx, `SELECT $1::int = ANY(pg_blocking_pids($2::int))`,
		int(holderPID), int(waiterPID)).Scan(&blocked)
	return blocked, err
}

func waitRefRequestGated(t *testing.T, ctx context.Context, holderPID, waiterPID uint32, done <-chan struct{}) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := refRequestGated(ctx, holderPID, waiterPID, done)
		if err != nil {
			t.Fatalf("observe resource update backends %d -> %d: %v", holderPID, waiterPID, err)
		}
		if ready {
			return
		}
		select {
		case <-done:
			return
		case <-ctx.Done():
			t.Fatalf("ref update neither completed nor blocked on label update: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func runConcurrentPartialUpdate(t *testing.T, ctx context.Context, projectID, resourceID string, refBody map[string]any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	paused, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	resume := func() { releaseOnce.Do(func() { close(release) }) }
	var requestA, requestB *resourceUpdateRequest
	defer func() {
		resume()
		cancel()
		for _, request := range []*resourceUpdateRequest{requestA, requestB} {
			if request == nil {
				continue
			}
			select {
			case <-request.done:
			case <-time.After(5 * time.Second):
				t.Error("resource update did not stop after cancellation")
			}
		}
	}()

	path := "/api/projects/" + projectID + "/resources/" + resourceID
	reqA := withURLParams(newRequest("PUT", path, map[string]any{"label": "New label"}).WithContext(ctx),
		"id", projectID, "resourceId", resourceID)
	requestA = startResourceUpdate(t, ctx, reqA, paused, release)
	pidA := resourceUpdatePID(t, ctx, requestA)
	select {
	case <-paused:
	case <-requestA.done:
		t.Fatalf("label update ended before reading resource: error=%v, response=%v", requestA.err, requestA.response)
	case <-ctx.Done():
		t.Fatalf("label update did not read resource: %v", ctx.Err())
	}

	reqB := withURLParams(newRequest("PUT", path, map[string]any{"resource_ref": refBody}).WithContext(ctx),
		"id", projectID, "resourceId", resourceID)
	requestB = startResourceUpdate(t, ctx, reqB, nil, nil)
	pidB := resourceUpdatePID(t, ctx, requestB)
	waitRefRequestGated(t, ctx, pidA, pidB, requestB.done)
	resume()
	for _, request := range []*resourceUpdateRequest{requestA, requestB} {
		select {
		case <-request.done:
		case <-ctx.Done():
			t.Fatalf("resource update did not complete: %v", ctx.Err())
		}
		if request.err != nil {
			t.Fatalf("resource update connection: %v", request.err)
		}
		request.response.Want(http.StatusOK)
	}
}

// storedProjectResource reads the row back after both updates settled.
func storedProjectResource(t *testing.T, ctx context.Context, resourceID string) db.ProjectResource {
	t.Helper()
	stored, err := testHandler.Queries.GetProjectResourceInWorkspace(ctx, db.GetProjectResourceInWorkspaceParams{
		ID: parseUUID(resourceID), WorkspaceID: parseUUID(testWorkspaceID),
	})
	if err != nil {
		t.Fatal(err)
	}
	return stored
}

// Omitted columns must survive a competing ref update (#9238).
func TestUpdateProjectResourceConcurrentPartialUpdate(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Fatal("database-backed handler test fixture is required")
	}

	ctx := context.Background()
	projectID := dbfx.Project(t, "racing project")
	resourceID := dbfx.Insert(t, "project_resource", testutil.Cols{
		"project_id":    projectID,
		"workspace_id":  testWorkspaceID,
		"resource_type": "github_repo",
		"resource_ref":  `{"url":"https://github.com/example/repo","ref":"main"}`,
		"label":         "Old label",
		"position":      1,
	})

	runConcurrentPartialUpdate(t, ctx, projectID, resourceID, map[string]any{
		"url": "https://github.com/example/repo", "ref": "release",
	})

	stored := storedProjectResource(t, ctx, resourceID)
	var ref struct {
		Ref string `json:"ref"`
	}
	if err := json.Unmarshal(stored.ResourceRef, &ref); err != nil {
		t.Fatalf("stored ref %s: %v", stored.ResourceRef, err)
	}
	if ref.Ref != "release" {
		t.Errorf("stored ref = %q, want the concurrent ref update (release)", ref.Ref)
	}
	if !stored.Label.Valid || stored.Label.String != "New label" {
		t.Errorf("stored label = %v, want New label", stored.Label)
	}
	if stored.Position != 1 {
		t.Errorf("stored position = %d, want 1", stored.Position)
	}
}

// The legacy label mirror must not revert a competing execution-mode change.
func TestUpdateProjectResourceConcurrentLabelMirrorVsExecutionMode(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Fatal("database-backed handler test fixture is required")
	}
	for _, tc := range []struct {
		name, initialMode, nextMode string
	}{
		{"worktree_to_in_place", "worktree", ""},
		{"in_place_to_worktree", "in_place", "worktree"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			projectID := dbfx.Project(t, "racing local directory project")
			daemonID := "resource-update-" + projectID
			if tc.nextMode == "worktree" {
				metadata, err := json.Marshal(map[string]any{
					"capabilities": []string{protocol.DaemonCapabilityLocalWorktreeV1},
				})
				if err != nil {
					t.Fatal(err)
				}
				dbfx.Runtime(t, "worktree-capable resource runtime", testutil.Cols{
					"daemon_id": daemonID, "runtime_mode": "local", "metadata": string(metadata),
				})
			}
			initialRef, err := json.Marshal(map[string]any{
				"local_path": "/repo", "daemon_id": daemonID,
				"execution_mode": tc.initialMode, "label": "Old label",
			})
			if err != nil {
				t.Fatal(err)
			}
			resourceID := dbfx.Insert(t, "project_resource", testutil.Cols{
				"project_id": projectID, "workspace_id": testWorkspaceID,
				"resource_type": "local_directory", "resource_ref": string(initialRef),
				"label": "Old label", "position": 1,
			})
			refBody := map[string]any{
				"local_path": "/repo", "daemon_id": daemonID, "label": "Old label",
			}
			if tc.nextMode != "" {
				refBody["execution_mode"] = tc.nextMode
			}
			runConcurrentPartialUpdate(t, ctx, projectID, resourceID, refBody)
			stored := storedProjectResource(t, ctx, resourceID)
			var ref struct {
				ExecutionMode string `json:"execution_mode"`
				Label         string `json:"label"`
				LocalPath     string `json:"local_path"`
				DaemonID      string `json:"daemon_id"`
			}
			if err := json.Unmarshal(stored.ResourceRef, &ref); err != nil {
				t.Fatalf("stored ref %s: %v", stored.ResourceRef, err)
			}
			if ref.ExecutionMode != tc.nextMode {
				t.Errorf("stored execution_mode = %q, want %q", ref.ExecutionMode, tc.nextMode)
			}
			if !stored.Label.Valid || stored.Label.String != "New label" || ref.Label != "New label" {
				t.Errorf("stored labels = column:%v ref:%q, want both New label", stored.Label, ref.Label)
			}
			if ref.LocalPath != "/repo" || ref.DaemonID != daemonID {
				t.Errorf("stored directory = %q/%q, want /repo/%s", ref.LocalPath, ref.DaemonID, daemonID)
			}
			if stored.Position != 1 {
				t.Errorf("stored position = %d, want 1", stored.Position)
			}
		})
	}
}
