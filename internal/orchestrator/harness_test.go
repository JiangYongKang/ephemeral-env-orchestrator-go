package orchestrator

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/backend"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

// harness bundles the pieces a test drives. clock and backend are shared so a
// "restart" (New over the same state path) observes the same fake resources.
type harness struct {
	dir   string
	clk   *domain.FakeClock
	be    *backend.FakeBackend
	log   *log.Logger
	o     *Orchestrator
	mu    sync.Mutex
	pathN int
}

func newHarness(t *testing.T, be *backend.FakeBackend) *harness {
	t.Helper()
	lg := log.New(os.Stdout, "[test] ", log.LstdFlags|log.Lmsgprefix)
	dir := t.TempDir()
	clk := &domain.FakeClock{T: time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)}
	if be == nil {
		be = backend.NewFakeBackend()
	}
	h := &harness{dir: dir, clk: clk, be: be, log: lg}
	h.o = h.boot(t)
	return h
}

// statePath gives a fresh file name per "restart" is NOT desired: restarts must
// use the SAME file. Keep one fixed path.
func (h *harness) statePath() string { return filepath.Join(h.dir, "state.json") }

// boot constructs an orchestrator (startup recovery runs inside New).
func (h *harness) boot(t *testing.T) *Orchestrator {
	t.Helper()
	o, err := New(context.Background(), Config{
		StorePath: h.statePath(),
		Clock:     h.clk,
		Logger:    h.log,
		Backend:   h.be,
	})
	if err != nil {
		t.Fatalf("boot: %v", err)
	}
	return o
}

// restart closes and re-boots over the same state file and backend, simulating
// a control-plane process restart.
func (h *harness) restart(t *testing.T) *Orchestrator {
	t.Helper()
	h.log.Printf("[restart] simulating process restart at %s", h.clk.Now().Format("15:04:05"))
	h.o.Close()
	h.o = h.boot(t)
	return h.o
}

func user(tenant, user string) domain.Principal {
	return domain.Principal{User: domain.UserID(user), Tenant: domain.TenantID(tenant), Role: domain.RoleUser}
}
func admin(tenant string) domain.Principal {
	return domain.Principal{User: "root", Tenant: domain.TenantID(tenant), Role: domain.RoleAdmin}
}
func res(cpu, mem, sto int) domain.Resources {
	return domain.Resources{CPU: cpu, Memory: mem, Storage: sto}
}

func mustCreate(t *testing.T, o *Orchestrator, req CreateRequest) *domain.Environment {
	t.Helper()
	e, err := o.Create(context.Background(), req)
	if err != nil {
		t.Fatalf("create %q: %v", req.Name, err)
	}
	return e
}
