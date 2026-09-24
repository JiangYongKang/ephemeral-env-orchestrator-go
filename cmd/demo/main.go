// Command demo exercises the local ephemeral-environment orchestrator
// end-to-end without any cluster, external service or credentials. It uses a
// JSON file under the OS temp directory as the durable control-plane state.
//
// Run:  go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	cerr "github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/errors"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/model"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/orchestrator"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/quota"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/resource"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/store"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	path := filepath.Join(os.TempDir(), "ephemeral-orchestrator-demo", "state.json")
	st, err := store.New(path)
	must(err)
	backend := resource.NewBackend()
	o, err := orchestrator.New(st, quota.NewChecker(), backend,
		orchestrator.WithLogger(logger),
		orchestrator.WithTickInterval(100*time.Millisecond),
	)
	must(err)
	defer func() { _ = o.Shutdown(context.Background()) }()

	ctx := context.Background()
	admin := orchestrator.Actor{Name: "root", TenantID: "admin", Role: orchestrator.RoleAdmin}
	alice := orchestrator.Actor{Name: "alice", TenantID: "team-a", Role: orchestrator.RoleOwner}

	must(o.SetClusterCapacity(ctx, admin, model.Resources{CPUMilli: 8000, MemoryMB: 16000, StorageGB: 200}))
	must(o.SetTenantQuota(ctx, admin, "team-a", model.Resources{CPUMilli: 2000, MemoryMB: 4096, StorageGB: 50}))

	fmt.Println("== declare web: 3 replicas, TTL 30s (or recover it after restart) ==")
	env, err := o.CreateEnvironment(ctx, alice, orchestrator.CreateInput{
		Name: "web", Image: "registry.local/web:v1", Replicas: 3,
		Resources:  model.Resources{CPUMilli: 250, MemoryMB: 256, StorageGB: 2},
		TTLSeconds: 30,
	})
	if cerr.Is(err, cerr.KindConflict) {
		fmt.Println("   recovered existing declaration after process restart:", env.ID)
	} else {
		must(err)
	}
	wait(o, ctx)
	cur, _ := o.GetEnvironment(ctx, alice, env.ID)
	_ = cur
	show(o, ctx, env.ID, alice)

	fmt.Println("== stop ==")
	if cur.Phase != model.PhaseStopped {
		must(o.StopEnvironment(ctx, alice, env.ID))
	}
	wait(o, ctx)
	show(o, ctx, env.ID, alice)

	fmt.Println("== start again (renew TTL first if a restart found it expired) ==")
	cur, _ = o.GetEnvironment(ctx, alice, env.ID)
	if cur.Phase == model.PhaseExpired {
		ttl := 30
		fmt.Println("   (environment had expired while the process was away; renewing TTL)")
		_, err = o.UpdateEnvironment(ctx, alice, env.ID, orchestrator.UpdateInput{TTLSeconds: &ttl})
		must(err)
	}
	must(o.StartEnvironment(ctx, alice, env.ID))
	wait(o, ctx)
	show(o, ctx, env.ID, alice)

	fmt.Println("== duplicate create is rejected with Conflict, no side effect ==")
	if _, err := o.CreateEnvironment(ctx, alice, orchestrator.CreateInput{
		Name: "web", Image: "registry.local/web:v1", Replicas: 3,
		Resources: model.Resources{CPUMilli: 250, MemoryMB: 256, StorageGB: 2},
	}); err != nil {
		fmt.Println("   rejected:", err)
	}

	fmt.Printf("== backend stats: %+v ==\n", backend.Stats())
	fmt.Println("state file:", path)
	fmt.Println("restart this program to observe load + reattach recovery (delete to reset).")
}

func wait(o *orchestrator.Orchestrator, ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	must(o.WaitForIdle(ctx))
}

func show(o *orchestrator.Orchestrator, ctx context.Context, id string, a orchestrator.Actor) {
	e, err := o.GetEnvironment(ctx, a, id)
	must(err)
	fmt.Printf("   env=%s phase=%s desired=%s allocations=%d failReason=%q\n",
		e.ID, e.Phase, e.Desired, len(e.Allocations), e.FailReason)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
