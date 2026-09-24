// Command eo is a small demo CLI driving the local ephemeral environment
// orchestrator. It uses only the local file-backed state and an in-memory fake
// resource backend: no cluster, network or credentials are involved. It is the
// same code path exercised by the unit tests.
//
// Usage (all data lives under the given state directory):
//
//	eo demo --state ./eo-data
//
// The demo: sets capacity, creates two environments, enforces quota, retries
// through an injected transient failure, stops/starts one, expires a short-TTL
// environment and finally deletes one, printing every key state transition.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/backend"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/orchestrator"
)

func main() {
	stateDir := flag.String("state", "./eo-data", "directory for the local state file")
	flag.Parse()

	lg := log.New(os.Stdout, "[eo] ", log.LstdFlags)
	if err := run(*stateDir, lg); err != nil {
		lg.Fatalf("demo failed: %v", err)
	}
}

func run(stateDir string, lg *log.Logger) error {
	ctx := context.Background()
	be := backend.NewFakeBackend()
	o, err := orchestrator.New(ctx, orchestrator.Config{
		StorePath:         filepath.Join(stateDir, "state.json"),
		Clock:             domain.SystemClock{},
		Logger:            lg,
		Backend:           be,
		AutoReconcile:     true,
		ReconcileInterval: 10 * time.Millisecond,
	})
	if err != nil {
		return err
	}
	defer o.Close()

	adm := domain.Principal{User: "root", Role: domain.RoleAdmin}
	if err := o.SetClusterCapacity(adm, domain.Resources{CPU: 8, Memory: 16, Storage: 40}); err != nil {
		return err
	}
	if err := o.SetTenantLimit(adm, "acme", domain.Resources{CPU: 4, Memory: 8, Storage: 20}); err != nil {
		return err
	}

	alice := domain.Principal{User: "alice", Tenant: "acme", Role: domain.RoleUser}
	e1, err := o.Create(ctx, orchestrator.CreateRequest{
		Principal: alice, IdempotencyKey: "demo-e1", Name: "api",
		Resources: domain.Resources{CPU: 2, Memory: 4, Storage: 10},
		TTL:       50 * time.Millisecond, Start: true,
	})
	if err != nil {
		// Transient wobble on the first start: background pump will converge.
		lg.Printf("create returned transient error (will converge): %v", err)
	}
	e2, err := o.Create(ctx, orchestrator.CreateRequest{
		Principal: alice, IdempotencyKey: "demo-e2", Name: "web",
		Resources: domain.Resources{CPU: 1, Memory: 2, Storage: 5}, Start: false,
	})
	if err != nil {
		return err
	}

	// Quota boundary: this must be rejected (tenant limit).
	if _, err := o.Create(ctx, orchestrator.CreateRequest{
		Principal: alice, IdempotencyKey: "demo-too-big", Name: "huge",
		Resources: domain.Resources{CPU: 4, Memory: 4, Storage: 4}, Start: true,
	}); err != nil {
		if de := domain.AsError(err); de != nil {
			fmt.Printf(">> rejected as expected: kind=%s distinct=%s\n", de.Kind, de.Distinct)
		}
	}

	// Idempotent duplicate: returns the same env, no extra side effects.
	dup, err := o.Create(ctx, orchestrator.CreateRequest{
		Principal: alice, IdempotencyKey: "demo-e2", Name: "web",
		Resources: domain.Resources{CPU: 1, Memory: 2, Storage: 5}, Start: false,
	})
	if err != nil || dup.ID != e2.ID {
		return fmt.Errorf("idempotent replay broken: dup=%+v err=%v", dup, err)
	}
	fmt.Printf(">> duplicate create returned same env %s\n", e2.ID)

	// Give the background pump time to converge e1 through its wobble and for
	// e1's short TTL to expire and be reclaimed.
	time.Sleep(120 * time.Millisecond)
	g1, _ := o.Get(alice, e1.ID)
	fmt.Printf(">> e1 after TTL: phase=%s (short-TTL env is reclaimed)\n", g1.Phase)

	// Demonstrate a transient start wobble that self-heals: inject a single
	// failure for e2's first start attempt. The synchronous call reports the
	// transient error and stays converging; the background pump retries the
	// idempotent EnsureStarted and reaches running with no duplicate resource.
	be.WithFault(backend.Fault{FailStartAttempts: []int{1}})
	if err := o.Start(ctx, orchestrator.StartRequest{Principal: alice, IdempotencyKey: "demo-up", EnvID: e2.ID}); err != nil {
		fmt.Printf(">> start hit a transient wobble (retrying via background pump): %v\n", err)
	}
	be.WithFault(backend.Fault{})
	if err := o.TriggerReconcile(ctx); err != nil {
		return err
	}
	g2, _ := o.Get(alice, e2.ID)
	fmt.Printf(">> e2 started: phase=%s running=%v backendStartN=%d\n", g2.Phase, g2.Running, be.Counts().Started)
	if err := o.Stop(ctx, orchestrator.StopRequest{Principal: alice, IdempotencyKey: "demo-down", EnvID: e2.ID}); err != nil {
		return err
	}
	g2, _ = o.Get(alice, e2.ID)
	fmt.Printf(">> e2 stopped: phase=%s reserved=%+v (quota retained)\n", g2.Phase, g2.Reserved)

	// Delete e2 and show capacity freed.
	if err := o.Delete(ctx, orchestrator.DeleteRequest{Principal: alice, IdempotencyKey: "demo-del", EnvID: e2.ID}); err != nil {
		return err
	}
	used, _ := o.TenantUsage(adm, "acme")
	fmt.Printf(">> e2 deleted; acme usage now=%+v, live backend resources=%v, sideEffects=%+v\n",
		used, be.Existing(), be.Counts())
	return nil
}
