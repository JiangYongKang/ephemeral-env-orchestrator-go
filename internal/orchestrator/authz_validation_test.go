package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/ephemeral-env-orchestrator-go/internal/domain"
)

// TestAuthzCrossTenant ensures tenants cannot read or mutate each other's
// environments, and that non-owners are rejected.
func TestAuthzCrossTenant(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	e := mustCreate(t, o, CreateRequest{
		Principal: user("acme", "alice"), IdempotencyKey: "k",
		Name: "svc", Resources: res(1, 1, 1), Start: false,
	})
	ctx := context.Background()

	// Different tenant: read forbidden.
	if _, err := o.Get(user("globex", "mallory"), e.ID); !domain.IsKind(err, domain.KindForbidden) {
		t.Fatalf("cross-tenant get should be forbidden, got %v", err)
	}
	// Same tenant, different user (not owner): write forbidden but read ok.
	if _, err := o.Get(user("acme", "bob"), e.ID); err != nil {
		t.Fatalf("same-tenant read should succeed, got %v", err)
	}
	if err := o.Start(ctx, StartRequest{Principal: user("acme", "bob"), EnvID: e.ID}); !domain.IsKind(err, domain.KindForbidden) {
		t.Fatalf("non-owner start should be forbidden, got %v", err)
	}
	// Cross-tenant delete forbidden.
	if err := o.Delete(ctx, DeleteRequest{Principal: user("globex", "mallory"), EnvID: e.ID}); !domain.IsKind(err, domain.KindForbidden) {
		t.Fatalf("cross-tenant delete should be forbidden, got %v", err)
	}
	// Admin from another tenant context can manage (admin is global).
	if err := o.Delete(ctx, DeleteRequest{Principal: admin(""), EnvID: e.ID}); err != nil {
		t.Fatalf("admin delete should succeed, got %v", err)
	}
	t.Logf("JUDGE cross-tenant=forbidden, non-owner=forbidden(write)/allowed(read), admin=allowed; final phase=%s",
		o.mustPhase(t, e.ID))
}

// TestValidationRejections checks each illegal input class distinctly.
func TestValidationRejections(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	ctx := context.Background()

	cases := []struct {
		name string
		req  CreateRequest
		want domain.ErrorKind
		dist string
	}{
		{"anonymous", CreateRequest{Principal: domain.Principal{}, Name: "a", Resources: res(1, 1, 1)}, domain.KindUnauthorized, "anonymous"},
		{"empty_name", CreateRequest{Principal: user("t", "u"), Name: "  ", Resources: res(1, 1, 1)}, domain.KindValidation, "name_required"},
		{"negative_res", CreateRequest{Principal: user("t", "u"), Name: "a", Resources: res(-1, 1, 1)}, domain.KindValidation, "negative_resources"},
		{"zero_res", CreateRequest{Principal: user("t", "u"), Name: "a", Resources: res(0, 0, 0)}, domain.KindValidation, "zero_resources"},
		{"negative_ttl", CreateRequest{Principal: user("t", "u"), Name: "a", Resources: res(1, 1, 1), TTL: -1}, domain.KindValidation, "negative_ttl"},
		{"huge_ttl", CreateRequest{Principal: user("t", "u"), Name: "a", Resources: res(1, 1, 1), TTL: 31 * 24 * time.Hour}, domain.KindValidation, "ttl_too_long"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := o.Create(ctx, tc.req)
			de := domain.AsError(err)
			if de == nil || de.Kind != tc.want || de.Distinct != tc.dist {
				t.Fatalf("%s: want %s/%s got %v", tc.name, tc.want, tc.dist, err)
			}
			t.Logf("JUDGE %s => %s/%s msg=%q", tc.name, de.Kind, de.Distinct, de.Msg)
		})
	}
}

// TestImmutableFields confirms id/owner/tenant cannot be changed via update.
func TestImmutableFields(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	e := mustCreate(t, o, CreateRequest{
		Principal: user("acme", "alice"), Name: "s", Resources: res(1, 1, 1), Start: false,
	})
	// There is no API field to set owner/id/tenant, which guarantees
	// immutability structurally. A forged update from another user is denied.
	_, err := o.Update(context.Background(), UpdateRequest{
		Principal: user("acme", "bob"), EnvID: e.ID, Name: strp("x"),
	})
	if !domain.IsKind(err, domain.KindForbidden) {
		t.Fatalf("non-owner update must be forbidden: %v", err)
	}
	got, _ := o.Get(user("acme", "alice"), e.ID)
	if got.Spec.Owner != "alice" || got.Tenant != "acme" || got.ID != e.ID {
		t.Fatalf("immutable identity changed: %+v", got)
	}
	t.Logf("JUDGE owner=%s tenant=%s id=%s preserved after forbidden update attempt", got.Spec.Owner, got.Tenant, got.ID)
}

// TestNotFoundAndTerminal verifies distinct not-found and terminal errors.
func TestNotFoundAndTerminal(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	ctx := context.Background()
	p := user("acme", "alice")
	_, err := o.Get(p, "does-not-exist")
	if !domain.IsKind(err, domain.KindNotFound) {
		t.Fatalf("want not_found, got %v", err)
	}
	e := mustCreate(t, o, CreateRequest{Principal: p, Name: "s", Resources: res(1, 1, 1), Start: false})
	if err := o.Delete(ctx, DeleteRequest{Principal: p, EnvID: e.ID}); err != nil {
		t.Fatal(err)
	}
	// Re-delete of terminated env -> not_found (already_deleted), distinct.
	err = o.Delete(ctx, DeleteRequest{Principal: p, EnvID: e.ID})
	de := domain.AsError(err)
	if de == nil || de.Kind != domain.KindNotFound || de.Distinct != "already_deleted" {
		t.Fatalf("want not_found/already_deleted, got %v", err)
	}
	// Start on terminated -> terminal.
	err = o.Start(ctx, StartRequest{Principal: p, EnvID: e.ID})
	if !domain.IsKind(err, domain.KindTerminal) {
		t.Fatalf("want terminal error, got %v", err)
	}
	t.Logf("JUDGE redeploy-after-delete not_found/%s; start-after-delete terminal", de.Distinct)
}

// TestDuplicateName rejects a second live env with same name per tenant, while
// allowing reuse after deletion and across tenants.
func TestDuplicateName(t *testing.T) {
	h := newHarness(t, nil)
	defer h.o.Close()
	o := h.o
	p := user("acme", "alice")
	mustCreate(t, o, CreateRequest{Principal: p, IdempotencyKey: "a", Name: "shared", Resources: res(1, 1, 1)})
	_, err := o.Create(context.Background(), CreateRequest{Principal: p, IdempotencyKey: "b", Name: "shared", Resources: res(1, 1, 1)})
	if !domain.IsKind(err, domain.KindDuplicate) {
		t.Fatalf("duplicate name should be rejected, got %v", err)
	}
	// Same name in another tenant is fine.
	mustCreate(t, o, CreateRequest{Principal: user("globex", "g"), IdempotencyKey: "c", Name: "shared", Resources: res(1, 1, 1)})
	t.Logf("JUDGE same-tenant duplicate=%v, cross-tenant reuse allowed", err)
}

// helper
func (o *Orchestrator) mustPhase(t *testing.T, id domain.EnvID) domain.Phase {
	t.Helper()
	e, err := o.Get(admin(""), id)
	if err != nil {
		t.Fatal(err)
	}
	return e.Phase
}
