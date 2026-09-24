package model

import "testing"

func TestResourcesMath(t *testing.T) {
	r := Resources{CPUMilli: 100, MemoryMB: 10, StorageGB: 1}
	mul := r.Mul(3)
	if mul.CPUMilli != 300 || mul.MemoryMB != 30 || mul.StorageGB != 3 {
		t.Fatalf("mul = %+v", mul)
	}
	sum := mul.Add(r)
	if sum.CPUMilli != 400 {
		t.Fatalf("add = %+v", sum)
	}
	if r.Mul(-1).CPUMilli != 0 {
		t.Fatal("negative mul should clamp to 0")
	}
}

func TestRequestedAndAllocated(t *testing.T) {
	s := EnvSpec{Replicas: 4, Resources: Resources{CPUMilli: 50, MemoryMB: 64, StorageGB: 2}}
	if got := s.Requested(); got.CPUMilli != 200 || got.StorageGB != 8 {
		t.Fatalf("requested=%+v", got)
	}
	e := &Environment{Spec: s, Allocations: []Allocation{
		{Resources: s.Resources}, {Resources: s.Resources},
	}}
	if got := e.Allocated(); got.CPUMilli != 100 {
		t.Fatalf("allocated=%d want 100", got.CPUMilli)
	}
}

func TestExpiryAndTenantUsage(t *testing.T) {
	st := NewState()
	e1 := &Environment{Desired: DesiredRunning, Spec: EnvSpec{TenantID: "t1", Replicas: 2, Resources: Resources{CPUMilli: 10}}}
	e1.Allocations = []Allocation{{Resources: e1.Spec.Resources}, {Resources: e1.Spec.Resources}}
	e2 := &Environment{Desired: DesiredStopped, Spec: EnvSpec{TenantID: "t1", Replicas: 9, Resources: Resources{CPUMilli: 10}}}
	e2.Allocations = nil
	st.Envs["1"] = e1
	st.Envs["2"] = e2
	if got := TenantReserved(st, "t1"); got.CPUMilli != 20 {
		t.Fatalf("reserved=%d want 20", got.CPUMilli)
	}
	if got := TenantUsage(st, "t1"); got.CPUMilli != 20 {
		t.Fatalf("usage=%d want 20", got.CPUMilli)
	}
	// Deleted env contributes nothing.
	e2.Desired = DesiredRunning
	e2.Deleted = true
	if got := TenantReserved(st, "t1"); got.CPUMilli != 20 {
		t.Fatalf("reserved with deleted=%d want 20", got)
	}
}
