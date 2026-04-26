package idempotency

//unit test, no network or database
//we are testing that KeyFor() is deterministic and produces distinct keys
//tis matters because the entire idempotency guarantee rests on the assumption that the same transaction Id always produces the same key
//if it ever returned diffrent values for the same input we will get silent duplicate charges on retry

import "testing"

// TestKeyFor_Deterministic proves the same input always gives the same key
//if this test ever fails, retries would generate new keys and bypass
//idempotency  meaning duplicate charges become possible
func TestKeyFor_Deterministic(t *testing.T) {
	txnID := "3efe5585-8ec1-4716-8aa5-54a5b4b7f832"

	first := KeyFor(txnID)
	second := KeyFor(txnID)

	// t.Errorf logs the failure message but keeps running unlike t.Fatalf
	// We use it here because the test can still check length even if keys differ
	if first != second {
		t.Errorf("KeyFor is not deterministic:\n  first call:  %q\n  second call: %q", first, second)
	}
}

// TestKeyFor_Distinct proves different transaction IDs produce different keys
// If two transactions shared a key, the second would return the first's
// cached result a completely wrong payment outcome
func TestKeyFor_Distinct(t *testing.T) {
	key1 := KeyFor("txn-aaa")
	key2 := KeyFor("txn-bbb")

	if key1 == key2 {
		t.Errorf("different transaction IDs produced the same key: %q", key1)
	}
}

// TestKeyFor_Length checks the key is a usable length
//we truncate the SHA-256 to 16 bytes → 32 hex chars
//this is long enough to be collision-resistant, short enough for HTTP headers
func TestKeyFor_Length(t *testing.T) {
	key := KeyFor("any-transaction-id")

	const want = 32
	if len(key) != want {
		t.Errorf("key length = %d, want %d (key was %q)", len(key), want, key)
	}
}
