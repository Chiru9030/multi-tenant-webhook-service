package crypto

import (
	"strings"
	"testing"
	"time"
)

func TestSignAndVerifySignature(t *testing.T) {
	secret, err := GenerateSecretKey()
	if err != nil {
		t.Fatalf("failed to generate secret: %v", err)
	}
	if !strings.HasPrefix(secret, "whsec_") {
		t.Errorf("expected secret to start with whsec_, got %s", secret)
	}

	payload := []byte(`{"event":"payment.succeeded","amount":15000,"currency":"usd"}`)
	now := time.Now().Unix()

	sigHeader := SignPayload(now, payload, secret)
	if !strings.Contains(sigHeader, "t=") || !strings.Contains(sigHeader, "v1=") {
		t.Fatalf("invalid signature header format: %s", sigHeader)
	}

	// 1. Valid verification within tolerance window
	valid, err := VerifySignature(sigHeader, payload, secret, 5*time.Minute)
	if err != nil || !valid {
		t.Fatalf("expected signature to be valid, got valid=%v, err=%v", valid, err)
	}

	// 2. Tampered payload
	tamperedPayload := []byte(`{"event":"payment.succeeded","amount":99999,"currency":"usd"}`)
	validTampered, _ := VerifySignature(sigHeader, tamperedPayload, secret, 5*time.Minute)
	if validTampered {
		t.Fatalf("expected tampered payload to fail signature verification")
	}

	// 3. Wrong secret
	wrongSecret := "whsec_00000000000000000000000000000000"
	validWrongSec, _ := VerifySignature(sigHeader, payload, wrongSecret, 5*time.Minute)
	if validWrongSec {
		t.Fatalf("expected wrong secret to fail signature verification")
	}

	// 4. Expired timestamp beyond tolerance window
	oldTimestamp := time.Now().Unix() - 600 // 10 minutes ago
	oldSigHeader := SignPayload(oldTimestamp, payload, secret)
	validExpired, errExpired := VerifySignature(oldSigHeader, payload, secret, 5*time.Minute)
	if validExpired || errExpired == nil {
		t.Fatalf("expected expired timestamp to fail verification, got valid=%v, err=%v", validExpired, errExpired)
	}
}
