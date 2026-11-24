package crypto

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// GenerateSecretKey produces a high-entropy secret key prefixed with whsec_
func GenerateSecretKey() (string, error) {
	bytes := make([]byte, 24)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "whsec_" + hex.EncodeToString(bytes), nil
}

// SignPayload generates a standard Stripe/Svix compatible signature header:
// "t=1700000000,v1=9b3c...hex_hash"
func SignPayload(timestamp int64, payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	// Canonical signing string: "{timestamp}.{payload}"
	mac.Write([]byte(fmt.Sprintf("%d.", timestamp)))
	mac.Write(payload)
	signature := hex.EncodeToString(mac.Sum(nil))

	return fmt.Sprintf("t=%d,v1=%s", timestamp, signature)
}

// VerifySignature validates that an incoming webhook signature matches the payload and secret,
// and ensures the timestamp is within the tolerance window to prevent replay attacks.
func VerifySignature(header string, payload []byte, secret string, tolerance time.Duration) (bool, error) {
	parts := strings.Split(header, ",")
	var tStr, v1Str string

	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "t":
			tStr = kv[1]
		case "v1":
			v1Str = kv[1]
		}
	}

	if tStr == "" || v1Str == "" {
		return false, fmt.Errorf("malformed signature header")
	}

	timestamp, err := strconv.ParseInt(tStr, 10, 64)
	if err != nil {
		return false, fmt.Errorf("invalid timestamp format: %w", err)
	}

	if tolerance > 0 {
		diff := math.Abs(float64(time.Now().Unix() - timestamp))
		if diff > tolerance.Seconds() {
			return false, fmt.Errorf("timestamp outside tolerance window (%.0fs > %.0fs)", diff, tolerance.Seconds())
		}
	}

	expectedSig := SignPayload(timestamp, payload, secret)
	expectedParts := strings.Split(expectedSig, ",")
	var expectedV1 string
	for _, p := range expectedParts {
		if strings.HasPrefix(p, "v1=") {
			expectedV1 = strings.TrimPrefix(p, "v1=")
			break
		}
	}

	// Constant-time compare to mitigate timing attacks
	v1Bytes, err1 := hex.DecodeString(v1Str)
	expBytes, err2 := hex.DecodeString(expectedV1)
	if err1 != nil || err2 != nil {
		return false, fmt.Errorf("hex decoding error")
	}

	return hmac.Equal(v1Bytes, expBytes), nil
}
