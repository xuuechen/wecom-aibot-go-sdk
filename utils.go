package aibot

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func GenerateRandomString(length int) string {
	if length <= 0 {
		length = 8
	}

	buf := make([]byte, (length+1)/2)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Errorf("generate random string: %w", err))
	}

	return hex.EncodeToString(buf)[:length]
}

func GenerateReqID(prefix string) string {
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UnixMilli(), GenerateRandomString(8))
}
