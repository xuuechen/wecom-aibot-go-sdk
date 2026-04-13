package aibot

import (
	"strings"
	"testing"
)

func TestGenerateRandomString(t *testing.T) {
	value := GenerateRandomString(12)
	if len(value) != 12 {
		t.Fatalf("expected random string length 12, got %d", len(value))
	}
}

func TestGenerateReqID(t *testing.T) {
	reqID := GenerateReqID(CmdSubscribe)
	if !strings.HasPrefix(reqID, CmdSubscribe+"_") {
		t.Fatalf("unexpected req id prefix: %s", reqID)
	}
}
