package aibot

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"testing"
)

func TestDecryptFile(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	plaintext := []byte("hello wecom")
	padLen := aes.BlockSize - len(plaintext)%aes.BlockSize
	padded := append([]byte(nil), plaintext...)
	for range padLen {
		padded = append(padded, byte(padLen))
	}

	encrypted := make([]byte, len(padded))
	cipher.NewCBCEncrypter(mustCipher(t, key), key[:aes.BlockSize]).CryptBlocks(encrypted, padded)

	result, err := DecryptFile(encrypted, base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatalf("DecryptFile returned error: %v", err)
	}
	if !bytes.Equal(result, plaintext) {
		t.Fatalf("expected %q, got %q", plaintext, result)
	}
}

func TestDecryptFileRejectsEmptyInput(t *testing.T) {
	if _, err := DecryptFile(nil, "abc"); err == nil {
		t.Fatal("expected error for empty input")
	}
}

func mustCipher(t *testing.T, key []byte) cipher.Block {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("aes.NewCipher: %v", err)
	}
	return block
}
