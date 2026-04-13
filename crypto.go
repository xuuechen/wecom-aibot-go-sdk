package aibot

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"strings"
)

func DecryptFile(encryptedData []byte, aesKey string) ([]byte, error) {
	if len(encryptedData) == 0 {
		return nil, fmt.Errorf("decrypt_file: encrypted_data is empty or not provided")
	}
	if strings.TrimSpace(aesKey) == "" {
		return nil, fmt.Errorf("decrypt_file: aes_key must be a non-empty string")
	}

	paddedKey := aesKey
	if mod := len(paddedKey) % 4; mod != 0 {
		paddedKey += strings.Repeat("=", 4-mod)
	}

	key, err := base64.StdEncoding.DecodeString(paddedKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt_file: decryption failed - invalid base64 aesKey: %w", err)
	}
	if len(key) < aes.BlockSize {
		return nil, fmt.Errorf("decrypt_file: decryption failed - decoded aesKey is too short")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("decrypt_file: decryption failed - %w", err)
	}

	ciphertext := encryptedData
	if remainder := len(ciphertext) % aes.BlockSize; remainder != 0 {
		padding := make([]byte, aes.BlockSize-remainder)
		ciphertext = append(append([]byte(nil), ciphertext...), padding...)
	} else {
		ciphertext = append([]byte(nil), ciphertext...)
	}

	plaintext := make([]byte, len(ciphertext))
	iv := key[:aes.BlockSize]
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(plaintext, ciphertext)

	if len(plaintext) == 0 {
		return nil, fmt.Errorf("decrypt_file: decryption failed - decrypted data is empty")
	}

	padLen := int(plaintext[len(plaintext)-1])
	if padLen < 1 || padLen > 32 || padLen > len(plaintext) {
		return nil, fmt.Errorf("decrypt_file: decryption failed - invalid PKCS#7 padding value: %d", padLen)
	}
	for i := len(plaintext) - padLen; i < len(plaintext); i++ {
		if int(plaintext[i]) != padLen {
			return nil, fmt.Errorf("decrypt_file: decryption failed - invalid PKCS#7 padding: padding bytes mismatch")
		}
	}

	return plaintext[:len(plaintext)-padLen], nil
}
