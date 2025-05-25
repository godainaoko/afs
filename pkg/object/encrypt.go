// pkg/object/encrypt.go

package object

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"golang.org/x/crypto/pbkdf2"
	"io"
	"os"
	"strings"
)

type Encryptor interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

type rsaEncryptor struct {
	privKey *rsa.PrivateKey
	label   []byte
}

func ExportRsaPrivateKeyToPem(key *rsa.PrivateKey, passphrase string) string {
	// Marshal to PKCS#8 (better than PKCS#1 for encryption)
	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		panic(err)
	}

	block := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: pkcs8Bytes,
	}

	if passphrase != "" {
		// Use PBKDF2 + AES-GCM for secure encryption
		salt := make([]byte, 8)
		if _, err := io.ReadFull(rand.Reader, salt); err != nil {
			panic(err)
		}

		// Derive key using PBKDF2
		key := pbkdf2.Key([]byte(passphrase), salt, 10000, 32, sha256.New)

		// AES-GCM encryption
		aesBlock, err := aes.NewCipher(key)
		if err != nil {
			panic(err)
		}

		gcm, err := cipher.NewGCM(aesBlock)
		if err != nil {
			panic(err)
		}

		nonce := make([]byte, gcm.NonceSize())
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			panic(err)
		}

		encryptedData := gcm.Seal(nil, nonce, block.Bytes, nil)

		// Store salt + nonce + encrypted data in PEM
		encryptedBlock := &pem.Block{
			Type: "ENCRYPTED PRIVATE KEY",
			Headers: map[string]string{
				"Proc-Type": "4,ENCRYPTED",
				"DEK-Info":  fmt.Sprintf("PBES2-AES256-GCM,%X", salt),
			},
			Bytes: append(nonce, encryptedData...),
		}

		return string(pem.EncodeToMemory(encryptedBlock))
	}

	// No passphrase: return unencrypted PEM
	return string(pem.EncodeToMemory(block))
}

func ParseRsaPrivateKeyFromPem(privPEM string, passphrase string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(privPEM))
	if block == nil {
		return nil, errors.New("failed to parse PEM block containing the key")
	}

	buf := block.Bytes

	// Check if PEM is encrypted
	if strings.Contains(block.Headers["Proc-Type"], "ENCRYPTED") {
		if passphrase == "" {
			return nil, fmt.Errorf("passphrase is required to decrypt private key")
		}

		// Extract salt from DEK-Info header
		dekInfo := block.Headers["DEK-Info"]
		if !strings.HasPrefix(dekInfo, "PBES2-AES256-GCM,") {
			return nil, fmt.Errorf("unsupported encryption scheme")
		}

		salt, err := hex.DecodeString(strings.TrimPrefix(dekInfo, "PBES2-AES256-GCM,"))
		if err != nil {
			return nil, fmt.Errorf("invalid salt in DEK-Info")
		}

		// Derive key using PBKDF2
		key := pbkdf2.Key([]byte(passphrase), salt, 10000, 32, sha256.New)

		// AES-GCM decryption
		aesBlock, err := aes.NewCipher(key)
		if err != nil {
			return nil, fmt.Errorf("failed to create AES cipher: %v", err)
		}

		gcm, err := cipher.NewGCM(aesBlock)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCM: %v", err)
		}

		nonceSize := gcm.NonceSize()
		if len(buf) < nonceSize {
			return nil, fmt.Errorf("invalid encrypted data length")
		}

		nonce, encryptedData := buf[:nonceSize], buf[nonceSize:]
		buf, err = gcm.Open(nil, nonce, encryptedData, nil)
		if err != nil {
			return nil, fmt.Errorf("decryption failed: %v", err)
		}
	} else if passphrase != "" {
		fmt.Println("Warning: passphrase is not used, because private key is not encrypted")
	}

	// Try parsing as PKCS#8 first
	privKey, err := x509.ParsePKCS8PrivateKey(buf)
	if err == nil {
		if rsaKey, ok := privKey.(*rsa.PrivateKey); ok {
			return rsaKey, nil
		}
		return nil, errors.New("key is not an RSA private key")
	}

	// Fallback to PKCS#1 if PKCS#8 fails
	priv, err := x509.ParsePKCS1PrivateKey(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %v", err)
	}

	return priv, nil
}

func ParseRsaPrivateKeyFromPath(path, passphrase string) (*rsa.PrivateKey, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseRsaPrivateKeyFromPem(string(b), passphrase)
}

func NewRSAEncryptor(privKey *rsa.PrivateKey) Encryptor {
	return &rsaEncryptor{privKey, []byte("keys")}
}

func (e *rsaEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	return rsa.EncryptOAEP(sha256.New(), rand.Reader, &e.privKey.PublicKey, plaintext, e.label)
}

func (e *rsaEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	return rsa.DecryptOAEP(sha256.New(), rand.Reader, e.privKey, ciphertext, e.label)
}

type aesEncryptor struct {
	keyEncryptor Encryptor
	keyLen       int
}

func NewAESEncryptor(keyEncryptor Encryptor) Encryptor {
	return &aesEncryptor{keyEncryptor, 32} //  AES-256-GCM
}

func (e *aesEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	key := make([]byte, e.keyLen)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, err
	}
	cipherKey, err := e.keyEncryptor.Encrypt(key)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aesgcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	headerSize := 3 + len(cipherKey) + len(nonce)
	buf := make([]byte, headerSize+len(plaintext)+aesgcm.Overhead())
	buf[0] = byte(len(cipherKey) >> 8)
	buf[1] = byte(len(cipherKey) & 0xFF)
	buf[2] = byte(len(nonce))
	p := buf[3:]
	copy(p, cipherKey)
	p = p[len(cipherKey):]
	copy(p, nonce)
	p = p[len(nonce):]
	ciphertext := aesgcm.Seal(p[:0], nonce, plaintext, nil)
	return buf[:headerSize+len(ciphertext)], nil
}

func (e *aesEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	keyLen := int(ciphertext[0])<<8 + int(ciphertext[1])
	nonceLen := int(ciphertext[2])
	if 3+keyLen+nonceLen >= len(ciphertext) {
		return nil, fmt.Errorf("misformed ciphertext: %d %d", keyLen, nonceLen)
	}
	ciphertext = ciphertext[3:]
	cipherKey := ciphertext[:keyLen]
	nonce := ciphertext[keyLen : keyLen+nonceLen]
	ciphertext = ciphertext[keyLen+nonceLen:]

	key, err := e.keyEncryptor.Decrypt(cipherKey)
	if err != nil {
		return nil, errors.New("decrypt key: " + err.Error())
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return aesgcm.Open(ciphertext[:0], nonce, ciphertext, nil)
}

type encrypted struct {
	ObjectStorage
	enc Encryptor
}

// NewEncrypted returns an encrypted object storage
func NewEncrypted(o ObjectStorage, enc Encryptor) ObjectStorage {
	return &encrypted{o, enc}
}

func (e *encrypted) String() string {
	return fmt.Sprintf("%s(encrypted)", e.ObjectStorage)
}

func (e *encrypted) Get(key string, off, limit int64) (io.ReadCloser, error) {
	r, err := e.ObjectStorage.Get(key, 0, -1)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	ciphertext, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	plain, err := e.enc.Decrypt(ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %s", err)
	}
	l := int64(len(plain))
	if off > l {
		return nil, io.EOF
	}
	if limit == -1 || off+limit > l {
		limit = l - off
	}
	data := plain[off : off+limit]
	return io.NopCloser(bytes.NewBuffer(data)), nil
}

func (e *encrypted) Put(key string, in io.Reader) error {
	plain, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	ciphertext, err := e.enc.Encrypt(plain)
	if err != nil {
		return err
	}
	return e.ObjectStorage.Put(key, bytes.NewReader(ciphertext))
}

var _ ObjectStorage = &encrypted{}
