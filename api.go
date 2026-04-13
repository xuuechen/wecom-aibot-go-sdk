package aibot

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var (
	filenameUTF8Pattern = regexp.MustCompile(`filename\*=UTF-8''([^;\s]+)`)
	filenamePattern     = regexp.MustCompile(`filename="?([^";\s]+)"?`)
)

type APIClient struct {
	logger Logger
	client *http.Client
}

func NewAPIClient(logger Logger, timeout time.Duration) *APIClient {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout: timeout,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	return &APIClient{
		logger: logger,
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
	}
}

func (c *APIClient) DownloadFileRaw(ctx context.Context, rawURL string) ([]byte, string, error) {
	ctx = ensureContext(ctx)
	c.logger.Info("Downloading file...")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, "", err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		c.logger.Error("File download failed: %v", err)
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("file download failed: unexpected status %s", resp.Status)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		c.logger.Error("File download failed: %v", err)
		return nil, "", err
	}

	filename := extractFilename(resp.Header.Get("Content-Disposition"))
	c.logger.Info("File downloaded successfully")
	return data, filename, nil
}

func extractFilename(contentDisposition string) string {
	if contentDisposition == "" {
		return ""
	}

	if match := filenameUTF8Pattern.FindStringSubmatch(contentDisposition); len(match) == 2 {
		if name, err := url.PathUnescape(match[1]); err == nil {
			return name
		}
		return match[1]
	}

	if match := filenamePattern.FindStringSubmatch(contentDisposition); len(match) == 2 {
		if name, err := url.PathUnescape(match[1]); err == nil {
			return name
		}
		return match[1]
	}

	return strings.TrimSpace(contentDisposition)
}
