package etcd

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// buildTLSConfig builds TLS configuration from certificate files
func buildTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	config := &tls.Config{}

	// Load client certificate if provided
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}
	}

	// Load CA certificate if provided
	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		config.RootCAs = caCertPool
	}

	// If no CA file is provided, use system default
	if caFile == "" {
		config.InsecureSkipVerify = false
	}

	return config, nil
}
