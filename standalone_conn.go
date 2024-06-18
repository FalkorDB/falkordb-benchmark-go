package main

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/FalkorDB/falkordb-go"
	"log"
	"os"
	"time"
)

func getStandaloneConn(graphName, addr string, password string, tlsCaCertFile string, loadTimeout int) (graph *falkordb.Graph, conn *falkordb.FalkorDB) {
	var err error
	if tlsCaCertFile != "" {
		// Load CA cert
		caCert, err := os.ReadFile(tlsCaCertFile)
		if err != nil {
			log.Panicln(err.Error())
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		clientTLSConfig := &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: true,
		}
		// InsecureSkipVerify controls whether a client verifies the
		// server's certificate chain and host name.
		// If InsecureSkipVerify is true, TLS accepts any certificate
		// presented by the server and any host name in that certificate.
		// In this mode, TLS is susceptible to man-in-the-middle attacks.
		// This should be used only for testing.
		if password != "" {
			conn, err = falkordb.FalkorDBNew(&falkordb.ConnectionOption{
				Addr:        addr,
				Password:    password,
				TLSConfig:   clientTLSConfig,
				ReadTimeout: time.Duration(loadTimeout) * time.Second,
			})
		} else {
			conn, err = falkordb.FalkorDBNew(&falkordb.ConnectionOption{
				Addr:        addr,
				TLSConfig:   clientTLSConfig,
				ReadTimeout: time.Duration(loadTimeout) * time.Second,
			})
		}
	} else {
		if password != "" {
			conn, err = falkordb.FalkorDBNew(&falkordb.ConnectionOption{
				Addr:        addr,
				Password:    password,
				ReadTimeout: time.Duration(loadTimeout) * time.Second,
			})
		} else {
			conn, err = falkordb.FalkorDBNew(&falkordb.ConnectionOption{
				Addr:        addr,
				ReadTimeout: time.Duration(loadTimeout) * time.Second,
			})
		}
	}

	if err != nil {
		log.Panicf("Error preparing for benchmark, while creating new connection. error = %v", err)
	}
	if conn != nil {
		graph = conn.SelectGraph(graphName)
	}
	return graph, conn
}
