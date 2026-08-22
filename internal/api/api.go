// Package api defines the HTTP + JSON wire contract shared by the Keva
// server and client.
package api

// PutRequest is the body of a PUT request. encoding/json represents the
// []byte value as a base64 string, so binary values travel safely.
type PutRequest struct {
	Value []byte `json:"value"`
}

// ValueResponse is the body of a successful GET.
type ValueResponse struct {
	Value []byte `json:"value"`
}

// ErrorResponse is the body returned for any non-2xx result.
type ErrorResponse struct {
	Error string `json:"error"`
}
