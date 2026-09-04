// Command keva is the command-line client for a Keva cluster.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/0xnikshi/keva/internal/api"
)

// errNotFound reports that a requested key is absent.
var errNotFound = errors.New("key not found")

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "keva:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("keva", flag.ContinueOnError)
	addr := fs.String("addr", "http://localhost:7070", "Keva daemon address")
	fs.Usage = func() {
		_, _ = fmt.Fprint(fs.Output(), "usage: keva [--addr ADDR] <command> [args]\n\n"+
			"Commands:\n"+
			"  put <key> <value>   store a value\n"+
			"  get <key>           print a value\n"+
			"  delete <key>        remove a key\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	rest := fs.Args()
	if len(rest) == 0 {
		fs.Usage()
		return errors.New("no command given")
	}

	c := &client{addr: normalizeAddr(*addr), http: http.DefaultClient}
	cmd, cmdArgs := rest[0], rest[1:]

	switch cmd {
	case "put":
		if len(cmdArgs) != 2 {
			return errors.New("usage: keva put <key> <value>")
		}
		return c.put(cmdArgs[0], cmdArgs[1])
	case "get":
		if len(cmdArgs) != 1 {
			return errors.New("usage: keva get <key>")
		}
		val, err := c.get(cmdArgs[0])
		if err != nil {
			return err
		}
		fmt.Println(string(val))
		return nil
	case "delete":
		if len(cmdArgs) != 1 {
			return errors.New("usage: keva delete <key>")
		}
		return c.delete(cmdArgs[0])
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func normalizeAddr(a string) string {
	a = strings.TrimRight(a, "/")
	if !strings.HasPrefix(a, "http://") && !strings.HasPrefix(a, "https://") {
		a = "http://" + a
	}
	return a
}

// client talks to a Keva daemon over HTTP.
type client struct {
	addr string
	http *http.Client
}

func (c *client) keyURL(key string) string {
	return c.addr + "/v1/kv/" + url.PathEscape(key)
}

func (c *client) put(key, value string) error {
	body, err := json.Marshal(api.PutRequest{Value: []byte(value)})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, c.keyURL(key), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNoContent {
		return serverError(resp)
	}
	return nil
}

func (c *client) get(key string) ([]byte, error) {
	resp, err := c.http.Get(c.keyURL(key))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
		var vr api.ValueResponse
		if err := json.NewDecoder(resp.Body).Decode(&vr); err != nil {
			return nil, err
		}
		return vr.Value, nil
	case http.StatusNotFound:
		return nil, errNotFound
	default:
		return nil, serverError(resp)
	}
}

func (c *client) delete(key string) error {
	req, err := http.NewRequest(http.MethodDelete, c.keyURL(key), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNoContent {
		return serverError(resp)
	}
	return nil
}

// serverError turns a non-success response into an error, preferring the
// server's JSON error message when present.
func serverError(resp *http.Response) error {
	var er api.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&er); err == nil && er.Error != "" {
		return fmt.Errorf("server: %s (status %d)", er.Error, resp.StatusCode)
	}
	return fmt.Errorf("server returned status %d", resp.StatusCode)
}
