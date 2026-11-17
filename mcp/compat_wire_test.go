// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type compatWireParams struct {
	ParamsBase
	TaskID string `json:"taskId,omitempty"`
	Value  string `json:"value,omitempty"`
}

type compatWireResult struct {
	ResultBase
	Value string `json:"value,omitempty"`
}

type compatWireLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *compatWireLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for line := range strings.SplitSeq(strings.TrimSuffix(string(p), "\n"), "\n") {
		if strings.HasPrefix(line, "read: ") || strings.HasPrefix(line, "write: ") {
			l.lines = append(l.lines, line)
		}
	}
	return len(p), nil
}

func (l *compatWireLog) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *compatWireLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n") + "\n"
}

func (l *compatWireLog) byDirection() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var writes, reads []string
	for _, line := range l.lines {
		if strings.HasPrefix(line, "read: ") {
			reads = append(reads, line)
		} else {
			writes = append(writes, line)
		}
	}
	return "== writes ==\n" + strings.Join(writes, "\n") + "\n== reads ==\n" + strings.Join(reads, "\n") + "\n"
}

// TestCompatWire compares what a program with its own tasks/* custom methods
// sends and receives with testdata/compat_wire.golden. The test uses nothing
// of the tasks extension, and the golden is written (with -update) on the SDK
// without the extension, never on this tree.
func TestCompatWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var transcript strings.Builder
	transcript.WriteString("== in-memory ==\n")
	transcript.WriteString(runCompatWireInMemory(ctx, t))
	transcript.WriteString("== streamable-http-stateless ==\n")
	transcript.WriteString(runCompatWireHTTP(ctx, t, true))
	transcript.WriteString("== streamable-http-stateful ==\n")
	transcript.WriteString(runCompatWireHTTP(ctx, t, false))
	transcript.WriteString("== unsolicited-task ==\n")
	transcript.WriteString(runCompatWireUnsolicitedTask(ctx, t))
	got := transcript.String()

	const golden = "testdata/compat_wire.golden"
	if *update {
		if err := os.WriteFile(golden, []byte(got), 0o666); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Errorf("wire transcript changed\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}
}

func newCompatWireServer(t *testing.T) *Server {
	t.Helper()
	s := NewServer(&Implementation{Name: "compat-server", Version: "v1"}, &ServerOptions{
		GetSessionID: func() string { return "compat-session" },
		SubscribeHandler: func(context.Context, *SubscribeRequest) error {
			return nil
		},
		UnsubscribeHandler: func(context.Context, *UnsubscribeRequest) error {
			return nil
		},
	})
	s.AddTool(&Tool{Name: "success", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, *CallToolRequest) (*CallToolResult, error) {
			return &CallToolResult{Content: []Content{&TextContent{Text: "ok"}}}, nil
		})
	s.AddTool(&Tool{Name: "failure", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, *CallToolRequest) (*CallToolResult, error) {
			return nil, errors.New("tool failed")
		})
	s.AddResource(&Resource{URI: "compat://resource", Name: "compat"},
		func(context.Context, *ReadResourceRequest) (*ReadResourceResult, error) {
			return &ReadResourceResult{}, nil
		})
	for _, method := range []string{"tasks/get", "tasks/update", "tasks/cancel", "acme/echo"} {
		if err := AddReceivingCustomMethod(s, method,
			func(_ context.Context, _ *ServerSession, p *compatWireParams) (*compatWireResult, error) {
				return &compatWireResult{Value: p.Value}, nil
			}); err != nil {
			t.Fatalf("AddReceivingCustomMethod(%q): %v", method, err)
		}
	}
	return s
}

func newCompatWireClient(t *testing.T) *Client {
	t.Helper()
	c := NewClient(&Implementation{Name: "compat-client", Version: "v1"}, nil)
	for _, method := range []string{"tasks/get", "tasks/update", "tasks/cancel", "acme/echo"} {
		if err := AddSendingCustomMethod[*compatWireParams, *compatWireResult](c, method); err != nil {
			t.Fatalf("AddSendingCustomMethod(%q): %v", method, err)
		}
	}
	return c
}

func runCompatWireSession(ctx context.Context, t *testing.T, cs *ClientSession) {
	t.Helper()
	if _, err := cs.ListTools(ctx, nil); err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	for _, name := range []string{"success", "failure"} {
		_, err := cs.CallTool(ctx, &CallToolParams{Name: name})
		if name == "failure" {
			if err == nil {
				t.Fatal("CallTool(failure) succeeded")
			}
		} else if err != nil {
			t.Fatalf("CallTool(%q): %v", name, err)
		}
	}
	for _, method := range []string{"tasks/get", "tasks/update", "tasks/cancel", "acme/echo"} {
		if _, err := CallCustomMethod[*compatWireParams, *compatWireResult](ctx, cs, method,
			&compatWireParams{TaskID: "task-1", Value: method}); err != nil {
			t.Fatalf("CallCustomMethod(%q): %v", method, err)
		}
	}
	if _, err := CallCustomMethod[*compatWireParams, *compatWireResult](ctx, cs, "tasks/not-registered",
		&compatWireParams{TaskID: "task-1"}); err == nil {
		t.Fatal("unregistered tasks/not-registered call succeeded")
	}
}

func runCompatWireInMemory(ctx context.Context, t *testing.T) string {
	t.Helper()
	s := newCompatWireServer(t)
	var log compatWireLog
	ct, st := NewInMemoryTransports()
	ss, err := s.Connect(ctx, st, nil)
	if err != nil {
		t.Fatalf("server Connect: %v", err)
	}
	cs, err := newCompatWireClient(t).Connect(ctx, &LoggingTransport{Transport: ct, Writer: &log}, nil)
	if err != nil {
		t.Fatalf("client Connect: %v", err)
	}
	runCompatWireSession(ctx, t, cs)
	if err := cs.Close(); err != nil {
		t.Fatalf("client Close: %v", err)
	}
	if err := ss.Close(); err != nil {
		t.Fatalf("server Close: %v", err)
	}

	ct, st = NewInMemoryTransports()
	ss, err = s.Connect(ctx, st, nil)
	if err != nil {
		t.Fatalf("plain server Connect: %v", err)
	}
	cs, err = NewClient(&Implementation{Name: "compat-client", Version: "v1"}, nil).Connect(ctx,
		&LoggingTransport{Transport: ct, Writer: &log}, nil)
	if err != nil {
		t.Fatalf("plain client Connect: %v", err)
	}
	if _, err := CallCustomMethod[*compatWireParams, *compatWireResult](ctx, cs, "tasks/get",
		&compatWireParams{TaskID: "task-1"}); err == nil {
		t.Fatal("unregistered tasks/get call succeeded")
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("plain client Close: %v", err)
	}
	if err := ss.Close(); err != nil {
		t.Fatalf("plain server Close: %v", err)
	}

	ct, st = NewInMemoryTransports()
	ss, err = s.Connect(ctx, st, nil)
	if err != nil {
		t.Fatalf("legacy server Connect: %v", err)
	}
	cs, err = newCompatWireClient(t).Connect(ctx, &LoggingTransport{Transport: ct, Writer: &log},
		&ClientSessionOptions{ProtocolVersion: "2025-11-25"})
	if err != nil {
		t.Fatalf("legacy client Connect: %v", err)
	}
	if err := cs.Subscribe(ctx, &SubscribeParams{URI: "compat://resource"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := cs.Unsubscribe(ctx, &UnsubscribeParams{URI: "compat://resource"}); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("legacy client Close: %v", err)
	}
	if err := ss.Close(); err != nil {
		t.Fatalf("legacy server Close: %v", err)
	}
	return log.byDirection()
}

type compatWireRoundTripper struct {
	base http.RoundTripper
	log  *compatWireLog
}

func (t *compatWireRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	path := req.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	t.log.add("request %s %s headers=%s body=%s", req.Method, path, compatWireHeaders(req.Header), body)
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body = &compatWireResponseBody{
		ReadCloser: resp.Body,
		done: func(body []byte) {
			t.log.add("response %s headers=%s body=%s", resp.Status, compatWireHeaders(resp.Header), body)
		},
	}
	return resp, nil
}

type compatWireResponseBody struct {
	io.ReadCloser
	once sync.Once
	buf  bytes.Buffer
	done func([]byte)
}

func (b *compatWireResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.buf.Write(p[:n])
	return n, err
}

func (b *compatWireResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() { b.done(b.buf.Bytes()) })
	return err
}

func compatWireHeaders(h http.Header) string {
	keys := make([]string, 0, len(h))
	for key := range h {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var parts []string
	for _, key := range keys {
		for _, value := range h[key] {
			if strings.EqualFold(key, "Date") {
				value = "<date>"
			}
			parts = append(parts, fmt.Sprintf("%s=%q", http.CanonicalHeaderKey(key), value))
		}
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func runCompatWireHTTP(ctx context.Context, t *testing.T, stateless bool) string {
	t.Helper()
	s := newCompatWireServer(t)
	handler := NewStreamableHTTPHandler(func(*http.Request) *Server { return s }, &StreamableHTTPOptions{Stateless: stateless, JSONResponse: true})
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()
	var log compatWireLog
	httpClient := &http.Client{Transport: &compatWireRoundTripper{base: http.DefaultTransport, log: &log}}
	c := newCompatWireClient(t)
	cs, err := c.Connect(ctx, &StreamableClientTransport{
		Endpoint:             httpServer.URL,
		HTTPClient:           httpClient,
		DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("client Connect: %v", err)
	}
	runCompatWireSession(ctx, t, cs)
	for _, notifications := range []string{`{}`, `{"taskIds":["task-1"]}`} {
		body := fmt.Sprintf(`{"jsonrpc":"2.0","id":9001,"method":"subscriptions/listen","params":{"_meta":{"io.modelcontextprotocol/clientCapabilities":{"roots":{"listChanged":true}},"io.modelcontextprotocol/clientInfo":{"name":"compat-client","version":"v1"},"io.modelcontextprotocol/protocolVersion":"2026-07-28"},"notifications":%s}}`, notifications)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, httpServer.URL, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set("Mcp-Protocol-Version", "2026-07-28")
		req.Header.Set("Mcp-Method", "subscriptions/listen")
		req.Header.Set("Mcp-Session-Id", "compat-session")
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("subscriptions/listen %s: %v", notifications, err)
		}
		_, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("subscriptions/listen body: read=%v close=%v", readErr, closeErr)
		}
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("client Close: %v", err)
	}
	return log.String()
}

func runCompatWireUnsolicitedTask(ctx context.Context, t *testing.T) string {
	t.Helper()
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var message struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(req.Body).Decode(&message); err != nil {
			t.Errorf("decoding raw request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch message.Method {
		case "server/discover":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"resultType":"complete","_meta":{"io.modelcontextprotocol/serverInfo":{"name":"raw-server","version":"v1"}},"ttlMs":0,"cacheScope":"public","supportedVersions":["2026-07-28"],"capabilities":{"tools":{}}}}`, message.ID)
		case "initialize":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"capabilities":{"tools":{}},"protocolVersion":"2026-07-28","serverInfo":{"name":"raw-server","version":"v1"}}}`, message.ID)
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/call":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"resultType":"task","taskId":"unsolicited","status":"working","createdAt":"2026-07-28T10:30:00Z","lastUpdatedAt":"2026-07-28T10:30:00Z","ttlMs":60000}}`, message.ID)
		default:
			t.Errorf("unexpected raw method %q", message.Method)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer httpServer.Close()
	var log compatWireLog
	httpClient := &http.Client{Transport: &compatWireRoundTripper{base: http.DefaultTransport, log: &log}}
	cs, err := NewClient(&Implementation{Name: "compat-client", Version: "v1"}, nil).Connect(ctx, &StreamableClientTransport{
		Endpoint:             httpServer.URL,
		HTTPClient:           httpClient,
		DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("raw client Connect: %v", err)
	}
	result, err := cs.CallTool(ctx, &CallToolParams{Name: "unsolicited"})
	if err != nil {
		t.Fatalf("CallTool(unsolicited): %v", err)
	}
	wire, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("re-marshalling unsolicited result: %v", err)
	}
	log.add("client re-marshal: %s", wire)
	if err := cs.Close(); err != nil {
		t.Fatalf("raw client Close: %v", err)
	}
	return log.String()
}
