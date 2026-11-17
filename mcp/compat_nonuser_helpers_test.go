// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// The compat_nonuser tests record what a program that does not use the tasks
// extension sees and sends: JSON for shared types, JSON-RPC messages in both
// directions, and HTTP headers and status codes, in the order they happened.
//
// They are in package mcp_test and use only API that the SDK exports without
// the extension, so the same files compile and run against the unchanged SDK:
// the commit the extension was added to. The transcripts the unchanged SDK
// produces are the reference; any difference is a compatibility break.
//
// Each transcript is compared with testdata/compat_nonuser/<name>.golden. A
// missing golden is a failure, and TestCompatNonUserGoldens checks that the
// directory holds exactly the goldens the tests compare.
//
// The goldens are generated on the unchanged SDK only, never on this tree.
// With BASE the commit before the extension:
//
//	mkdir /tmp/base && git archive BASE | tar -x -C /tmp/base
//	cp mcp/compat_nonuser_*_test.go /tmp/base/mcp/
//	(cd /tmp/base && MCP_NONUSER_WIRE_GENERATE=1 MCP_NONUSER_WIRE_OUT=/tmp/base-goldens \
//		go test -count=1 -timeout 10m -run '^TestCompatNonUser' ./mcp/)
//	diff -r /tmp/base-goldens mcp/testdata/compat_nonuser    # or copy, for a new scenario
//
// Writing needs both variables. MCP_NONUSER_WIRE_OUT alone fails every test,
// so a variable left in the environment cannot switch the comparison off.
//
// What this set cannot see: a break that needs a call that uses the
// extension in the same program (for example a plain call made inside an
// opted-in one). No program written for the unchanged SDK can make such a call; those cases are
// pinned by the task tests in package mcp.
//
// Nothing here uses time as a window: every wait is on a message, a channel
// or a closed stream. The context deadlines are hang guards only.

package mcp_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// nonUserHangGuard bounds one scenario. It is a hang guard, never a window.
const nonUserHangGuard = 2 * time.Minute

const (
	nonUserTasksExt  = "io.modelcontextprotocol/tasks"
	nonUserNewest    = "2026-07-28"
	nonUserLegacy    = "2025-11-25"
	nonUserNoteURI   = "fixture://note"
	nonUserCapsPlain = `{"roots":{"listChanged":true}}`
	nonUserCapsTasks = `{"roots":{"listChanged":true},"extensions":{"io.modelcontextprotocol/tasks":{}}}`
)

func nonUserContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), nonUserHangGuard)
	t.Cleanup(cancel)
	return ctx
}

// A wireLog is an ordered transcript. Lines are appended in the order the
// events happened; nothing is sorted.
type wireLog struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *wireLog) addf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(&l.b, format, args...)
	l.b.WriteByte('\n')
}

func (l *wireLog) section(format string, args ...any) {
	l.addf("== "+format+" ==", args...)
}

func (l *wireLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// nonUserGoldenDir holds the transcripts the unchanged SDK produces.
var nonUserGoldenDir = filepath.Join("testdata", "compat_nonuser")

// nonUserRun records, for this test binary, which goldens were compared and
// which of the transcript tests did the comparing. TestCompatNonUserGoldens
// reads it.
var nonUserRun = struct {
	mu      sync.Mutex
	goldens map[string]bool
	tests   map[string]bool
}{goldens: make(map[string]bool), tests: make(map[string]bool)}

// nonUserGenerating reports whether this run writes goldens instead of
// comparing with them. Writing needs both variables; either one alone is an
// error, so that a variable left in the environment cannot turn the
// comparison off.
func nonUserGenerating(t *testing.T) (dir string, generating bool) {
	t.Helper()
	dir, generate := os.Getenv("MCP_NONUSER_WIRE_OUT"), os.Getenv("MCP_NONUSER_WIRE_GENERATE")
	switch {
	case dir != "" && generate == "1":
		return dir, true
	case dir != "":
		t.Fatal("MCP_NONUSER_WIRE_OUT is set in the environment without the generate switch (MCP_NONUSER_WIRE_GENERATE=1): unset it to compare with the goldens")
	case generate != "":
		t.Fatal("MCP_NONUSER_WIRE_GENERATE is set in the environment without MCP_NONUSER_WIRE_OUT: unset it to compare with the goldens")
	}
	return "", false
}

// emitTranscript compares the transcript with its golden file, or, when
// generating, writes it.
func emitTranscript(t *testing.T, name string, log *wireLog) {
	t.Helper()
	got := log.String()
	if dir, generating := nonUserGenerating(t); generating {
		if err := os.MkdirAll(dir, 0o777); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name+".golden"), []byte(got), 0o666); err != nil {
			t.Fatal(err)
		}
		return
	}
	golden := filepath.Join(nonUserGoldenDir, name+".golden")
	want, err := os.ReadFile(golden)
	if errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("no golden file %s: the goldens are generated on the unchanged SDK, see the top of this file", golden)
	}
	if err != nil {
		t.Fatal(err)
	}
	test, _, _ := strings.Cut(t.Name(), "/")
	nonUserRun.mu.Lock()
	nonUserRun.goldens[name+".golden"] = true
	nonUserRun.tests[test] = true
	nonUserRun.mu.Unlock()
	if got == string(want) {
		return
	}
	gotLines, wantLines := strings.Split(got, "\n"), strings.Split(string(want), "\n")
	for i := 0; i < len(gotLines) || i < len(wantLines); i++ {
		var g, w string
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if g != w {
			t.Errorf("%s: transcript differs from the unchanged SDK, first at line %d\n--- unchanged SDK ---\n%s\n--- got ---\n%s", name, i+1, w, g)
			return
		}
	}
}

// fmtHeaders prints HTTP headers. A header map has no order, so the keys are
// printed in the order net/http writes them (sorted); values keep their order.
// Only the Date value is normalised.
func fmtHeaders(h http.Header) string {
	keys := make([]string, 0, len(h))
	for key := range h {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var parts []string
	for _, key := range keys {
		for _, value := range h[key] {
			if key == "Date" {
				value = "<date>"
			}
			parts = append(parts, fmt.Sprintf("%s=%q", key, value))
		}
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// A rawMsg is the part of a JSON-RPC message a script needs to answer it.
type rawMsg struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
	line   string
}

func (m rawMsg) isResponse() bool { return m.Method == "" && len(m.ID) > 0 }

// A rawPeer is the scripted end of a newline-delimited JSON connection. The
// SDK end is an [mcp.IOTransport]; the rawPeer reads and writes plain bytes
// and logs them.
type rawPeer struct {
	log     *wireLog
	fromSDK string // label for a line the SDK wrote
	toSDK   string // label for a line the script wrote
	lines   chan string
	w       *io.PipeWriter
	quiet   atomic.Bool // set once the test is over: report nothing more
	muted   atomic.Bool // see mute
	// served, if set before serve, is called after the replies to a message
	// are written and logged.
	served func(rawMsg)
}

// answered returns a channel that is closed once the peer, serving a script,
// has written its replies to the first message with the given method. It is
// called before serve.
func (p *rawPeer) answered(method string) <-chan struct{} {
	done := make(chan struct{})
	var once sync.Once
	previous := p.served
	p.served = func(msg rawMsg) {
		if previous != nil {
			previous(msg)
		}
		if msg.Method == method {
			once.Do(func() { close(done) })
		}
	}
	return done
}

// newRawPeer returns a rawPeer and the transport for the SDK end. sdkIsClient
// says which role the SDK end plays; with hop (for example "upstream ") it
// only picks the labels.
func newRawPeer(log *wireLog, sdkIsClient bool, hop string) (*rawPeer, mcp.Transport) {
	sdkWrites, peerWrites := hop+"s>c", hop+"c>s"
	if sdkIsClient {
		sdkWrites, peerWrites = hop+"c>s", hop+"s>c"
	}
	fromSDKr, fromSDKw := io.Pipe()
	toSDKr, toSDKw := io.Pipe()
	p := &rawPeer{log: log, fromSDK: sdkWrites, toSDK: peerWrites, lines: make(chan string, 4096), w: toSDKw}
	go func() {
		defer close(p.lines)
		r := bufio.NewReader(fromSDKr)
		for {
			line, err := r.ReadString('\n')
			if line != "" {
				p.lines <- strings.TrimSuffix(line, "\n")
			}
			if err != nil {
				return
			}
		}
	}()
	return p, &mcp.IOTransport{Reader: toSDKr, Writer: fromSDKw}
}

// mute stops the peer from logging. It is for the end of a session whose
// closing messages have no order in the unchanged SDK itself.
func (p *rawPeer) mute() { p.muted.Store(true) }

func (p *rawPeer) logf(format string, args ...any) {
	if !p.muted.Load() {
		p.log.addf(format, args...)
	}
}

// recv returns the next line the SDK wrote. ok is false once the SDK closed.
func (p *rawPeer) recv(ctx context.Context, t *testing.T) (msg rawMsg, ok bool) {
	t.Helper()
	select {
	case line, open := <-p.lines:
		if !open {
			p.logf("%s <closed>", p.fromSDK)
			return rawMsg{}, false
		}
		p.logf("%s %s", p.fromSDK, line)
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Errorf("undecodable line %q: %v", line, err)
		}
		msg.line = line
		return msg, true
	case <-ctx.Done():
		// Errorf, not Fatalf: recv also runs on serve's goroutine.
		if !p.quiet.Load() {
			p.logf("%s <hang: nothing arrived>", p.fromSDK)
			t.Errorf("hang guard: no message from the SDK end")
		}
		return rawMsg{}, false
	}
}

// recvResponse reads lines up to and including the response for id. A request
// the SDK end makes meanwhile (a server asking its client for roots, say) is
// answered: roots/list with no roots, ping with an empty result, anything
// else with method not found.
func (p *rawPeer) recvResponse(ctx context.Context, t *testing.T, id string) rawMsg {
	t.Helper()
	for {
		msg, ok := p.recv(ctx, t)
		if !ok {
			t.Fatalf("no response for id %s", id)
		}
		if msg.isResponse() && string(msg.ID) == id {
			return msg
		}
		if msg.Method != "" && len(msg.ID) > 0 {
			switch msg.Method {
			case "roots/list":
				p.send(t, `{"jsonrpc":"2.0","id":%s,"result":{"roots":[]}}`, msg.ID)
			case "ping":
				p.send(t, `{"jsonrpc":"2.0","id":%s,"result":{}}`, msg.ID)
			default:
				p.send(t, "%s", rawMethodNotFound(msg.ID, msg.Method))
			}
		}
	}
}

// send writes one line to the SDK end.
func (p *rawPeer) send(t *testing.T, format string, args ...any) {
	t.Helper()
	line := fmt.Sprintf(format, args...)
	p.logf("%s %s", p.toSDK, line)
	if _, err := io.WriteString(p.w, line+"\n"); err != nil {
		p.logf("%s <write failed: %v>", p.toSDK, err)
	}
}

// call sends a request and reads up to its response.
func (p *rawPeer) call(ctx context.Context, t *testing.T, id int, method, params string) rawMsg {
	t.Helper()
	if params == "" {
		p.send(t, `{"jsonrpc":"2.0","id":%d,"method":%q}`, id, method)
	} else {
		p.send(t, `{"jsonrpc":"2.0","id":%d,"method":%q,"params":%s}`, id, method, params)
	}
	return p.recvResponse(ctx, t, fmt.Sprint(id))
}

// closeWrite ends the script's side, which the SDK end reads as EOF.
func (p *rawPeer) closeWrite() {
	p.logf("%s <closed>", p.toSDK)
	p.w.Close()
}

// drain logs whatever the SDK still writes, until it closes its side.
func (p *rawPeer) drain(ctx context.Context, t *testing.T) {
	t.Helper()
	for {
		if _, ok := p.recv(ctx, t); !ok {
			return
		}
	}
}

// serve answers each message the SDK writes with the lines script returns,
// until the SDK closes. The returned channel is closed when serving ends. The
// test must close the SDK end before it returns.
func (p *rawPeer) serve(ctx context.Context, t *testing.T, script func(rawMsg) []string) <-chan struct{} {
	done := make(chan struct{})
	t.Cleanup(func() {
		if t.Failed() {
			p.quiet.Store(true)
			return
		}
		select {
		case <-done:
		case <-ctx.Done():
			p.quiet.Store(true)
			t.Errorf("the scripted peer is still serving: the SDK end never closed")
		}
	})
	go func() {
		defer close(done)
		for {
			msg, ok := p.recv(ctx, t)
			if !ok {
				return
			}
			for _, line := range script(msg) {
				p.send(t, "%s", line)
			}
			if p.served != nil {
				p.served(msg)
			}
		}
	}()
	return done
}

// waitFor blocks until ch is closed or receives, with the hang guard.
func waitFor[T any](ctx context.Context, t *testing.T, what string, ch <-chan T) (v T) {
	t.Helper()
	select {
	case v = <-ch:
		return v
	case <-ctx.Done():
		t.Fatalf("hang guard: %s", what)
		return v
	}
}

// rawDiscoverResult is a scripted server's answer to server/discover. The
// scripted server advertises the tasks extension: it is a peer that uses it.
func rawDiscoverResult(id json.RawMessage) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"resultType":"complete","_meta":{"io.modelcontextprotocol/serverInfo":{"name":"raw-server","version":"v1"}},"ttlMs":0,"cacheScope":"public","supportedVersions":["2026-07-28"],"capabilities":{"tools":{"listChanged":true},"resources":{"subscribe":true},"extensions":{"io.modelcontextprotocol/tasks":{}}}}}`, id)
}

func rawInitializeResult(id json.RawMessage) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"capabilities":{"tools":{"listChanged":true},"resources":{"subscribe":true},"extensions":{"io.modelcontextprotocol/tasks":{}}},"protocolVersion":"2025-11-25","serverInfo":{"name":"raw-server","version":"v1"}}}`, id)
}

func rawMethodNotFound(id json.RawMessage, method string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"method not found: %s"}}`, id, method)
}

// rawHandshake answers the connection handshake of an SDK client. legacy makes
// the scripted server refuse server/discover, so the client falls back to
// initialize. handled is false if msg is not part of the handshake.
func rawHandshake(msg rawMsg, legacy bool) (replies []string, handled bool) {
	switch msg.Method {
	case "server/discover":
		if legacy {
			return []string{rawMethodNotFound(msg.ID, msg.Method)}, true
		}
		return []string{rawDiscoverResult(msg.ID)}, true
	case "initialize":
		return []string{rawInitializeResult(msg.ID)}, true
	case "notifications/initialized":
		return nil, true
	}
	return nil, false
}

// newMeta is the _meta member of a 2026-07-28 request from a raw caller.
func newMeta(declareTasks bool) string { return newMetaVersion(nonUserNewest, declareTasks) }

// newMetaVersion is newMeta for another protocol version.
func newMetaVersion(version string, declareTasks bool) string {
	caps := nonUserCapsPlain
	if declareTasks {
		caps = nonUserCapsTasks
	}
	return `"_meta":{"io.modelcontextprotocol/clientCapabilities":` + caps +
		`,"io.modelcontextprotocol/clientInfo":{"name":"raw-caller","version":"v1"},"io.modelcontextprotocol/protocolVersion":"` + version + `"}`
}

// onMethod wraps script so that the returned channel is closed the first time
// the SDK end sends method, once that line is in the log.
func onMethod(script func(rawMsg) []string, method string) (func(rawMsg) []string, <-chan struct{}) {
	seen := make(chan struct{})
	var once sync.Once
	return func(msg rawMsg) []string {
		replies := script(msg)
		if msg.Method == method {
			once.Do(func() { close(seen) })
		}
		return replies
	}, seen
}

// legacyInitialize is the initialize request of a raw 2025-11-25 caller.
func legacyInitializeParams(version string, declareTasks bool) string {
	caps := `{}`
	if declareTasks {
		caps = `{"extensions":{"io.modelcontextprotocol/tasks":{}},"tasks":{"requests":{"tools":{"call":{}}}}}`
	}
	return fmt.Sprintf(`{"protocolVersion":%q,"capabilities":%s,"clientInfo":{"name":"raw-caller","version":"v1"}}`, version, caps)
}

// An httpProbe sends hand-written HTTP requests to an SDK server and logs the
// request and the response.
type httpProbe struct {
	log    *wireLog
	client *http.Client
	url    string
}

func (p *httpProbe) logf(format string, args ...any) { p.log.addf(format, args...) }

func newHTTPProbe(t *testing.T, log *wireLog, handler http.Handler) *httpProbe {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &httpProbe{log: log, client: srv.Client(), url: srv.URL}
}

// hdr builds the header list of a probe request from key, value pairs. An
// empty value leaves the header out.
func hdr(pairs ...string) http.Header {
	h := http.Header{
		"Content-Type": {"application/json"},
		"Accept":       {"application/json, text/event-stream"},
	}
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			h[pairs[i]] = []string{pairs[i+1]}
		}
	}
	return h
}

func (p *httpProbe) request(ctx context.Context, t *testing.T, method string, h http.Header, body string) *http.Response {
	t.Helper()
	var rd io.Reader
	if body != "" || method == http.MethodPost {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.url, rd)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range h {
		req.Header[k] = v
	}
	p.logf("request %s headers=%s body=%s", method, fmtHeaders(h), body)
	resp, err := p.client.Do(req)
	if err != nil {
		p.logf("response <transport error: %v>", err)
		t.Fatalf("%s: %v", method, err)
	}
	return resp
}

// do sends a request and reads the whole response. The server must end the
// response by itself; a stream that stays open is a hang.
func (p *httpProbe) do(ctx context.Context, t *testing.T, method string, h http.Header, body string) (int, http.Header, string) {
	t.Helper()
	resp := p.request(ctx, t, method, h, body)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		p.logf("response %s headers=%s body=%s <read error: %v>", resp.Status, fmtHeaders(resp.Header), data, err)
		t.Fatalf("reading response: %v", err)
	}
	p.logf("response %s headers=%s body=%s", resp.Status, fmtHeaders(resp.Header), data)
	return resp.StatusCode, resp.Header, string(data)
}

func (p *httpProbe) post(ctx context.Context, t *testing.T, h http.Header, body string) (int, http.Header, string) {
	t.Helper()
	return p.do(ctx, t, http.MethodPost, h, body)
}

// An httpStream is a response that the server keeps open.
type httpStream struct {
	log    *wireLog
	resp   *http.Response
	r      *bufio.Reader
	cancel context.CancelFunc
}

// open sends a request whose response is a server-sent event stream that the
// server keeps open. The caller reads events and then closes it.
func (p *httpProbe) open(ctx context.Context, t *testing.T, method string, h http.Header, body string) *httpStream {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	resp := p.request(ctx, t, method, h, body)
	p.logf("response %s headers=%s body follows as events", resp.Status, fmtHeaders(resp.Header))
	return &httpStream{log: p.log, resp: resp, r: bufio.NewReader(resp.Body), cancel: cancel}
}

// event reads one server-sent event. ok is false at the end of the stream.
func (s *httpStream) event(t *testing.T) (ok bool) {
	t.Helper()
	var lines []string
	for {
		line, err := s.r.ReadString('\n')
		if err != nil {
			if len(lines) > 0 || line != "" {
				s.log.addf("  event <cut short> %q", strings.Join(append(lines, line), "\n"))
			}
			s.log.addf("  <end of stream>")
			return false
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			s.log.addf("  got %s", strings.Join(lines, " | "))
			return true
		}
		lines = append(lines, line)
	}
}

// close abandons the stream, as a client that stops listening does.
func (s *httpStream) close() {
	s.log.addf("  <client closes the stream>")
	s.cancel()
	s.resp.Body.Close()
}

// A rawHTTPServer is a scripted HTTP server for an SDK client. It logs every
// request with all of its headers, and the response the script gives.
type rawHTTPServer struct {
	log   *wireLog
	url   string
	muted atomic.Bool
}

// mute stops the server from logging; see [rawPeer.mute].
func (s *rawHTTPServer) mute() { s.muted.Store(true) }

func (s *rawHTTPServer) logf(format string, args ...any) {
	if !s.muted.Load() {
		s.log.addf(format, args...)
	}
}

// A rawHTTPReply is a scripted HTTP response.
type rawHTTPReply struct {
	status int
	header http.Header
	body   string
	// hold, if non-nil, keeps the response open after body is flushed until
	// the client goes away; then it is closed.
	hold chan struct{}
	// after, if non-nil, runs once the request and the response are logged.
	after func()
}

func jsonReply(body string) rawHTTPReply {
	return rawHTTPReply{status: http.StatusOK, header: http.Header{"Content-Type": {"application/json"}}, body: body}
}

func sseReply(messages ...string) rawHTTPReply {
	var b strings.Builder
	for _, m := range messages {
		fmt.Fprintf(&b, "event: message\ndata: %s\n\n", m)
	}
	return rawHTTPReply{status: http.StatusOK, header: http.Header{"Content-Type": {"text/event-stream"}}, body: b.String()}
}

func newRawHTTPServer(t *testing.T, log *wireLog, script func(req *http.Request, msg rawMsg) rawHTTPReply) *rawHTTPServer {
	t.Helper()
	s := &rawHTTPServer{log: log}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		s.logf("request %s headers=%s body=%s", req.Method, fmtHeaders(req.Header), body)
		var msg rawMsg
		if len(bytes.TrimSpace(body)) > 0 {
			if err := json.Unmarshal(body, &msg); err != nil {
				t.Errorf("undecodable request body %q: %v", body, err)
			}
		}
		reply := script(req, msg)
		for k, v := range reply.header {
			w.Header()[k] = v
		}
		// Logged before it is written, so that whatever the client does next
		// comes after it in the transcript.
		s.logf("response %d headers=%s body=%q", reply.status, fmtHeaders(reply.header), reply.body)
		if reply.after != nil {
			reply.after()
		}
		w.WriteHeader(reply.status)
		io.WriteString(w, reply.body)
		if reply.hold != nil {
			// Not logged here: the client may also be posting a cancellation,
			// and the two have no order. The test logs it once it has both.
			w.(http.Flusher).Flush()
			<-req.Context().Done()
			close(reply.hold)
		}
	}))
	t.Cleanup(srv.Close)
	s.url = srv.URL
	return s
}

// rawHTTPHandshake answers the handshake of an SDK client over HTTP.
func rawHTTPHandshake(req *http.Request, msg rawMsg) (rawHTTPReply, bool) {
	if req.Method == http.MethodDelete {
		return rawHTTPReply{status: http.StatusNoContent}, true
	}
	if req.Method == http.MethodGet {
		return rawHTTPReply{status: http.StatusMethodNotAllowed}, true
	}
	switch msg.Method {
	case "server/discover":
		return jsonReply(rawDiscoverResult(msg.ID)), true
	case "initialize":
		r := jsonReply(rawInitializeResult(msg.ID))
		r.header["Mcp-Session-Id"] = []string{"raw-session"}
		return r, true
	case "notifications/initialized", "notifications/cancelled":
		return rawHTTPReply{status: http.StatusAccepted}, true
	}
	return rawHTTPReply{}, false
}

// guarded runs f and reports a panic as text instead of ending the test binary.
func guarded(f func() string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("panic=%v", r)
		}
	}()
	return f()
}

// describeToolResult prints everything a caller can see of a CallTool return.
func describeToolResult(res *mcp.CallToolResult, err error) string {
	if err != nil {
		return fmt.Sprintf("err=%q", err.Error())
	}
	if res == nil {
		return "result=<nil>"
	}
	data, merr := json.Marshal(res)
	out := fmt.Sprintf("marshal=%s needsInput=%v isError=%v content=%d", data, res.NeedsInput(), res.IsError, len(res.Content))
	if merr != nil {
		out += fmt.Sprintf(" marshalErr=%q", merr.Error())
	}
	return out
}

// metaDeclaresTasks reports whether a request's _meta declares the tasks
// extension, the way a program written for the unchanged SDK would look for it.
func metaDeclaresTasks(meta mcp.Meta) bool {
	caps, _ := meta[mcp.MetaKeyClientCapabilities].(map[string]any)
	exts, _ := caps["extensions"].(map[string]any)
	_, ok := exts[nonUserTasksExt]
	return ok
}
