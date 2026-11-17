// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// rawTaskHTTP speaks Streamable HTTP to a server by hand, one POST per
// JSON-RPC message. Every request is bounded by the test context.
type rawTaskHTTP struct {
	t   *testing.T
	ctx context.Context
	url string
}

func newRawTaskHTTP(t *testing.T, server *Server, options *StreamableHTTPOptions) *rawTaskHTTP {
	t.Helper()
	handler := NewStreamableHTTPHandler(func(*http.Request) *Server { return server }, options)
	httpServer := httptest.NewServer(handler)
	// Open listen streams end with their request contexts, which the test
	// context bounds; streams are cancelled before the server closes.
	closeTaskHTTPServer(t, httpServer)
	return &rawTaskHTTP{t: t, ctx: taskTestContext(t), url: httpServer.URL}
}

// rawTaskStream is the response to one POST: the HTTP status and the JSON-RPC
// messages of the body, in order, as they arrive.
type rawTaskStream struct {
	t        *testing.T
	status   int
	header   http.Header
	messages chan string // closed at the end of the body
	cancel   context.CancelFunc
}

// open sends one JSON-RPC body and returns the response without waiting for
// its end. Mcp-Method and the protocol version header are set from the body
// and the 2026-07-28 version; a header value of "-" removes a header.
func (p *rawTaskHTTP) open(body string, headers map[string]string) *rawTaskStream {
	p.t.Helper()
	ctx, cancel := context.WithCancel(p.ctx)
	p.t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, strings.NewReader(body))
	if err != nil {
		p.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set(protocolVersionHeader, protocolVersion20260728)
	var message struct {
		Method string `json:"method"`
	}
	_ = json.Unmarshal([]byte(body), &message)
	req.Header.Set(methodHeader, message.Method)
	for name, value := range headers {
		if value == "-" {
			req.Header.Del(name)
		} else {
			req.Header.Set(name, value)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		p.t.Fatalf("POST %s: %v", message.Method, err)
	}
	stream := &rawTaskStream{t: p.t, status: resp.StatusCode, header: resp.Header, messages: make(chan string, 256), cancel: cancel}
	go func() {
		defer close(stream.messages)
		defer resp.Body.Close()
		if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
			data, _ := io.ReadAll(resp.Body)
			if text := strings.TrimSpace(string(data)); text != "" {
				stream.messages <- text
			}
			return
		}
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 1<<20), 1<<20)
		for scanner.Scan() {
			if data, ok := strings.CutPrefix(scanner.Text(), "data: "); ok && data != "" {
				stream.messages <- data
			}
		}
	}()
	return stream
}

// next returns the next message of the stream. The end of the stream is a
// failure.
func (s *rawTaskStream) next(waiting string) string {
	s.t.Helper()
	message, ok := waitTaskOK(s.t, s.messages, waiting)
	if !ok {
		s.t.Fatalf("the stream ended while waiting for %s", waiting)
	}
	return message
}

// rest returns the remaining messages, up to the end of the stream.
func (s *rawTaskStream) rest() []string {
	s.t.Helper()
	var messages []string
	for {
		message, ok := waitTaskOK(s.t, s.messages, "the end of the response")
		if !ok {
			return messages
		}
		messages = append(messages, message)
	}
}

// post sends one JSON-RPC body and returns the status and the whole answer.
func (p *rawTaskHTTP) post(body string, headers map[string]string) (int, []string) {
	p.t.Helper()
	stream := p.open(body, headers)
	return stream.status, stream.rest()
}

// rawTaskRequest returns a 2026-07-28 request body. params are the members of
// the params object besides _meta; declare adds the tasks extension to the
// request's client capabilities.
func rawTaskRequest(id int, method, params string, declare bool) string {
	meta := rawTaskMeta
	if declare {
		meta = rawTaskMetaDeclaring
	}
	if params != "" {
		params += ","
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":%q,"params":{%s%s}}`, id, method, params, meta)
}

// rawTaskMessage is the part of a JSON-RPC message the tests look at.
type rawTaskMessage struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int64           `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	} `json:"error"`
}

func decodeRawTaskMessage(t *testing.T, message string) rawTaskMessage {
	t.Helper()
	var decoded rawTaskMessage
	if err := json.Unmarshal([]byte(message), &decoded); err != nil {
		t.Fatalf("decoding %s: %v", message, err)
	}
	return decoded
}

// rawTaskErrorCode returns the error code of a one-message answer, or 0.
func rawTaskErrorCode(t *testing.T, messages []string) int64 {
	t.Helper()
	if len(messages) != 1 {
		t.Fatalf("answer has %d messages, want one: %v", len(messages), messages)
	}
	if decoded := decodeRawTaskMessage(t, messages[0]); decoded.Error != nil {
		return decoded.Error.Code
	}
	return 0
}

// The order in which a server with the extension refuses a tasks/get,
// tasks/update or tasks/cancel request over Streamable HTTP: the standard
// Mcp-Method check, then the missing declaration whatever the rest of the
// request looks like, then Mcp-Name for a declaring request whose taskId is a
// string, then the params, then the id. Every refusal is the answer to the
// request: it carries the request's id.
func TestTaskHTTPErrorPrecedence(t *testing.T) {
	server := NewServer(testImpl, nil)
	if _, err := NewTaskServer(server, nil); err != nil {
		t.Fatal(err)
	}
	peer := newRawTaskHTTP(t, server, &StreamableHTTPOptions{Stateless: true})
	tests := []struct {
		name     string
		declare  bool
		headers  map[string]string
		params   string
		wantCode int64
	}{
		{"not declaring, Mcp-Method differs", false, map[string]string{methodHeader: "tools/list", nameHeader: "unknown"}, `"taskId":"unknown"`, CodeHeaderMismatch},
		{"not declaring, Mcp-Method missing", false, map[string]string{methodHeader: "-", nameHeader: "unknown"}, `"taskId":"unknown"`, CodeHeaderMismatch},
		{"declaring, Mcp-Method differs", true, map[string]string{methodHeader: "tools/list", nameHeader: "unknown"}, `"taskId":"unknown"`, CodeHeaderMismatch},
		{"not declaring, no Mcp-Name", false, nil, `"taskId":"unknown"`, CodeMissingRequiredClientCapabilities},
		{"not declaring, Mcp-Name matches", false, map[string]string{nameHeader: "unknown"}, `"taskId":"unknown"`, CodeMissingRequiredClientCapabilities},
		{"not declaring, Mcp-Name differs", false, map[string]string{nameHeader: "other"}, `"taskId":"unknown"`, CodeMissingRequiredClientCapabilities},
		{"not declaring, taskId is a number", false, map[string]string{nameHeader: "7"}, `"taskId":7`, CodeMissingRequiredClientCapabilities},
		{"not declaring, taskId is a number, no Mcp-Name", false, nil, `"taskId":7`, CodeMissingRequiredClientCapabilities},
		{"not declaring, no taskId", false, nil, `"other":1`, CodeMissingRequiredClientCapabilities},
		{"declaring, no Mcp-Name", true, nil, `"taskId":"unknown"`, CodeHeaderMismatch},
		{"declaring, Mcp-Name differs", true, map[string]string{nameHeader: "other"}, `"taskId":"unknown"`, CodeHeaderMismatch},
		{"declaring, taskId is a number", true, map[string]string{nameHeader: "7"}, `"taskId":7`, jsonrpc2CodeInvalidParams},
		{"declaring, taskId is a number, no Mcp-Name", true, nil, `"taskId":7`, jsonrpc2CodeInvalidParams},
		{"declaring, no taskId, no Mcp-Name", true, nil, `"other":1`, jsonrpc2CodeInvalidParams},
		{"declaring, headers right, unknown id", true, map[string]string{nameHeader: "unknown"}, `"taskId":"unknown"`, jsonrpc2CodeInvalidParams},
	}
	for _, method := range []string{methodGetTask, methodUpdateTask, methodCancelTask} {
		for _, test := range tests {
			t.Run(method+"/"+test.name, func(t *testing.T) {
				status, messages := peer.post(rawTaskRequest(7, method, test.params, test.declare), test.headers)
				if code := rawTaskErrorCode(t, messages); status != http.StatusBadRequest || code != test.wantCode {
					t.Errorf("HTTP %d, error code %d; want 400, %d: %v", status, code, test.wantCode, messages)
				}
				if id := string(decodeRawTaskMessage(t, messages[0]).ID); id != "7" {
					t.Errorf("the answer has id %s, want the request's id 7: %v", id, messages)
				}
			})
		}
	}
}

const jsonrpc2CodeInvalidParams = -32602

// The same order without a header layer.
func TestTaskMalformedParamsPrecedence(t *testing.T) {
	server := NewServer(testImpl, nil)
	if _, err := NewTaskServer(server, nil); err != nil {
		t.Fatal(err)
	}
	peer := newRawTaskPeer(t, server)
	for _, method := range []string{methodGetTask, methodUpdateTask, methodCancelTask} {
		for _, test := range []struct {
			params   string
			meta     string
			wantCode int64
		}{
			{`"taskId":7`, rawTaskMeta, CodeMissingRequiredClientCapabilities},
			{`"taskId":7`, rawTaskMetaDeclaring, jsonrpc2CodeInvalidParams},
			{`"taskId":""`, rawTaskMeta, CodeMissingRequiredClientCapabilities},
			{`"taskId":""`, rawTaskMetaDeclaring, jsonrpc2CodeInvalidParams},
			{`"taskId":"unknown"`, rawTaskMeta, CodeMissingRequiredClientCapabilities},
			{`"taskId":"unknown"`, rawTaskMetaDeclaring, jsonrpc2CodeInvalidParams},
		} {
			peer.request(method, "{"+test.params+","+test.meta+"}")
			answer := peer.read("the answer to " + method)
			decoded := decodeRawTaskMessage(t, answer)
			if decoded.Error == nil || decoded.Error.Code != test.wantCode {
				t.Errorf("%s {%s} declaring=%t: %s; want error %d", method, test.params, test.meta == rawTaskMetaDeclaring, answer, test.wantCode)
			}
		}
	}
	// A request whose _meta cannot be read declares nothing.
	peer.request(methodGetTask, `{"taskId":"unknown","_meta":7}`)
	if answer := peer.read("the answer to a request with a malformed _meta"); !strings.Contains(answer, `"code":-32021`) {
		t.Errorf("tasks/get with _meta 7: %s; want the missing-capability error", answer)
	}
}

// A server without the extension checks no task header: its own tasks/*
// custom methods are served with or without Mcp-Name, whatever the request
// declares.
func TestTaskHeadersNeedTaskServer(t *testing.T) {
	server := NewServer(testImpl, nil)
	for _, method := range []string{methodGetTask, methodUpdateTask, methodCancelTask} {
		if err := addInstallTestMethod(server, method); err != nil {
			t.Fatal(err)
		}
	}
	peer := newRawTaskHTTP(t, server, &StreamableHTTPOptions{Stateless: true})
	for _, method := range []string{methodGetTask, methodUpdateTask, methodCancelTask} {
		for _, declare := range []bool{false, true} {
			for _, headers := range []map[string]string{nil, {nameHeader: "other"}, {nameHeader: "own-task"}} {
				status, messages := peer.post(rawTaskRequest(1, method, `"taskId":"own-task"`, declare), headers)
				if code := rawTaskErrorCode(t, messages); status != http.StatusOK || code != 0 {
					t.Errorf("%s declaring=%t headers %v: HTTP %d %v; want the custom method's answer", method, declare, headers, status, messages)
				}
			}
		}
	}
}

func TestTaskServerValidateHeaders(t *testing.T) {
	server := NewServer(testImpl, nil)
	tasks, err := NewTaskServer(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	const declaring = `{"taskId":"task-1",` + rawTaskMetaDeclaring + `}`
	tests := []struct {
		name    string
		version string
		method  string
		params  string
		header  string // Mcp-Name
		wantErr bool
	}{
		{"matching name", protocolVersion20260728, methodGetTask, declaring, "task-1", false},
		{"missing name", protocolVersion20260728, methodGetTask, declaring, "", true},
		{"other name", protocolVersion20260728, methodUpdateTask, declaring, "task-2", true},
		{"missing name, cancel", protocolVersion20260728, methodCancelTask, declaring, "", true},
		{"older protocol header", protocolVersion20251125, methodGetTask, declaring, "", false},
		{"no protocol header", "", methodGetTask, declaring, "", false},
		{"not declaring", protocolVersion20260728, methodGetTask, `{"taskId":"task-1",` + rawTaskMeta + `}`, "", false},
		{"taskId not a string", protocolVersion20260728, methodGetTask, `{"taskId":7,` + rawTaskMetaDeclaring + `}`, "", false},
		{"no taskId", protocolVersion20260728, methodGetTask, `{` + rawTaskMetaDeclaring + `}`, "", false},
		{"empty taskId", protocolVersion20260728, methodGetTask, `{"taskId":"",` + rawTaskMetaDeclaring + `}`, "", false},
		{"params not an object", protocolVersion20260728, methodGetTask, `7`, "", false},
		{"another method", protocolVersion20260728, "tasks/list", declaring, "", false},
		{"tools/call", protocolVersion20260728, methodCallTool, declaring, "", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header := make(http.Header)
			if test.version != "" {
				header.Set(protocolVersionHeader, test.version)
			}
			if test.header != "" {
				header.Set(nameHeader, test.header)
			}
			err := tasks.validateHeaders(header, &jsonrpc.Request{Method: test.method, Params: json.RawMessage(test.params)})
			if (err != nil) != test.wantErr {
				t.Errorf("validateHeaders = %v, want an error: %t", err, test.wantErr)
			}
		})
	}
	// Not a request: nothing to check.
	header := http.Header{protocolVersionHeader: {protocolVersion20260728}}
	if err := tasks.validateHeaders(header, &jsonrpc.Response{}); err != nil {
		t.Errorf("validateHeaders on a response = %v", err)
	}
}

// The Mcp-Name of a task request is read as the Mcp-Name of a tool, a prompt
// or a resource is: sent once, and Base64-wrapped when the id is not
// header-safe.
func TestTaskServerValidateHeadersEncodedName(t *testing.T) {
	server := NewServer(testImpl, nil)
	tasks, err := NewTaskServer(server, nil)
	if err != nil {
		t.Fatal(err)
	}
	const id = "tâche 1 "
	params := fmt.Sprintf(`{"taskId":%q,%s}`, id, rawTaskMetaDeclaring)
	for _, test := range []struct {
		name    string
		id      string
		names   []string // Mcp-Name field lines
		wantErr bool
	}{
		{"wrapped name", id, []string{encodeBase64(id)}, false},
		{"wrapped name of another task", id, []string{encodeBase64("tâche 2 ")}, true},
		{"invalid Base64", id, []string{base64Prefix + "!" + base64Suffix}, true},
		{"wrapped header-safe name", "task-1", []string{encodeBase64("task-1")}, false},
		{"name sent twice", "task-1", []string{"task-1", "task-1"}, true},
		{"empty name", "task-1", []string{""}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := http.Header{protocolVersionHeader: {protocolVersion20260728}, nameHeader: test.names}
			body := params
			if test.id != id {
				body = fmt.Sprintf(`{"taskId":%q,%s}`, test.id, rawTaskMetaDeclaring)
			}
			err := tasks.validateHeaders(header, &jsonrpc.Request{Method: methodGetTask, Params: json.RawMessage(body)})
			if (err != nil) != test.wantErr {
				t.Errorf("validateHeaders = %v, want an error: %t", err, test.wantErr)
			}
		})
	}
}

// The client writes Mcp-Name for a task request from the context its terminal
// made, and the shared validation asks for no such header.
func TestTaskStreamableRoutingHeaders(t *testing.T) {
	for _, method := range []string{methodGetTask, methodUpdateTask, methodCancelTask} {
		t.Run(method, func(t *testing.T) {
			params, err := json.Marshal(&GetTaskParams{TaskID: "task-1", ParamsBase: ParamsBase{Meta: tasksRequestMeta()}})
			if err != nil {
				t.Fatal(err)
			}
			message := &jsonrpc.Request{Method: method, Params: params}
			header := make(http.Header)
			header.Set(protocolVersionHeader, protocolVersion20260728)
			setStandardHeaders(withTaskName(t.Context(), method, "task-1"), header, message)
			if got := header.Get(methodHeader); got != method {
				t.Fatalf("Mcp-Method = %q, want %q", got, method)
			}
			if got := header.Get(nameHeader); got != "task-1" {
				t.Fatalf("Mcp-Name = %q, want task-1", got)
			}
			header.Del(nameHeader)
			if err := validateMcpHeaders(header, message, nil); err != nil {
				t.Fatalf("the shared header validation refuses a task request without Mcp-Name: %v", err)
			}
		})
	}
}
