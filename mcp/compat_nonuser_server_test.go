// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// probeParams and probeResult are the types of a program's own
// tasks/get, tasks/update and tasks/cancel methods. The result says what the
// handler saw, so the transcript shows that the handler ran and with what.
type probeParams struct {
	mcp.ParamsBase
	TaskID json.RawMessage `json:"taskId,omitempty"`
	Value  string          `json:"value,omitempty"`
}

type probeResult struct {
	mcp.ResultBase
	Handler  string          `json:"handler"`
	TaskID   json.RawMessage `json:"sawTaskId,omitempty"`
	Value    string          `json:"value,omitempty"`
	Declared bool            `json:"sawTasksDeclared"`
}

var probeMethods = []string{"tasks/get", "tasks/update", "tasks/cancel"}

// A plainServer is a server with no tasks extension installed.
type plainServer struct {
	*mcp.Server
	subscribed   chan string
	unsubscribed chan string
}

type plainServerConfig struct {
	// custom registers the program's own tasks/get, tasks/update,
	// tasks/cancel and acme/echo methods.
	custom bool
	// ownExtension makes the program advertise the tasks extension by
	// itself, as a program written for the unchanged SDK with its own implementation would.
	ownExtension bool
	sessionIDs   func() string
}

func newPlainServer(t *testing.T, cfg plainServerConfig) *plainServer {
	t.Helper()
	ps := &plainServer{subscribed: make(chan string, 64), unsubscribed: make(chan string, 64)}
	opts := &mcp.ServerOptions{
		GetSessionID: cfg.sessionIDs,
		SubscribeHandler: func(_ context.Context, req *mcp.SubscribeRequest) error {
			ps.subscribed <- req.Params.URI
			return nil
		},
		UnsubscribeHandler: func(_ context.Context, req *mcp.UnsubscribeRequest) error {
			ps.unsubscribed <- req.Params.URI
			return nil
		},
	}
	if cfg.ownExtension {
		opts.Capabilities = &mcp.ServerCapabilities{}
		opts.Capabilities.AddExtension(nonUserTasksExt, nil)
	}
	s := mcp.NewServer(&mcp.Implementation{Name: "plain-server", Version: "v1"}, opts)
	schema := json.RawMessage(`{"type":"object"}`)
	s.AddTool(&mcp.Tool{Name: "echo", InputSchema: schema},
		func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "echo " + marshalValue(req.Params)}}}, nil
		})
	s.AddTool(&mcp.Tool{Name: "fail", InputSchema: schema},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return nil, errors.New("tool failed")
		})
	s.AddResource(&mcp.Resource{URI: nonUserNoteURI, Name: "note"},
		func(context.Context, *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{URI: nonUserNoteURI, Text: "note"}}}, nil
		})
	if cfg.custom {
		for _, method := range append([]string{"acme/echo"}, probeMethods...) {
			if err := mcp.AddReceivingCustomMethod(s, method,
				func(_ context.Context, _ *mcp.ServerSession, p *probeParams) (*probeResult, error) {
					return &probeResult{Handler: method, TaskID: p.TaskID, Value: p.Value, Declared: metaDeclaresTasks(p.Meta)}, nil
				}); err != nil {
				t.Fatalf("AddReceivingCustomMethod(%q): %v", method, err)
			}
		}
	}
	ps.Server = s
	return ps
}

// A wireVersion is how a raw request states its protocol version.
type wireVersion struct {
	label  string
	header string // Mcp-Protocol-Version, or "" for none
	meta   string // _meta protocolVersion, or "" for none
}

var statelessWireVersions = []wireVersion{
	{"2026-07-28", "2026-07-28", "2026-07-28"},
	{"2026-07-28-header-only", "2026-07-28", ""},
	{"2026-07-28-meta-only", "", "2026-07-28"},
	{"2025-11-25", "2025-11-25", ""},
	{"2025-06-18", "2025-06-18", ""},
	{"2025-03-26", "2025-03-26", ""},
	{"2024-11-05", "2024-11-05", ""},
	{"no-version", "", ""},
	{"2099-01-01", "2099-01-01", "2099-01-01"},
}

// A session negotiated 2025-11-25; these are what later requests may say.
var statefulWireVersions = []wireVersion{
	{"2025-11-25", "2025-11-25", ""},
	{"no-version", "", ""},
	{"2025-06-18", "2025-06-18", ""},
	{"2026-07-28", "2026-07-28", "2026-07-28"},
}

// A headerShape is which of Mcp-Method and Mcp-Name a raw request carries.
type headerShape struct {
	label        string
	method, name string // "" for none, "=" for the true value, else literal
}

var headerShapes = []headerShape{
	{"no-mcp-headers", "", ""},
	{"method", "=", ""},
	{"method+name", "=", "="},
	{"method+wrong-name", "=", "another-task"},
	{"wrong-method", "tools/list", ""},
	{"name-only", "", "="},
}

func (hs headerShape) headers(v wireVersion, method, name, sessionID string) http.Header {
	pick := func(shape, truth string) string {
		if shape == "=" {
			return truth
		}
		return shape
	}
	return hdr("Mcp-Protocol-Version", v.header, "Mcp-Session-Id", sessionID,
		"Mcp-Method", pick(hs.method, method), "Mcp-Name", pick(hs.name, name))
}

// taskParams are the params of a raw tasks/* request.
func taskParams(v wireVersion, declare bool, taskID string) string {
	var members []string
	switch {
	case v.meta != "":
		members = append(members, newMetaVersion(v.meta, declare))
	case declare:
		members = append(members, `"_meta":{"io.modelcontextprotocol/clientCapabilities":`+nonUserCapsTasks+`}`)
	}
	if taskID != "" {
		members = append(members, `"taskId":`+taskID)
	}
	members = append(members, `"value":"v"`)
	return "{" + strings.Join(members, ",") + "}"
}

// TestCompatNonUserServerHTTPMatrix sends tasks/get, tasks/update and
// tasks/cancel to servers that have no tasks extension, over Streamable HTTP,
// for every combination of protocol version, MCP headers, declared
// capability and taskId form: to the program's own methods of those names,
// and to a server that has none.
func TestCompatNonUserServerHTTPMatrix(t *testing.T) {
	taskIDs := []struct{ label, json, name string }{
		{"string", `"t-1"`, "t-1"},
		{"number", `7`, "7"},
		{"absent", ``, "t-1"},
	}
	for _, stateless := range []bool{true, false} {
		name, versions := "server-http-matrix-stateful", statefulWireVersions
		if stateless {
			name, versions = "server-http-matrix-stateless", statelessWireVersions
		}
		t.Run(name, func(t *testing.T) {
			ctx := nonUserContext(t)
			var log wireLog
			id := 0
			next := func() int { id++; return id }
			for _, custom := range []bool{true, false} {
				sessionID := ""
				if !stateless {
					sessionID = "matrix-session"
				}
				server := newPlainServer(t, plainServerConfig{custom: custom, sessionIDs: func() string { return sessionID }})
				probe := newHTTPProbe(t, &log, mcp.NewStreamableHTTPHandler(
					func(*http.Request) *mcp.Server { return server.Server },
					&mcp.StreamableHTTPOptions{Stateless: stateless, JSONResponse: true}))
				methods, ids := probeMethods, taskIDs
				if custom {
					log.section("server with its own tasks/get, tasks/update, tasks/cancel")
				} else {
					log.section("server without any tasks/* method")
					methods, ids = []string{"tasks/get", "tasks/list", "tasks/result"}, taskIDs[:1]
				}
				if !stateless {
					probe.post(ctx, t, hdr(), fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"initialize","params":%s}`, next(), legacyInitializeParams(nonUserLegacy, false)))
					probe.post(ctx, t, hdr("Mcp-Protocol-Version", nonUserLegacy, "Mcp-Session-Id", sessionID), `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
				}
				for _, v := range versions {
					for _, method := range methods {
						for _, shape := range headerShapes {
							for _, declare := range []bool{false, true} {
								for _, taskID := range ids {
									log.addf("-- %s %s %s declare=%v taskId=%s", v.label, method, shape.label, declare, taskID.label)
									probe.post(ctx, t, shape.headers(v, method, taskID.name, sessionID),
										fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":%q,"params":%s}`, next(), method, taskParams(v, declare, taskID.json)))
								}
							}
						}
					}
				}
				log.section("batches")
				for _, v := range versions[:2] {
					for _, shape := range headerShapes[:3] {
						for _, declare := range []bool{false, true} {
							log.addf("-- batch %s %s declare=%v", v.label, shape.label, declare)
							params := taskParams(v, declare, `"t-1"`)
							// One call and one notification: the server answers the
							// calls of a batch in no fixed order, in the unchanged SDK too.
							probe.post(ctx, t, shape.headers(v, "tasks/get", "t-1", sessionID),
								fmt.Sprintf(`[{"jsonrpc":"2.0","id":%d,"method":"tasks/get","params":%s},{"jsonrpc":"2.0","method":"tasks/cancel","params":%s}]`, next(), params, params))
						}
					}
				}
				log.section("tasks/get as a notification")
				for _, v := range versions[:2] {
					probe.post(ctx, t, headerShapes[1].headers(v, "tasks/get", "t-1", sessionID),
						fmt.Sprintf(`{"jsonrpc":"2.0","method":"tasks/get","params":%s}`, taskParams(v, true, `"t-1"`)))
				}
				if !stateless {
					probe.do(ctx, t, http.MethodDelete, http.Header{"Mcp-Session-Id": {sessionID}, "Mcp-Protocol-Version": {nonUserLegacy}}, "")
				}
			}
			emitTranscript(t, name, &log)
		})
	}
}

// listenCases are subscriptions/listen notifications objects that name tasks.
// On a server without the extension every one of them leaves no subscription
// open, so the server answers and ends the response by itself.
var listenCases = []string{
	`{}`,
	`{"taskIds":["t-1"]}`,
	`{"taskIds":["t-1","t-2"],"toolsListChanged":false}`,
	`{"taskIds":[]}`,
	`{"taskIds":null}`,
	`{"taskIds":"t-1"}`,
	`{"taskIds":[7]}`,
}

// TestCompatNonUserServerHTTP sends the requests a tasks-aware client makes to
// a server that has no tasks extension, over stateless Streamable HTTP.
func TestCompatNonUserServerHTTP(t *testing.T) {
	for _, tc := range []struct {
		name         string
		jsonResponse bool
		ownExtension bool
	}{
		{"server-http-stateless-json", true, false},
		{"server-http-stateless-sse", false, false},
		{"server-http-stateless-own-extension", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := nonUserContext(t)
			var log wireLog
			server := newPlainServer(t, plainServerConfig{custom: true, ownExtension: tc.ownExtension})
			probe := newHTTPProbe(t, &log, mcp.NewStreamableHTTPHandler(
				func(*http.Request) *mcp.Server { return server.Server },
				&mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: tc.jsonResponse}))
			id := 0
			request := func(method, params string) string {
				id++
				return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":%q,"params":%s}`, id, method, params)
			}
			newest := func(method, name string) http.Header {
				return hdr("Mcp-Protocol-Version", nonUserNewest, "Mcp-Method", method, "Mcp-Name", name)
			}

			log.section("server/discover")
			for _, declare := range []bool{false, true} {
				probe.post(ctx, t, newest("server/discover", ""), request("server/discover", "{"+newMeta(declare)+"}"))
			}
			for _, version := range []string{nonUserLegacy, "2025-06-18", ""} {
				probe.post(ctx, t, hdr("Mcp-Protocol-Version", version), request("server/discover", "{}"))
			}

			log.section("initialize")
			for _, version := range []string{nonUserNewest, nonUserLegacy, "2025-06-18", "2025-03-26", "2024-11-05"} {
				for _, declare := range []bool{false, true} {
					probe.post(ctx, t, hdr(), request("initialize", legacyInitializeParams(version, declare)))
				}
			}

			log.section("tools/list")
			for _, declare := range []bool{false, true} {
				probe.post(ctx, t, newest("tools/list", ""), request("tools/list", "{"+newMeta(declare)+"}"))
			}

			log.section("tools/call")
			for _, declare := range []bool{false, true} {
				probe.post(ctx, t, newest("tools/call", "echo"), request("tools/call", `{`+newMeta(declare)+`,"name":"echo","arguments":{"a":1}}`))
				probe.post(ctx, t, newest("tools/call", "echo"), request("tools/call", `{`+newMeta(declare)+`,"name":"echo","arguments":{"a":1},"task":{"ttl":1000}}`))
				probe.post(ctx, t, newest("tools/call", "fail"), request("tools/call", `{`+newMeta(declare)+`,"name":"fail","arguments":{}}`))
			}
			probe.post(ctx, t, hdr("Mcp-Protocol-Version", nonUserLegacy), request("tools/call", `{"name":"echo","arguments":{},"task":{"ttl":1000}}`))

			log.section("subscriptions/listen naming tasks")
			for _, notifications := range listenCases {
				for _, declare := range []bool{false, true} {
					probe.post(ctx, t, newest("subscriptions/listen", ""), request("subscriptions/listen", `{`+newMeta(declare)+`,"notifications":`+notifications+`}`))
				}
			}

			log.section("subscriptions/listen naming tasks and a resource")
			for _, declare := range []bool{false, true} {
				stream := probe.open(ctx, t, http.MethodPost, newest("subscriptions/listen", ""),
					request("subscriptions/listen", `{`+newMeta(declare)+`,"notifications":{"resourceSubscriptions":["`+nonUserNoteURI+`"],"taskIds":["t-1"]}}`))
				// The acknowledgement is sent once the subscription exists.
				stream.event(t)
				log.addf("  go SubscribeHandler uri=%s", waitFor(ctx, t, "SubscribeHandler", server.subscribed))
				log.addf("  go ResourceUpdated err=%v", server.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: nonUserNoteURI}))
				stream.event(t)
				stream.close()
				log.addf("  go UnsubscribeHandler uri=%s", waitFor(ctx, t, "UnsubscribeHandler", server.unsubscribed))
				log.addf("  go ResourceUpdated err=%v", server.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: nonUserNoteURI}))
			}

			log.section("notifications a tasks-aware peer may send")
			probe.post(ctx, t, newest("notifications/tasks", ""), `{"jsonrpc":"2.0","method":"notifications/tasks","params":{"taskId":"t-1","status":"working","ttlMs":null}}`)
			probe.post(ctx, t, newest("notifications/tasks", "t-1"), `{"jsonrpc":"2.0","method":"notifications/tasks","params":{"taskId":7}}`)
			probe.post(ctx, t, hdr("Mcp-Protocol-Version", nonUserLegacy), `{"jsonrpc":"2.0","method":"notifications/tasks/status","params":{"taskId":"t-1","status":"working"}}`)

			log.section("ordinary calls afterwards")
			probe.post(ctx, t, newest("tools/call", "echo"), request("tools/call", `{`+newMeta(false)+`,"name":"echo","arguments":{}}`))
			probe.post(ctx, t, newest("acme/echo", ""), request("acme/echo", `{`+newMeta(false)+`,"taskId":"t-1","value":"v"}`))
			emitTranscript(t, tc.name, &log)
		})
	}
}

// TestCompatNonUserServerInMemory is TestCompatNonUserServerHTTP without
// HTTP: raw JSON-RPC lines to a server that has no tasks extension.
func TestCompatNonUserServerInMemory(t *testing.T) {
	for _, tc := range []struct {
		name         string
		custom       bool
		ownExtension bool
	}{
		{"server-mem-own-methods", true, false},
		{"server-mem-no-methods", false, false},
		{"server-mem-own-extension", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := nonUserContext(t)
			var log wireLog
			server := newPlainServer(t, plainServerConfig{custom: tc.custom, ownExtension: tc.ownExtension})
			taskIDs := []string{`"t-1"`, `7`, ``}

			log.section("2026-07-28 connection")
			peer, transport := newRawPeer(&log, false, "")
			ss, err := server.Connect(ctx, transport, nil)
			if err != nil {
				t.Fatal(err)
			}
			id := 0
			call := func(method, params string) rawMsg {
				id++
				return peer.call(ctx, t, id, method, params)
			}
			newest := wireVersion{meta: nonUserNewest}
			for _, declare := range []bool{false, true} {
				call("server/discover", "{"+newMeta(declare)+"}")
				call("tools/list", "{"+newMeta(declare)+"}")
				call("tools/call", `{`+newMeta(declare)+`,"name":"echo","arguments":{"a":1},"task":{"ttl":1000}}`)
				for _, method := range append([]string{"tasks/list", "tasks/result"}, probeMethods...) {
					for _, taskID := range taskIDs {
						call(method, taskParams(newest, declare, taskID))
					}
				}
				for _, notifications := range listenCases {
					call("subscriptions/listen", `{`+newMeta(declare)+`,"notifications":`+notifications+`}`)
				}
				// A subscription that stays open: read the acknowledgement,
				// make the server notify, cancel. A cancelled request of this
				// protocol version is not answered: the listen has ended when
				// the server has unsubscribed.
				id++
				peer.send(t, `{"jsonrpc":"2.0","id":%d,"method":"subscriptions/listen","params":{%s,"notifications":{"resourceSubscriptions":[%q],"taskIds":["t-1"]}}}`, id, newMeta(declare), nonUserNoteURI)
				peer.recv(ctx, t)
				log.addf("go SubscribeHandler uri=%s", waitFor(ctx, t, "SubscribeHandler", server.subscribed))
				log.addf("go ResourceUpdated err=%v", server.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: nonUserNoteURI}))
				peer.recv(ctx, t)
				peer.send(t, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":%d}}`, id)
				log.addf("go UnsubscribeHandler uri=%s", waitFor(ctx, t, "UnsubscribeHandler", server.unsubscribed))
			}
			peer.send(t, `{"jsonrpc":"2.0","method":"notifications/tasks","params":{"taskId":"t-1","status":"working","ttlMs":null}}`)
			peer.send(t, `{"jsonrpc":"2.0","method":"notifications/tasks","params":{"taskId":7}}`)
			call("tools/call", `{`+newMeta(false)+`,"name":"echo","arguments":{}}`)
			peer.closeWrite()
			log.addf("go ServerSession.Wait err=%v", ss.Wait())
			peer.drain(ctx, t)

			log.section("2025-11-25 connection to the same server")
			peer, transport = newRawPeer(&log, false, "")
			if ss, err = server.Connect(ctx, transport, nil); err != nil {
				t.Fatal(err)
			}
			legacy := wireVersion{}
			call("initialize", legacyInitializeParams(nonUserLegacy, true))
			peer.send(t, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
			call("tools/list", "{}")
			call("tools/call", `{"name":"echo","arguments":{"a":1},"task":{"ttl":1000}}`)
			for _, declare := range []bool{false, true} {
				for _, method := range append([]string{"tasks/list", "tasks/result"}, probeMethods...) {
					for _, taskID := range taskIDs {
						call(method, taskParams(legacy, declare, taskID))
					}
				}
			}
			call("resources/subscribe", `{"uri":"`+nonUserNoteURI+`"}`)
			log.addf("go SubscribeHandler uri=%s", waitFor(ctx, t, "SubscribeHandler", server.subscribed))
			log.addf("go ResourceUpdated err=%v", server.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: nonUserNoteURI}))
			peer.recv(ctx, t)
			call("resources/unsubscribe", `{"uri":"`+nonUserNoteURI+`"}`)
			log.addf("go UnsubscribeHandler uri=%s", waitFor(ctx, t, "UnsubscribeHandler", server.unsubscribed))
			peer.send(t, `{"jsonrpc":"2.0","method":"notifications/tasks/status","params":{"taskId":"t-1","status":"working"}}`)
			call("tools/call", `{"name":"echo","arguments":{}}`)
			peer.closeWrite()
			log.addf("go ServerSession.Wait err=%v", ss.Wait())
			peer.drain(ctx, t)
			emitTranscript(t, tc.name, &log)
		})
	}
}

// TestCompatNonUserServerSends records what a server that has no tasks
// extension sends to its client by itself: log messages, progress, and its
// own requests (roots/list, ping) made from inside a tool call.
func TestCompatNonUserServerSends(t *testing.T) {
	ctx := nonUserContext(t)
	var log wireLog
	server := mcp.NewServer(&mcp.Implementation{Name: "plain-server", Version: "v1"}, nil)
	schema := json.RawMessage(`{"type":"object"}`)
	text := func(format string, args ...any) *mcp.CallToolResult {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf(format, args...)}}}
	}
	server.AddTool(&mcp.Tool{Name: "log", InputSchema: schema},
		func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			err := req.Session.Log(ctx, &mcp.LoggingMessageParams{Level: "error", Logger: "fixture", Data: "logged"})
			return text("Log err=%v", err), nil
		})
	server.AddTool(&mcp.Tool{Name: "progress", InputSchema: schema},
		func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			err := req.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{ProgressToken: "tok", Progress: 1, Total: 2})
			return text("NotifyProgress err=%v", err), nil
		})
	server.AddTool(&mcp.Tool{Name: "roots", InputSchema: schema},
		func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			res, err := req.Session.ListRoots(ctx, nil)
			return text("ListRoots result=%s err=%v", marshalValue(res), err), nil
		})
	server.AddTool(&mcp.Tool{Name: "ping", InputSchema: schema},
		func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return text("Ping err=%v", req.Session.Ping(ctx, nil)), nil
		})
	tools := []string{"log", "progress", "roots", "ping"}

	log.section("2025-11-25 connection")
	peer, transport := newRawPeer(&log, false, "")
	ss, err := server.Connect(ctx, transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := 0
	call := func(method, params string) {
		id++
		peer.call(ctx, t, id, method, params)
	}
	call("initialize", `{"protocolVersion":"2025-11-25","capabilities":{"roots":{},"extensions":{"io.modelcontextprotocol/tasks":{}}},"clientInfo":{"name":"raw-caller","version":"v1"}}`)
	peer.send(t, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	call("logging/setLevel", `{"level":"debug"}`)
	for _, tool := range tools {
		call("tools/call", fmt.Sprintf(`{"_meta":{"progressToken":"tok"},"name":%q,"arguments":{}}`, tool))
	}
	peer.closeWrite()
	log.addf("go ServerSession.Wait err=%v", ss.Wait())
	peer.drain(ctx, t)

	log.section("2026-07-28 connection")
	peer, transport = newRawPeer(&log, false, "")
	if ss, err = server.Connect(ctx, transport, nil); err != nil {
		t.Fatal(err)
	}
	meta := `"_meta":{"progressToken":"tok","io.modelcontextprotocol/logLevel":"debug","io.modelcontextprotocol/clientCapabilities":` + nonUserCapsTasks +
		`,"io.modelcontextprotocol/clientInfo":{"name":"raw-caller","version":"v1"},"io.modelcontextprotocol/protocolVersion":"2026-07-28"}`
	for _, tool := range tools {
		call("tools/call", fmt.Sprintf(`{%s,"name":%q,"arguments":{}}`, meta, tool))
	}
	peer.closeWrite()
	log.addf("go ServerSession.Wait err=%v", ss.Wait())
	peer.drain(ctx, t)
	emitTranscript(t, "server-sends-mem", &log)
}
