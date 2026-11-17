// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	plainToolResult = `{"content":[{"type":"text","text":"plain"}],"resultType":"complete"}`
	taskToolResult  = `{"resultType":"task","taskId":"t-1",` + taskFieldsJSON + `}`
)

// taskNotifications are notifications a server that uses tasks may send to a
// client that never asked for them.
var taskNotifications = []struct{ name, line string }{
	{"working", `{"jsonrpc":"2.0","method":"notifications/tasks","params":{"taskId":"t-1",` + taskFieldsJSON + `}}`},
	{"with-subscription-id", `{"jsonrpc":"2.0","method":"notifications/tasks","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":3},"taskId":"t-1",` + taskFieldsJSON + `}}`},
	{"completed", `{"jsonrpc":"2.0","method":"notifications/tasks","params":{"taskId":"t-1","status":"completed","createdAt":"2026-01-02T03:04:05Z","lastUpdatedAt":"2026-01-02T03:04:06Z","ttlMs":null,"result":{"content":[{"type":"text","text":"done"}]}}}`},
	{"taskid-number", `{"jsonrpc":"2.0","method":"notifications/tasks","params":{"taskId":7,"status":"working"}}`},
	{"no-params", `{"jsonrpc":"2.0","method":"notifications/tasks"}`},
	{"legacy-name", `{"jsonrpc":"2.0","method":"notifications/tasks/status","params":{"taskId":"t-1","status":"working"}}`},
}

// serverRequests are requests a server may make of its client.
var serverRequests = []string{"tasks/get", "tasks/update", "tasks/cancel", "tasks/list", "tasks/result"}

// listenAcks are the notifications objects a scripted server acknowledges a
// subscription with. They carry taskIds the client never asked for.
var listenAcks = []string{
	`{"resourceSubscriptions":[%q],"taskIds":["t-1"]}`,
	`{"resourceSubscriptions":[%q],"taskIds":[]}`,
	`{"resourceSubscriptions":[%q],"taskIds":"t-1"}`,
	`{"resourceSubscriptions":[%q],"taskIds":[7]}`,
}

// A peerScript is a scripted server that behaves like one that uses tasks
// with every client, asked or not.
type peerScript struct {
	legacy bool
	// pending is the tools/call that is answered once the client has
	// answered the server's own request.
	pending json.RawMessage
	// acks are the acknowledgements for the next subscriptions, in order.
	acks chan string
	// notes receives the method of each notification the client sends.
	notes chan string
}

func newPeerScript(legacy bool) *peerScript {
	return &peerScript{legacy: legacy, acks: make(chan string, 16), notes: make(chan string, 256)}
}

func (s *peerScript) handle(msg rawMsg) []string {
	result := func(body string) string {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":%s}`, msg.ID, body)
	}
	if replies, ok := rawHandshake(msg, s.legacy); ok {
		return replies
	}
	switch {
	case msg.isResponse():
		// The client's answer to the server's request: now answer its call.
		pending := s.pending
		s.pending = nil
		return []string{fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":%s}`, pending, plainToolResult)}
	case msg.Method == "tools/call":
		var p struct {
			Name string `json:"name"`
		}
		_ = json.Unmarshal(msg.Params, &p)
		kind, arg, _ := strings.Cut(p.Name, ":")
		switch kind {
		case "task":
			return []string{result(taskToolResult)}
		case "needs-tasks":
			// What a server that requires the extension answers a client
			// that did not declare it.
			return []string{fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"error":{"code":-32021,"message":"missing required tasks extension capability","data":{"requiredCapabilities":{"extensions":{"io.modelcontextprotocol/tasks":{}}}}}}`, msg.ID)}
		case "notify":
			for _, n := range taskNotifications {
				if n.name == arg {
					return []string{n.line, result(plainToolResult)}
				}
			}
		case "request":
			s.pending = msg.ID
			return []string{fmt.Sprintf(`{"jsonrpc":"2.0","id":"srv-%s","method":%q,"params":{"taskId":"t-1"}}`, strings.ReplaceAll(arg, "/", "-"), arg)}
		}
		return []string{result(plainToolResult)}
	case msg.Method == "tools/list":
		return []string{result(`{"tools":[{"name":"plain","inputSchema":{"type":"object"},"execution":{"taskSupport":"required"}}]}`)}
	case msg.Method == "subscriptions/listen":
		var p struct {
			Notifications struct {
				ResourceSubscriptions []string `json:"resourceSubscriptions"`
				ToolsListChanged      bool     `json:"toolsListChanged"`
			} `json:"notifications"`
		}
		_ = json.Unmarshal(msg.Params, &p)
		if p.Notifications.ToolsListChanged {
			return []string{
				fmt.Sprintf(`{"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":%s},"notifications":{"toolsListChanged":true,"taskIds":["t-9"]}}}`, msg.ID),
				fmt.Sprintf(`{"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":%s}}}`, msg.ID),
			}
		}
		uri := p.Notifications.ResourceSubscriptions[0]
		return []string{
			fmt.Sprintf(`{"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":%s},"notifications":%s}}`, msg.ID, fmt.Sprintf(<-s.acks, uri)),
			fmt.Sprintf(`{"jsonrpc":"2.0","method":"notifications/resources/updated","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":%s},"uri":%q}}`, msg.ID, uri),
		}
	case msg.Method == "resources/subscribe":
		var p struct {
			URI string `json:"uri"`
		}
		_ = json.Unmarshal(msg.Params, &p)
		return []string{
			result(`{}`),
			fmt.Sprintf(`{"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"notifications":%s}}`, fmt.Sprintf(<-s.acks, p.URI)),
			fmt.Sprintf(`{"jsonrpc":"2.0","method":"notifications/resources/updated","params":{"uri":%q}}`, p.URI),
		}
	case msg.Method == "resources/unsubscribe":
		return []string{result(`{}`)}
	case msg.Method != "" && len(msg.ID) > 0:
		return []string{rawMethodNotFound(msg.ID, msg.Method)}
	}
	return nil
}

// note records a notification from the client. It is called once the
// notification is in the transcript.
func (s *peerScript) note(msg rawMsg) {
	if msg.Method != "" && len(msg.ID) == 0 {
		s.notes <- msg.Method
	}
}

// expectNote waits for the next notification from the client and logs a
// mismatch.
func (s *peerScript) expectNote(ctx context.Context, t *testing.T, log *wireLog, method string) {
	t.Helper()
	if got := waitFor(ctx, t, "the client to send "+method, s.notes); got != method {
		log.addf("go unexpected notification from the client: %s (wanted %s)", got, method)
	}
}

// TestCompatNonUserClientPeerMessages records what a client that did not opt
// in to tasks does when the server sends task messages anyway: results,
// notifications, requests, and subscription acknowledgements naming tasks.
func TestCompatNonUserClientPeerMessages(t *testing.T) {
	for _, tc := range []struct {
		name      string
		transport string
		legacy    bool
	}{
		{"client-peer-mem-2026", "mem", false},
		{"client-peer-mem-2025", "mem", true},
		{"client-peer-http-2026", "http", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := nonUserContext(t)
			var log wireLog
			updated := make(chan string, 16)
			client := mcp.NewClient(&mcp.Implementation{Name: "plain-client", Version: "v1"}, &mcp.ClientOptions{
				ResourceUpdatedHandler: func(_ context.Context, req *mcp.ResourceUpdatedNotificationRequest) {
					updated <- req.Params.URI
				},
			})
			script := newPeerScript(tc.legacy)
			holds := make(chan chan struct{}, 16)
			var transport mcp.Transport
			var served <-chan struct{}
			if tc.transport == "mem" {
				var peer *rawPeer
				peer, transport = newRawPeer(&log, true, "")
				served = peer.serve(ctx, t, func(msg rawMsg) []string {
					defer script.note(msg)
					return script.handle(msg)
				})
			} else {
				srv := newRawHTTPServer(t, &log, func(req *http.Request, msg rawMsg) rawHTTPReply {
					if req.Method != http.MethodPost {
						return rawHTTPReply{status: http.StatusMethodNotAllowed}
					}
					replies := script.handle(msg)
					switch {
					case msg.Method == "subscriptions/listen":
						reply := sseReply(replies...)
						reply.hold = make(chan struct{})
						holds <- reply.hold
						return reply
					case len(replies) == 0:
						return rawHTTPReply{status: http.StatusAccepted, after: func() { script.note(msg) }}
					case len(replies) == 1:
						return jsonReply(replies[0])
					}
					return sseReply(replies...)
				})
				transport = &mcp.StreamableClientTransport{Endpoint: srv.url, DisableStandaloneSSE: true, MaxRetries: -1}
			}

			log.section("handshake")
			session, err := client.Connect(ctx, transport, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.legacy && tc.transport == "mem" {
				script.expectNote(ctx, t, &log, "notifications/initialized")
			}
			call := func(name string) {
				log.addf("go CallTool(%s) %s", name, guarded(func() string {
					return describeToolResult(session.CallTool(ctx, &mcp.CallToolParams{Name: name}))
				}))
			}

			log.section("a task result nobody asked for")
			call("task")

			log.section("an error that asks for the tasks capability")
			call("needs-tasks")

			log.section("notifications/tasks nobody subscribed to")
			for _, n := range taskNotifications {
				call("notify:" + n.name)
			}

			if tc.transport == "mem" {
				log.section("tasks requests from the server")
				for _, method := range serverRequests {
					call("request:" + method)
				}
			}

			log.section("subscription acknowledgements naming tasks")
			for i, ack := range listenAcks {
				uri := fmt.Sprintf("fixture://note-%d", i)
				script.acks <- ack
				// Subscribe returns before the server has answered. The update
				// is the last thing the server sends, so wait for it first.
				subErr := session.Subscribe(ctx, &mcp.SubscribeParams{URI: uri})
				gotURI := waitFor(ctx, t, "ResourceUpdatedHandler", updated)
				log.addf("go Subscribe(%s) err=%v", uri, subErr)
				log.addf("go ResourceUpdatedHandler uri=%s", gotURI)
				unsubErr := session.Unsubscribe(ctx, &mcp.UnsubscribeParams{URI: uri})
				if !tc.legacy {
					script.expectNote(ctx, t, &log, "notifications/cancelled")
				}
				if tc.transport == "http" {
					waitFor(ctx, t, "the client to leave the listen stream", waitFor(ctx, t, "the listen stream", holds))
					log.addf("go the client left the listen stream")
				}
				log.addf("go Unsubscribe(%s) err=%v", uri, unsubErr)
			}

			log.section("ordinary calls afterwards")
			tools, err := session.ListTools(ctx, nil)
			log.addf("go ListTools %s err=%v", marshalValue(tools), err)
			call("plain")

			log.section("close")
			closeErr := session.Close()
			if served != nil {
				waitFor(ctx, t, "the scripted server to see the client close", served)
			}
			log.addf("go Close err=%v", closeErr)
			emitTranscript(t, tc.name, &log)
		})
	}
}

// Types of a program's own tasks/* client methods.
type ownTaskParams struct {
	mcp.ParamsBase
	TaskID string `json:"taskId,omitempty"`
	Value  string `json:"value,omitempty"`
}

type ownNumericTaskParams struct {
	mcp.ParamsBase
	TaskID int `json:"taskId"`
}

type ownTaskResult struct {
	mcp.ResultBase
	TaskID     string `json:"taskId,omitempty"`
	Status     string `json:"status,omitempty"`
	ResultType string `json:"resultType,omitempty"`
	Value      string `json:"value,omitempty"`
}

var ownClientMethods = []string{"tasks/get", "tasks/update", "tasks/cancel", "tasks/list", "tasks/result", "acme/echo"}

// TestCompatNonUserClientCustomMethods records what a client sends for its
// own custom methods named tasks/*, registered with AddSendingCustomMethod,
// and for the same names when they are not registered.
func TestCompatNonUserClientCustomMethods(t *testing.T) {
	script := func(legacy bool) func(rawMsg) []string {
		return func(msg rawMsg) []string {
			if msg.Method == "server/discover" && legacy {
				// Over HTTP a legacy server answers discover with the
				// versions it has, and the client falls back to initialize.
				return []string{fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"resultType":"complete","supportedVersions":["2025-11-25"],"capabilities":{}}}`, msg.ID)}
			}
			if replies, ok := rawHandshake(msg, false); ok {
				return replies
			}
			switch {
			case msg.Method == "tools/call":
				return []string{fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":%s}`, msg.ID, plainToolResult)}
			case strings.HasPrefix(msg.Method, "tasks/"), msg.Method == "acme/echo":
				return []string{fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"resultType":"task","taskId":"t-1","status":"working","ttlMs":60000,"value":%q}}`, msg.ID, msg.Method)}
			case msg.Method != "" && len(msg.ID) > 0:
				return []string{rawMethodNotFound(msg.ID, msg.Method)}
			}
			return nil
		}
	}
	for _, tc := range []struct {
		name      string
		transport string
		legacy    bool
	}{
		{"client-custom-mem-2026", "mem", false},
		{"client-custom-mem-2025", "mem", true},
		{"client-custom-http-2026", "http", false},
		{"client-custom-http-2025", "http", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := nonUserContext(t)
			var log wireLog
			for _, registered := range []bool{true, false} {
				client := mcp.NewClient(&mcp.Implementation{Name: "plain-client", Version: "v1"}, nil)
				if registered {
					log.section("client that registered its own tasks/* methods")
					for _, method := range ownClientMethods {
						if err := mcp.AddSendingCustomMethod[*ownTaskParams, *ownTaskResult](client, method); err != nil {
							t.Fatalf("AddSendingCustomMethod(%q): %v", method, err)
						}
					}
				} else {
					log.section("client that registered nothing")
				}
				handle, initialized := onMethod(script(tc.legacy), "notifications/initialized")
				var transport mcp.Transport
				var served <-chan struct{}
				if tc.transport == "mem" {
					var peer *rawPeer
					peer, transport = newRawPeer(&log, true, "")
					served = peer.serve(ctx, t, func(msg rawMsg) []string {
						if msg.Method == "server/discover" && tc.legacy {
							return []string{rawMethodNotFound(msg.ID, msg.Method)}
						}
						return handle(msg)
					})
				} else {
					srv := newRawHTTPServer(t, &log, func(req *http.Request, msg rawMsg) rawHTTPReply {
						if req.Method == http.MethodDelete {
							return rawHTTPReply{status: http.StatusNoContent}
						}
						if req.Method != http.MethodPost {
							return rawHTTPReply{status: http.StatusMethodNotAllowed}
						}
						replies := handle(msg)
						if len(replies) == 0 {
							return rawHTTPReply{status: http.StatusAccepted}
						}
						reply := jsonReply(replies[0])
						if msg.Method == "initialize" {
							reply.header["Mcp-Session-Id"] = []string{"raw-session"}
						}
						return reply
					})
					transport = &mcp.StreamableClientTransport{Endpoint: srv.url, DisableStandaloneSSE: true, MaxRetries: -1}
				}
				session, err := client.Connect(ctx, transport, nil)
				if err != nil {
					t.Fatal(err)
				}
				if tc.legacy {
					waitFor(ctx, t, "the scripted server to see notifications/initialized", initialized)
				}
				declared := func() mcp.ParamsBase {
					return mcp.ParamsBase{Meta: mcp.Meta{mcp.MetaKeyClientCapabilities: map[string]any{
						"extensions": map[string]any{nonUserTasksExt: map[string]any{}},
					}}}
				}
				for _, method := range ownClientMethods {
					for _, params := range []struct {
						label string
						call  func() (*ownTaskResult, error)
					}{
						{"taskId", func() (*ownTaskResult, error) {
							return mcp.CallCustomMethod[*ownTaskParams, *ownTaskResult](ctx, session, method, &ownTaskParams{TaskID: "t-1", Value: "v"})
						}},
						{"no-taskId", func() (*ownTaskResult, error) {
							return mcp.CallCustomMethod[*ownTaskParams, *ownTaskResult](ctx, session, method, &ownTaskParams{Value: "v"})
						}},
						{"nil-params", func() (*ownTaskResult, error) {
							return mcp.CallCustomMethod[*ownTaskParams, *ownTaskResult](ctx, session, method, nil)
						}},
						{"taskId+declared", func() (*ownTaskResult, error) {
							return mcp.CallCustomMethod[*ownTaskParams, *ownTaskResult](ctx, session, method, &ownTaskParams{ParamsBase: declared(), TaskID: "t-1"})
						}},
						{"numeric-taskId", func() (*ownTaskResult, error) {
							return mcp.CallCustomMethod[*ownNumericTaskParams, *ownTaskResult](ctx, session, method, &ownNumericTaskParams{TaskID: 7})
						}},
					} {
						log.addf("go CallCustomMethod(%s, %s) %s", method, params.label, guarded(func() string {
							res, err := params.call()
							if err != nil {
								return fmt.Sprintf("err=%q", err.Error())
							}
							return "result=" + marshalValue(res)
						}))
					}
				}
				log.addf("go CallTool(plain) %s", guarded(func() string {
					return describeToolResult(session.CallTool(ctx, &mcp.CallToolParams{Name: "plain"}))
				}))
				closeErr := session.Close()
				if served != nil {
					waitFor(ctx, t, "the scripted server to see the client close", served)
				}
				log.addf("go Close err=%v", closeErr)
			}
			emitTranscript(t, tc.name, &log)
		})
	}
}
