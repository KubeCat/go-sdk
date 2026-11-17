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

const afterInputResult = `{"content":[{"type":"text","text":"after-input"}],"resultType":"complete"}`

// resultScript is a scripted server that answers tools/call with the
// resultCase named by the "case" argument, whatever the caller declared.
func resultScript(legacy bool) func(rawMsg) []string {
	byName := make(map[string]string)
	for _, c := range resultCases() {
		byName[c.name] = c.json
	}
	return func(msg rawMsg) []string {
		if replies, ok := rawHandshake(msg, legacy); ok {
			return replies
		}
		if msg.Method == "tools/call" {
			var p struct {
				Arguments struct {
					Case string `json:"case"`
				} `json:"arguments"`
				InputResponses json.RawMessage `json:"inputResponses"`
			}
			if err := json.Unmarshal(msg.Params, &p); err != nil {
				return []string{fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"error":{"code":-32602,"message":"bad params"}}`, msg.ID)}
			}
			result, ok := byName[p.Arguments.Case]
			if !ok {
				result = `{"content":[{"type":"text","text":"no such case"}],"resultType":"complete"}`
			}
			if len(p.InputResponses) > 0 {
				// The retry of a call whose input requests were fulfilled.
				result = afterInputResult
			}
			return []string{fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":%s}`, msg.ID, result)}
		}
		if len(msg.ID) > 0 && msg.Method != "" {
			return []string{rawMethodNotFound(msg.ID, msg.Method)}
		}
		return nil
	}
}

func caseArgument(req *mcp.CallToolRequest) string {
	var args struct {
		Case string `json:"case"`
	}
	_ = json.Unmarshal(req.Params.Arguments, &args)
	return args.Case
}

// newRelayServer returns a server with no tasks extension whose tools hand on
// a result they did not build:
//
//   - relay-json decodes the wire JSON of a resultCase and returns it;
//   - relay-upstream returns what upstream.CallTool returned, unchanged;
//   - relay-typed does the same from a typed (generic) tool handler.
func newRelayServer(upstream *mcp.ClientSession, sessionIDs func() string) *mcp.Server {
	byName := make(map[string]string)
	for _, c := range resultCases() {
		byName[c.name] = c.json
	}
	s := mcp.NewServer(&mcp.Implementation{Name: "relay-server", Version: "v1"}, &mcp.ServerOptions{GetSessionID: sessionIDs})
	schema := json.RawMessage(`{"type":"object"}`)
	s.AddTool(&mcp.Tool{Name: "relay-json", InputSchema: schema},
		func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			var res mcp.CallToolResult
			if err := json.Unmarshal([]byte(byName[caseArgument(req)]), &res); err != nil {
				return nil, fmt.Errorf("decoding stored result: %w", err)
			}
			return &res, nil
		})
	if upstream != nil {
		s.AddTool(&mcp.Tool{Name: "relay-upstream", InputSchema: schema},
			func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return upstream.CallTool(ctx, &mcp.CallToolParams{Name: "upstream", Arguments: map[string]any{"case": caseArgument(req)}})
			})
		mcp.AddTool(s, &mcp.Tool{Name: "relay-typed"},
			func(ctx context.Context, _ *mcp.CallToolRequest, in map[string]any) (*mcp.CallToolResult, any, error) {
				res, err := upstream.CallTool(ctx, &mcp.CallToolParams{Name: "upstream", Arguments: map[string]any{"case": in["case"]}})
				return res, nil, err
			})
	}
	return s
}

// connectUpstream connects an SDK client, with default options, to a scripted
// upstream server.
func connectUpstream(ctx context.Context, t *testing.T, log *wireLog, legacy bool) *mcp.ClientSession {
	t.Helper()
	peer, transport := newRawPeer(log, true, "upstream ")
	script, initialized := onMethod(resultScript(legacy), "notifications/initialized")
	peer.serve(ctx, t, script)
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "relay-client", Version: "v1"}, nil).Connect(ctx, transport, nil)
	if err != nil {
		t.Fatalf("connecting upstream: %v", err)
	}
	if legacy {
		// Connect returns once notifications/initialized is written; hold
		// until the scripted server has logged it.
		waitFor(ctx, t, "the upstream to see notifications/initialized", initialized)
	}
	t.Cleanup(func() { cs.Close() })
	return cs
}

func callParams(meta, tool, caseName string) string {
	if meta != "" {
		meta += ","
	}
	return fmt.Sprintf(`{%s"name":%q,"arguments":{"case":%q}}`, meta, tool, caseName)
}

// TestCompatNonUserRelayInMemory records what the caller of a server without
// the tasks extension sees when a tool hands on a result it did not build.
func TestCompatNonUserRelayInMemory(t *testing.T) {
	for _, tc := range []struct {
		name           string
		callerLegacy   bool
		upstreamLegacy bool
		tools          []string
	}{
		{"relay-mem-caller2026-upstream2026", false, false, []string{"relay-json", "relay-upstream", "relay-typed"}},
		{"relay-mem-caller2025-upstream2026", true, false, []string{"relay-json", "relay-upstream"}},
		{"relay-mem-caller2026-upstream2025", false, true, []string{"relay-upstream"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := nonUserContext(t)
			var log wireLog
			log.section("upstream handshake")
			upstream := connectUpstream(ctx, t, &log, tc.upstreamLegacy)
			server := newRelayServer(upstream, nil)
			caller, transport := newRawPeer(&log, false, "")
			ss, err := server.Connect(ctx, transport, nil)
			if err != nil {
				t.Fatal(err)
			}
			id := 0
			next := func() int { id++; return id }
			meta := newMeta(false)
			log.section("caller handshake")
			if tc.callerLegacy {
				meta = ""
				caller.call(ctx, t, next(), "initialize", legacyInitializeParams(nonUserLegacy, false))
				caller.send(t, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
			} else {
				caller.call(ctx, t, next(), "server/discover", "{"+meta+"}")
			}
			for _, tool := range tc.tools {
				for _, c := range resultCases() {
					log.section("%s %s", tool, c.name)
					caller.call(ctx, t, next(), "tools/call", callParams(meta, tool, c.name))
				}
			}
			log.section("ordinary call afterwards")
			caller.call(ctx, t, next(), "tools/list", "{"+meta+"}")
			caller.closeWrite()
			if err := ss.Wait(); err != nil {
				log.addf("server session ended: %v", err)
			}
			emitTranscript(t, tc.name, &log)
		})
	}
}

// TestCompatNonUserRelayHTTP is TestCompatNonUserRelayInMemory with the caller
// on Streamable HTTP, stateless (2026-07-28) and with a session (2025-11-25).
func TestCompatNonUserRelayHTTP(t *testing.T) {
	for _, stateless := range []bool{true, false} {
		name := "relay-http-stateful"
		if stateless {
			name = "relay-http-stateless"
		}
		t.Run(name, func(t *testing.T) {
			ctx := nonUserContext(t)
			var log wireLog
			log.section("upstream handshake")
			upstream := connectUpstream(ctx, t, &log, false)
			server := newRelayServer(upstream, func() string { return "relay-session" })
			probe := newHTTPProbe(t, &log, mcp.NewStreamableHTTPHandler(
				func(*http.Request) *mcp.Server { return server },
				&mcp.StreamableHTTPOptions{Stateless: stateless, JSONResponse: true}))
			id := 0
			next := func() int { id++; return id }
			request := func(method, params string) string {
				return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":%q,"params":%s}`, next(), method, params)
			}
			var headers func(method, name string) http.Header
			meta := newMeta(false)
			log.section("caller handshake")
			if stateless {
				headers = func(method, name string) http.Header {
					return hdr("Mcp-Protocol-Version", nonUserNewest, "Mcp-Method", method, "Mcp-Name", name)
				}
				probe.post(ctx, t, headers("server/discover", ""), request("server/discover", "{"+meta+"}"))
			} else {
				meta = ""
				headers = func(string, string) http.Header {
					return hdr("Mcp-Protocol-Version", nonUserLegacy, "Mcp-Session-Id", "relay-session")
				}
				probe.post(ctx, t, hdr(), request("initialize", legacyInitializeParams(nonUserLegacy, false)))
				probe.post(ctx, t, headers("", ""), `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
			}
			for _, tool := range []string{"relay-json", "relay-upstream"} {
				for _, c := range resultCases() {
					if !stateless && tool == "relay-json" && strings.Contains(c.name, "inputreq-roots") {
						// A 2025-11-25 session would be asked for roots over
						// the session's own stream; that exchange is covered
						// in memory (relay-mem-caller2025-upstream2026).
						continue
					}
					log.section("%s %s", tool, c.name)
					probe.post(ctx, t, headers("tools/call", tool), request("tools/call", callParams(meta, tool, c.name)))
				}
			}
			log.section("ordinary call afterwards")
			probe.post(ctx, t, headers("tools/list", ""), request("tools/list", "{"+meta+"}"))
			emitTranscript(t, name, &log)
		})
	}
}

// TestCompatNonUserClientResults records what a client that did not opt in to
// tasks returns from CallTool for every resultCase.
func TestCompatNonUserClientResults(t *testing.T) {
	declared := func() mcp.Meta {
		return mcp.Meta{mcp.MetaKeyClientCapabilities: map[string]any{
			"roots":      map[string]any{"listChanged": true},
			"extensions": map[string]any{nonUserTasksExt: map[string]any{}},
		}}
	}
	capsDeclared := &mcp.ClientCapabilities{}
	capsDeclared.AddExtension(nonUserTasksExt, nil)
	for _, tc := range []struct {
		name      string
		transport string // "mem" or "http"
		legacy    bool
		options   *mcp.ClientOptions
		meta      func() mcp.Meta
		// middleware adds a sending middleware that prints each tools/call
		// result it is handed, as a logging or tracing middleware would.
		middleware bool
	}{
		// A client with default options.
		{name: "client-results-mem-2026", transport: "mem"},
		{name: "client-results-mem-2025", transport: "mem", legacy: true},
		{name: "client-results-http-2026", transport: "http"},
		// A program written for the unchanged SDK that handles input-required results itself.
		{name: "client-results-mem-2026-mrtr-off", transport: "mem",
			options: &mcp.ClientOptions{MultiRoundTrip: &mcp.MultiRoundTripOptions{Disabled: true}}},
		// A program written for the unchanged SDK that declares the extension by itself, with
		// the unchanged SDK's API only: in its capabilities, or in one call's _meta.
		{name: "client-results-mem-2026-caps-declare", transport: "mem",
			options: &mcp.ClientOptions{Capabilities: capsDeclared}},
		{name: "client-results-mem-2026-meta-declare", transport: "mem", meta: declared},
		{name: "client-results-mem-2026-middleware", transport: "mem", middleware: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := nonUserContext(t)
			var log wireLog
			client := mcp.NewClient(&mcp.Implementation{Name: "plain-client", Version: "v1"}, tc.options)
			if tc.middleware {
				client.AddSendingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
					return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
						res, err := next(ctx, method, req)
						if method == "tools/call" {
							log.addf("go middleware saw result=%s err=%v", marshalValue(res), err)
						}
						return res, err
					}
				})
			}
			script, initialized := onMethod(resultScript(tc.legacy), "notifications/initialized")
			var transport mcp.Transport
			var served <-chan struct{}
			if tc.transport == "mem" {
				var peer *rawPeer
				peer, transport = newRawPeer(&log, true, "")
				served = peer.serve(ctx, t, script)
			} else {
				srv := newRawHTTPServer(t, &log, func(req *http.Request, msg rawMsg) rawHTTPReply {
					if req.Method != http.MethodPost {
						return rawHTTPReply{status: http.StatusMethodNotAllowed}
					}
					replies := script(msg)
					if len(replies) == 0 {
						return rawHTTPReply{status: http.StatusAccepted}
					}
					return jsonReply(replies[0])
				})
				transport = &mcp.StreamableClientTransport{Endpoint: srv.url, DisableStandaloneSSE: true, MaxRetries: -1}
			}
			log.section("handshake")
			cs, err := client.Connect(ctx, transport, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.legacy {
				waitFor(ctx, t, "the scripted server to see notifications/initialized", initialized)
			}
			for _, c := range resultCases() {
				log.section("%s", c.name)
				log.addf("go %s", guarded(func() string {
					params := &mcp.CallToolParams{Name: "probe", Arguments: map[string]any{"case": c.name}}
					if tc.meta != nil {
						params.Meta = tc.meta()
					}
					return describeToolResult(cs.CallTool(ctx, params))
				}))
			}
			log.section("ordinary calls afterwards")
			log.addf("go %s", guarded(func() string {
				return describeToolResult(cs.CallTool(ctx, &mcp.CallToolParams{Name: "probe", Arguments: map[string]any{"case": "complete/content"}}))
			}))
			// CallTool fills in the caller's params in the unchanged SDK; print them after.
			params := &mcp.CallToolParams{Name: "probe"}
			log.addf("go %s", guarded(func() string { return describeToolResult(cs.CallTool(ctx, params)) }))
			log.addf("go params after CallTool %s", marshalValue(params))
			log.addf("go Ping err=%v", cs.Ping(ctx, nil))
			log.section("close")
			closeErr := cs.Close()
			if served != nil {
				waitFor(ctx, t, "the scripted server to see the client close", served)
			}
			log.addf("go Close err=%v", closeErr)
			emitTranscript(t, tc.name, &log)
		})
	}
}
