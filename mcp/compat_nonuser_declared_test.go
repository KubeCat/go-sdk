// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// declaredTaskFields are the fields of a task, as a scripted server puts them
// in an answer. They carry no ttlMs: a client keeps a list result that has
// one, and these tests ask again.
const declaredTaskFields = `"taskId":"t-1","status":"working","createdAt":"2026-01-02T03:04:05Z","lastUpdatedAt":"2026-01-02T03:04:06Z"`

// describeResult prints what a caller sees of a call's return.
func describeResult(v any, err error) string {
	if err != nil {
		return fmt.Sprintf("err=%q", err.Error())
	}
	return marshalValue(v)
}

// TestCompatNonUserClientDecoding records how a client decodes the answers
// to the standard requests: well-formed ones, ones whose keys have the wrong
// case or the wrong type, and ones that are not objects. The answers are
// decoded by the SDK's own decoder, which matches field names by their exact
// case; that is what stops a paginating caller.
//
// A program written for the unchanged SDK can name the tasks extension by itself, with the unchanged SDK's API:
// in ClientOptions.Capabilities, or in one request's _meta. Such a client
// decodes every answer as the unchanged SDK does, with one exception that the unchanged SDK cannot
// show and that is therefore left out of that one transcript: a client whose
// capabilities name the extension, on a 2026-07-28 session, takes a task in
// answer to a request other than tools/call as an invalid response.
//
// Every client here has a list-changed handler, so Connect opens the standard
// subscriptions/listen, from a goroutine of its own. The scripted server
// refuses it at once, and the test goes on when it has.
func TestCompatNonUserClientDecoding(t *testing.T) {
	const taskAnswer = "task"
	payloads := []struct{ name, body string }{
		{"ok", `{"tools":[{"name":"a","inputSchema":{"type":"object"}}],"prompts":[{"name":"p"}],"resources":[{"uri":"file:///a","name":"r"}],"resourceTemplates":[{"uriTemplate":"file:///{a}","name":"rt"}],"messages":[{"role":"user","content":{"type":"text","text":"hi"}}],"contents":[{"uri":"file:///a","text":"x"}],"completion":{"values":["v"]},"nextCursor":"c"}`},
		{"upper-keys", `{"Tools":[{"Name":"a","InputSchema":{"type":"object"}}],"PROMPTS":[{"NAME":"p"}],"Resources":[{"URI":"file:///a","Name":"r"}],"ResourceTemplates":[{"uriTemplate":"file:///{a}","name":"rt"}],"Messages":[{"role":"user","content":{"type":"text","text":"hi"}}],"Contents":[{"uri":"file:///a","text":"x"}],"Completion":{"Values":["v"]},"NextCursor":"c","_META":{"k":1}}`},
		{"mixed-keys", `{"tools":[{"NAME":"a","inputschema":{"type":"object"},"Description":"d"}],"prompts":[{"Name":"p","Title":"t"}],"nextcursor":"c","completion":{"VALUES":["v"],"Total":3}}`},
		{"shadowed-keys", `{"tools":[],"NextCursor":"smuggled","Tools":[{"name":"x","inputSchema":{"type":"object"}}]}`},
		{"wrong-types", `{"tools":7,"prompts":"x","resources":{},"messages":7,"contents":7,"completion":7}`},
		{"null", `null`},
		{"seven", `7`},
		{"array", `[]`},
		{"string", `"x"`},
		{"dup-keys", `{"tools":[{"name":"a","inputSchema":{"type":"object"}}],"tools":[{"name":"b","inputSchema":{"type":"object"}}],"nextCursor":"1","nextCursor":"2"}`},
		{"bad-utf8", "{\"nextCursor\":\"a\xffb\",\"tools\":[{\"name\":\"a\xffb\",\"inputSchema\":{\"type\":\"object\"}}]}"},
		{"result-type-wrong-case", `{"ResultType":"task","RESULTTYPE":"task",` + declaredTaskFields + `}`},
		{"result-type-number", `{"resultType":7,"tools":[]}`},
		{"result-type-complete-with-task-fields", `{"resultType":"complete",` + declaredTaskFields + `}`},
		{"task-fields-wrong-case", `{"resultType":"complete","TaskId":"t-1","STATUS":"working","tools":[]}`},
		{"meta-number", `{"_meta":7,"tools":[]}`},
		{"trailing-space", `{"tools":[]} `},
		{taskAnswer, `{"resultType":"task",` + declaredTaskFields + `}`},
	}
	declared := func() mcp.Meta {
		return mcp.Meta{mcp.MetaKeyClientCapabilities: map[string]any{"extensions": map[string]any{nonUserTasksExt: map[string]any{}}}}
	}
	otherExtension := func() mcp.Meta {
		return mcp.Meta{mcp.MetaKeyClientCapabilities: map[string]any{"extensions": map[string]any{"example.test/other": map[string]any{}}}}
	}
	for _, tc := range []struct {
		name   string
		legacy bool
		caps   bool // the client's capabilities name the tasks extension
		meta   func() mcp.Meta
	}{
		{name: "client-decode-mem-2026"},
		{name: "client-decode-mem-2026-caps-declare", caps: true},
		{name: "client-decode-mem-2025-caps-declare", caps: true, legacy: true},
		// An earlier protocol version has no extensions, whatever the program
		// writes in its capabilities and in a request.
		{name: "client-decode-mem-2025-caps-and-meta-declare", caps: true, legacy: true, meta: declared},
		{name: "client-decode-mem-2026-meta-declare", meta: declared},
		{name: "client-decode-mem-2026-meta-other-extension", meta: otherExtension},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := nonUserContext(t)
			var log wireLog
			opts := &mcp.ClientOptions{
				ToolListChangedHandler: func(context.Context, *mcp.ToolListChangedRequest) {},
			}
			if tc.caps {
				opts.Capabilities = &mcp.ClientCapabilities{}
				opts.Capabilities.AddExtension(nonUserTasksExt, nil)
			}
			client := mcp.NewClient(&mcp.Implementation{Name: "decode-client", Version: "v1"}, opts)
			var answer atomic.Pointer[string]
			answer.Store(&payloads[0].body)
			script, initialized := onMethod(func(msg rawMsg) []string {
				if replies, ok := rawHandshake(msg, tc.legacy); ok {
					return replies
				}
				switch {
				case msg.Method == "subscriptions/listen":
					return []string{fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"error":{"code":-32603,"message":"no subscriptions here"}}`, msg.ID)}
				case msg.Method != "" && len(msg.ID) > 0:
					return []string{fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":%s}`, msg.ID, *answer.Load())}
				}
				return nil
			}, "notifications/initialized")
			peer, transport := newRawPeer(&log, true, "")
			listenRefused := peer.answered("subscriptions/listen")
			peer.serve(ctx, t, script)
			log.section("handshake")
			session, err := client.Connect(ctx, transport, nil)
			if err != nil {
				t.Fatalf("Connect: %v", err)
			}
			if tc.legacy {
				waitFor(ctx, t, "the scripted server to see notifications/initialized", initialized)
			} else {
				// Connect does not wait for its listen to be sent, nor for
				// the answer: wait until the refusal is in the transcript.
				waitFor(ctx, t, "the scripted server to refuse the listen", listenRefused)
			}
			log.addf("go Ping after Connect: err=%v", session.Ping(ctx, nil))
			meta := func() mcp.Meta {
				if tc.meta == nil {
					return nil
				}
				return tc.meta()
			}
			for _, p := range payloads {
				if p.name == taskAnswer && tc.caps && !tc.legacy {
					// The one exception; see the test's comment.
					continue
				}
				answer.Store(&p.body)
				log.section("%s", p.name)
				log.addf("go ListTools: %s", guarded(func() string {
					return describeResult(session.ListTools(ctx, &mcp.ListToolsParams{Meta: meta()}))
				}))
				log.addf("go ListPrompts: %s", guarded(func() string {
					return describeResult(session.ListPrompts(ctx, &mcp.ListPromptsParams{Meta: meta()}))
				}))
				log.addf("go ListResources: %s", guarded(func() string {
					return describeResult(session.ListResources(ctx, &mcp.ListResourcesParams{Meta: meta()}))
				}))
				log.addf("go ListResourceTemplates: %s", guarded(func() string {
					return describeResult(session.ListResourceTemplates(ctx, &mcp.ListResourceTemplatesParams{Meta: meta()}))
				}))
				log.addf("go GetPrompt: %s", guarded(func() string {
					return describeResult(session.GetPrompt(ctx, &mcp.GetPromptParams{Meta: meta(), Name: "p"}))
				}))
				log.addf("go ReadResource: %s", guarded(func() string {
					return describeResult(session.ReadResource(ctx, &mcp.ReadResourceParams{Meta: meta(), URI: "file:///a"}))
				}))
				log.addf("go Complete: %s", guarded(func() string {
					return describeResult(session.Complete(ctx, &mcp.CompleteParams{
						Meta:     meta(),
						Ref:      &mcp.CompleteReference{Type: "ref/prompt", Name: "p"},
						Argument: mcp.CompleteParamsArgument{Name: "a", Value: "v"},
					}))
				}))
				log.addf("go Ping: err=%v", session.Ping(ctx, &mcp.PingParams{Meta: meta()}))
				log.addf("go Ping without params: err=%v", session.Ping(ctx, nil))
				log.addf("go SetLoggingLevel: err=%v", session.SetLoggingLevel(ctx, &mcp.SetLoggingLevelParams{Meta: meta(), Level: "info"}))
				log.addf("go CallTool: %s", guarded(func() string {
					return describeToolResult(session.CallTool(ctx, &mcp.CallToolParams{Meta: meta(), Name: "a"}))
				}))
				log.addf("go Tools: %s", guarded(func() string {
					// A caller that pages through the tools. It stops by
					// itself after four tools: the scripted server gives the
					// same answer to every page.
					var b strings.Builder
					seen := 0
					for tool, err := range session.Tools(ctx, nil) {
						if err != nil {
							fmt.Fprintf(&b, "err=%q", err.Error())
							break
						}
						fmt.Fprintf(&b, "%s,", tool.Name)
						if seen++; seen > 3 {
							b.WriteString("(stopped by the test)")
							break
						}
					}
					return b.String()
				}))
			}
			peer.mute()
			session.Close()
			emitTranscript(t, tc.name, &log)
		})
	}
}

// TestCompatNonUserRelayMeta records what a relay returns: a server whose
// tools call an upstream server and hand it the _meta of the request they
// are serving, as a proxy that passes _meta through does. The relay never
// names the tasks extension; in the second transcript its caller does, so
// the relay's upstream requests carry a declaration the relay did not make.
// The upstream server answers every request with a task.
func TestCompatNonUserRelayMeta(t *testing.T) {
	for _, tc := range []struct {
		name           string
		callerDeclares bool
	}{
		{name: "relay-meta-mem-caller-plain"},
		{name: "relay-meta-mem-caller-declares", callerDeclares: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := nonUserContext(t)
			var log wireLog
			peer, transport := newRawPeer(&log, true, "upstream ")
			peer.serve(ctx, t, func(msg rawMsg) []string {
				if replies, ok := rawHandshake(msg, false); ok {
					return replies
				}
				result := func(body string) []string {
					return []string{fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":%s}`, msg.ID, body)}
				}
				switch {
				case msg.Method == "tools/call":
					return result(`{"resultType":"task",` + declaredTaskFields + `}`)
				case msg.Method == "tools/list":
					return result(`{"tools":[{"name":"up","inputSchema":{"type":"object"}}],"resultType":"task",` + declaredTaskFields + `}`)
				case msg.Method == "prompts/get":
					return result(`{"messages":[{"role":"user","content":{"type":"text","text":"hi"}}],"resultType":"task",` + declaredTaskFields + `}`)
				case msg.Method == "resources/read":
					return result(`{"contents":[{"uri":"file:///a","text":"x"}],"resultType":"task",` + declaredTaskFields + `}`)
				case msg.Method == "ping":
					return result(`{"resultType":"task",` + declaredTaskFields + `}`)
				case msg.Method != "" && len(msg.ID) > 0:
					return []string{rawMethodNotFound(msg.ID, msg.Method)}
				}
				return nil
			})
			upstream, err := mcp.NewClient(&mcp.Implementation{Name: "relay-client", Version: "v1"}, nil).Connect(ctx, transport, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { upstream.Close() })
			server := mcp.NewServer(&mcp.Implementation{Name: "relay-server", Version: "v1"}, nil)
			schema := json.RawMessage(`{"type":"object"}`)
			text := func(s string) *mcp.CallToolResult {
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: s}}}
			}
			server.AddTool(&mcp.Tool{Name: "relay-call", InputSchema: schema}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return upstream.CallTool(ctx, &mcp.CallToolParams{Meta: req.Params.Meta, Name: "up"})
			})
			server.AddTool(&mcp.Tool{Name: "relay-call-describe", InputSchema: schema}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return text(guarded(func() string {
					return describeToolResult(upstream.CallTool(ctx, &mcp.CallToolParams{Meta: req.Params.Meta, Name: "up"}))
				})), nil
			})
			server.AddTool(&mcp.Tool{Name: "relay-list", InputSchema: schema}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return text(guarded(func() string {
					return describeResult(upstream.ListTools(ctx, &mcp.ListToolsParams{Meta: req.Params.Meta}))
				})), nil
			})
			server.AddTool(&mcp.Tool{Name: "relay-prompt", InputSchema: schema}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return text(guarded(func() string {
					return describeResult(upstream.GetPrompt(ctx, &mcp.GetPromptParams{Meta: req.Params.Meta, Name: "p"}))
				})), nil
			})
			server.AddTool(&mcp.Tool{Name: "relay-read", InputSchema: schema}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return text(guarded(func() string {
					return describeResult(upstream.ReadResource(ctx, &mcp.ReadResourceParams{Meta: req.Params.Meta, URI: "file:///a"}))
				})), nil
			})
			server.AddTool(&mcp.Tool{Name: "relay-ping", InputSchema: schema}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return text(fmt.Sprintf("err=%v", upstream.Ping(ctx, &mcp.PingParams{Meta: req.Params.Meta}))), nil
			})
			caller, callerTransport := newRawPeer(&log, false, "")
			serverSession, err := server.Connect(ctx, callerTransport, nil)
			if err != nil {
				t.Fatal(err)
			}
			meta := newMeta(tc.callerDeclares)
			id := 0
			next := func() int { id++; return id }
			caller.call(ctx, t, next(), "server/discover", "{"+meta+"}")
			for _, tool := range []string{"relay-call", "relay-call-describe", "relay-list", "relay-prompt", "relay-read", "relay-ping"} {
				log.section("%s", tool)
				caller.call(ctx, t, next(), "tools/call", fmt.Sprintf(`{%s,"name":%q,"arguments":{}}`, meta, tool))
			}
			caller.closeWrite()
			if err := serverSession.Wait(); err != nil {
				log.addf("server session ended: %v", err)
			}
			peer.mute()
			emitTranscript(t, tc.name, &log)
		})
	}
}
