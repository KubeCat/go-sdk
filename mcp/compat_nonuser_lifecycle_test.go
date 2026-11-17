// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestCompatNonUserServerSessions records session start, subscribe,
// unsubscribe, session end and further sessions on one server that has no
// tasks extension, over Streamable HTTP with sessions.
func TestCompatNonUserServerSessions(t *testing.T) {
	ctx := nonUserContext(t)
	var log wireLog
	sessions := 0
	server := newPlainServer(t, plainServerConfig{custom: true, sessionIDs: func() string {
		sessions++
		return fmt.Sprintf("session-%d", sessions)
	}})
	probe := newHTTPProbe(t, &log, mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return server.Server },
		&mcp.StreamableHTTPOptions{JSONResponse: true}))
	id := 0
	request := func(method, params string) string {
		id++
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":%q,"params":%s}`, id, method, params)
	}
	in := func(session string) http.Header {
		return hdr("Mcp-Protocol-Version", nonUserLegacy, "Mcp-Session-Id", session)
	}
	// start opens a session and returns the id the server gave it.
	start := func(declare bool) string {
		_, header, _ := probe.post(ctx, t, hdr(), request("initialize", legacyInitializeParams(nonUserLegacy, declare)))
		session := header.Get("Mcp-Session-Id")
		if session == "" {
			t.Fatal("initialize returned no Mcp-Session-Id")
		}
		probe.post(ctx, t, in(session), `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
		return session
	}
	work := func(session string) {
		probe.post(ctx, t, in(session), request("tools/call", `{"name":"echo","arguments":{"a":1},"task":{"ttl":1000}}`))
		probe.post(ctx, t, in(session), request("tasks/get", `{"taskId":"t-1","value":"v"}`))
		probe.post(ctx, t, in(session), request("tasks/cancel", `{"_meta":{"io.modelcontextprotocol/clientCapabilities":`+nonUserCapsTasks+`},"taskId":"t-1"}`))
	}

	log.section("server/discover on a server with sessions")
	for _, declare := range []bool{false, true} {
		probe.post(ctx, t, hdr("Mcp-Protocol-Version", nonUserNewest, "Mcp-Method", "server/discover"), request("server/discover", "{"+newMeta(declare)+"}"))
	}

	log.section("first session")
	first := start(true)
	stream := probe.open(ctx, t, http.MethodGet, http.Header{"Accept": {"text/event-stream"}, "Mcp-Protocol-Version": {nonUserLegacy}, "Mcp-Session-Id": {first}}, "")
	// The stream opens with a comment; read it so that the next event read
	// is the notification.
	stream.event(t)
	work(first)
	probe.post(ctx, t, in(first), request("resources/subscribe", `{"uri":"`+nonUserNoteURI+`"}`))
	log.addf("go SubscribeHandler uri=%s", waitFor(ctx, t, "SubscribeHandler", server.subscribed))
	log.addf("go ResourceUpdated err=%v", server.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: nonUserNoteURI}))
	stream.event(t)
	probe.post(ctx, t, in(first), request("resources/unsubscribe", `{"uri":"`+nonUserNoteURI+`"}`))
	log.addf("go UnsubscribeHandler uri=%s", waitFor(ctx, t, "UnsubscribeHandler", server.unsubscribed))
	log.addf("go ResourceUpdated err=%v", server.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: nonUserNoteURI}))

	log.section("second session while the first is open")
	second := start(false)
	work(second)

	log.section("end of the first session")
	probe.do(ctx, t, http.MethodDelete, in(first), "")
	for stream.event(t) {
	}
	stream.close()
	probe.post(ctx, t, in(first), request("tools/call", `{"name":"echo","arguments":{}}`))

	log.section("the second session goes on")
	work(second)
	probe.post(ctx, t, in(second), request("resources/subscribe", `{"uri":"`+nonUserNoteURI+`"}`))
	log.addf("go SubscribeHandler uri=%s", waitFor(ctx, t, "SubscribeHandler", server.subscribed))
	probe.do(ctx, t, http.MethodDelete, in(second), "")
	log.addf("go ResourceUpdated err=%v", server.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: nonUserNoteURI}))

	log.section("third session after both ended")
	third := start(true)
	work(third)
	probe.do(ctx, t, http.MethodDelete, in(third), "")
	emitTranscript(t, "server-sessions-http", &log)
}

// TestCompatNonUserClientSessions records connect, subscribe, unsubscribe,
// close and a second session of one client that did not opt in to tasks,
// against a scripted server that names tasks in every acknowledgement.
//
// The client's last messages at Close have no order in the unchanged SDK itself (the
// cancellation of a listen stream races with closing the connection), so the
// scripted server stops logging just before Close.
func TestCompatNonUserClientSessions(t *testing.T) {
	for _, transportKind := range []string{"mem", "http"} {
		name := "client-sessions-" + transportKind
		t.Run(name, func(t *testing.T) {
			ctx := nonUserContext(t)
			var log wireLog
			updated := make(chan string, 16)
			toolsChanged := make(chan struct{}, 16)
			client := mcp.NewClient(&mcp.Implementation{Name: "plain-client", Version: "v1"}, &mcp.ClientOptions{
				ResourceUpdatedHandler: func(_ context.Context, req *mcp.ResourceUpdatedNotificationRequest) {
					updated <- req.Params.URI
				},
				ToolListChangedHandler: func(context.Context, *mcp.ToolListChangedRequest) {
					toolsChanged <- struct{}{}
				},
			})
			for session := 1; session <= 2; session++ {
				log.section("session %d", session)
				script := newPeerScript(false)
				holds := make(chan chan struct{}, 16)
				var transport mcp.Transport
				var mute func()
				var served <-chan struct{}
				if transportKind == "mem" {
					peer, tr := newRawPeer(&log, true, "")
					served = peer.serve(ctx, t, func(msg rawMsg) []string {
						defer script.note(msg)
						return script.handle(msg)
					})
					transport, mute = tr, peer.mute
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
						}
						return jsonReply(replies[0])
					})
					transport, mute = &mcp.StreamableClientTransport{Endpoint: srv.url, DisableStandaloneSSE: true, MaxRetries: -1}, srv.mute
				}
				cs, err := client.Connect(ctx, transport, nil)
				// Connect opens a listen stream for tools/list_changed and
				// returns; the change notification is the last thing the
				// scripted server sends for it.
				waitFor(ctx, t, "ToolListChangedHandler", toolsChanged)
				log.addf("go Connect err=%v", err)
				log.addf("go ToolListChangedHandler called")
				if err != nil {
					t.Fatal(err)
				}
				var connectListen chan struct{}
				if transportKind == "http" {
					connectListen = waitFor(ctx, t, "the listen stream opened by Connect", holds)
				}
				log.addf("go CallTool(task) %s", guarded(func() string {
					return describeToolResult(cs.CallTool(ctx, &mcp.CallToolParams{Name: "task"}))
				}))
				for i, ack := range listenAcks[:2] {
					uri := fmt.Sprintf("fixture://note-%d-%d", session, i)
					script.acks <- ack
					subErr := cs.Subscribe(ctx, &mcp.SubscribeParams{URI: uri})
					gotURI := waitFor(ctx, t, "ResourceUpdatedHandler", updated)
					log.addf("go Subscribe(%s) err=%v", uri, subErr)
					log.addf("go ResourceUpdatedHandler uri=%s", gotURI)
					unsubErr := cs.Unsubscribe(ctx, &mcp.UnsubscribeParams{URI: uri})
					script.expectNote(ctx, t, &log, "notifications/cancelled")
					if transportKind == "http" {
						waitFor(ctx, t, "the client to leave the listen stream", waitFor(ctx, t, "the listen stream", holds))
						log.addf("go the client left the listen stream")
					}
					log.addf("go Unsubscribe(%s) err=%v", uri, unsubErr)
				}
				log.addf("go CallTool(plain) %s", guarded(func() string {
					return describeToolResult(cs.CallTool(ctx, &mcp.CallToolParams{Name: "plain"}))
				}))
				mute()
				closeErr := cs.Close()
				if served != nil {
					waitFor(ctx, t, "the scripted server to see the client close", served)
				}
				if connectListen != nil {
					waitFor(ctx, t, "the client to leave the listen stream opened by Connect", connectListen)
				}
				log.addf("go Close err=%v", closeErr)
			}
			emitTranscript(t, name, &log)
		})
	}
}
