// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// This file sorts after the other compat_nonuser files on purpose: Go runs
// the tests of a package in file order, and TestCompatNonUserGoldens looks at
// what the transcript tests before it compared.

package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestCompatNonUserReceivingMiddleware pins what the receiving middleware of
// a client that does not use the tasks extension is handed when the server
// sends task messages anyway: never a notifications/tasks, and subscription
// acknowledgements whose params carry no task ids, whatever taskIds the peer
// wrote. The expectations are the unchanged SDK's.
func TestCompatNonUserReceivingMiddleware(t *testing.T) {
	ctx := nonUserContext(t)
	var wire wireLog
	var mu sync.Mutex
	var handed []string
	client := mcp.NewClient(&mcp.Implementation{Name: "plain-client", Version: "v1"}, nil)
	client.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			params, err := json.Marshal(req.GetParams())
			if err != nil {
				params = []byte("<" + err.Error() + ">")
			}
			mu.Lock()
			handed = append(handed, method+" "+string(params))
			mu.Unlock()
			return next(ctx, method, req)
		}
	})
	peer, transport := newRawPeer(&wire, true, "")
	// The scripted server answers one tools/call with task messages, then a
	// ping. Messages are handled in the order they arrive, so once the
	// client has answered the ping it has handled all of them; only then
	// does the call get its result.
	var pending json.RawMessage
	served := peer.serve(ctx, t, func(msg rawMsg) []string {
		if replies, ok := rawHandshake(msg, false); ok {
			return replies
		}
		switch {
		case msg.Method == "tools/call":
			pending = msg.ID
			return []string{
				`{"jsonrpc":"2.0","method":"notifications/tasks","params":{"taskId":"t-1","status":"working","createdAt":"2026-01-02T03:04:05Z","lastUpdatedAt":"2026-01-02T03:04:06Z","ttlMs":null}}`,
				`{"jsonrpc":"2.0","method":"notifications/tasks","params":{"taskId":7}}`,
				`{"jsonrpc":"2.0","method":"notifications/tasks"}`,
				`{"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":3},"notifications":{"toolsListChanged":true,"taskIds":["t-1"]}}}`,
				`{"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"notifications":{"taskIds":[]}}}`,
				`{"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"notifications":{"taskIds":"t-1"}}}`,
				`{"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"notifications":{"taskIds":[7]}}}`,
				`{"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"notifications":{"taskIds":{"t-1":true}}}}`,
				`{"jsonrpc":"2.0","id":"srv-ping","method":"ping"}`,
			}
		case msg.isResponse():
			return []string{fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"content":[],"resultType":"complete"}}`, pending)}
		case msg.Method != "" && len(msg.ID) > 0:
			return []string{rawMethodNotFound(msg.ID, msg.Method)}
		}
		return nil
	})
	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "burst"}); err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	peer.mute()
	if err := session.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	waitFor(ctx, t, "the scripted server to see the client close", served)
	want := []string{
		`notifications/subscriptions/acknowledged {"_meta":{"io.modelcontextprotocol/subscriptionId":3},"notifications":{"toolsListChanged":true}}`,
		`notifications/subscriptions/acknowledged {"notifications":{}}`,
		`notifications/subscriptions/acknowledged {"notifications":{}}`,
		`notifications/subscriptions/acknowledged {"notifications":{}}`,
		`notifications/subscriptions/acknowledged {"notifications":{}}`,
		`ping null`,
	}
	mu.Lock()
	defer mu.Unlock()
	if got := strings.Join(handed, "\n"); got != strings.Join(want, "\n") {
		t.Errorf("the client's receiving middleware was handed\n%s\nwant, as in the unchanged SDK,\n%s\nwire:\n%s", got, strings.Join(want, "\n"), wire.String())
	}
}

// TestCompatNonUserAsyncResultMiddleware runs a plain tool call through a
// sending middleware that hands each result to another goroutine, as an
// asynchronous audit log does. Nothing may write to the result after the
// middleware chain has returned it. Run with -race.
func TestCompatNonUserAsyncResultMiddleware(t *testing.T) {
	ctx := nonUserContext(t)
	server := mcp.NewServer(&mcp.Implementation{Name: "plain-server", Version: "v1"}, nil)
	server.AddTool(&mcp.Tool{Name: "plain", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil
		})
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer serverSession.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "plain-client", Version: "v1"}, nil)
	logged := make(chan string, 64)
	client.AddSendingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			result, err := next(ctx, method, req)
			if method == "tools/call" && err == nil {
				go func() {
					data, _ := json.Marshal(result)
					logged <- string(data)
				}()
			}
			return result, err
		}
	})
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	for range 20 {
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "plain"})
		if err != nil {
			t.Fatal(err)
		}
		// The caller uses its result while the audit goroutine reads it.
		if _, err := json.Marshal(result); err != nil {
			t.Fatal(err)
		}
		waitFor(ctx, t, "the audit goroutine", logged)
	}
}

// nonUserTranscriptTests are the tests that compare transcripts with the
// goldens, and nonUserGoldenCount is how many goldens they compare.
var nonUserTranscriptTests = []string{
	"TestCompatNonUserClientCustomMethods",
	"TestCompatNonUserClientDecoding",
	"TestCompatNonUserClientPeerMessages",
	"TestCompatNonUserClientResults",
	"TestCompatNonUserClientSessions",
	"TestCompatNonUserJSON",
	"TestCompatNonUserRelayHTTP",
	"TestCompatNonUserRelayInMemory",
	"TestCompatNonUserRelayMeta",
	"TestCompatNonUserServerHTTP",
	"TestCompatNonUserServerHTTPMatrix",
	"TestCompatNonUserServerInMemory",
	"TestCompatNonUserServerSends",
	"TestCompatNonUserServerSessions",
}

const nonUserGoldenCount = 40

// TestCompatNonUserGoldens checks the golden directory itself: it holds
// exactly nonUserGoldenCount goldens, and, when all the transcript tests ran
// before this one (they do in a full run: this file sorts last), every one of
// them was compared. A golden no test compares, a scenario that stopped
// running, and a golden that went missing all fail here.
//
// When only some of the transcript tests ran (a -run filter, or -shuffle
// putting this test first), the second check is skipped and says so.
func TestCompatNonUserGoldens(t *testing.T) {
	if _, generating := nonUserGenerating(t); generating {
		t.Skip("generating goldens")
	}
	entries, err := os.ReadDir(nonUserGoldenDir)
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, entry := range entries {
		files = append(files, entry.Name())
	}
	sort.Strings(files)
	if len(files) != nonUserGoldenCount {
		t.Errorf("%s holds %d files, want %d goldens: %v", nonUserGoldenDir, len(files), nonUserGoldenCount, files)
	}
	nonUserRun.mu.Lock()
	defer nonUserRun.mu.Unlock()
	var notRun []string
	for _, test := range nonUserTranscriptTests {
		if !nonUserRun.tests[test] {
			notRun = append(notRun, test)
		}
	}
	if len(notRun) > 0 {
		t.Skipf("not checking that every golden was compared: these transcript tests have not run in this process: %v", notRun)
	}
	for _, file := range files {
		if !nonUserRun.goldens[file] {
			t.Errorf("no test compared %s", file)
		}
	}
	if len(nonUserRun.goldens) != nonUserGoldenCount {
		t.Errorf("the transcript tests compared %d goldens, want %d", len(nonUserRun.goldens), nonUserGoldenCount)
	}
}
