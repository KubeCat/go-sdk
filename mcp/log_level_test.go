// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"sync"
	"testing"
	"time"
)

// The log level of a 2026-07-28 request belongs to that request: a request
// that asks for warnings still gets its warning after another request on the
// same session, which asks for no logging, was handled.
//
// The subject is that of TestServerLogLevelDoesNotLeakBetweenNewProtocolRequests.
// Here the tools are added before the session exists and a client reads the
// server's messages, so no tools/list_changed notification is written to a
// connection nobody reads, and every wait fails the test instead of hanging
// it.
//
// In the second variant the test lags: the other request has been answered
// before the test looks for it at its tool. In the third the other request
// lags: it is still in flight when the first one logs.
func TestServerLogLevelIsPerRequest(t *testing.T) {
	const guard = time.Minute // hang guard only
	for _, variant := range []string{"other request answered", "other request answered, test behind", "other request in flight"} {
		lag := variant == "other request in flight"
		behind := variant == "other request answered, test behind"
		t.Run(variant, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*guard)
			t.Cleanup(cancel)

			server := NewServer(testImpl, nil)
			// The server's sending middleware sees a log message only if the
			// server did not suppress it; Log returns after it ran.
			var mu sync.Mutex
			var logged []LoggingLevel
			server.AddSendingMiddleware(func(next MethodHandler) MethodHandler {
				return func(ctx context.Context, method string, req Request) (Result, error) {
					if method == notificationLoggingMessage {
						mu.Lock()
						logged = append(logged, req.GetParams().(*LoggingMessageParams).Level)
						mu.Unlock()
					}
					return next(ctx, method, req)
				}
			})
			type gate struct {
				reached chan struct{}
				hold    chan struct{}
				release func()
			}
			newGate := func() *gate {
				g := &gate{reached: make(chan struct{}), hold: make(chan struct{})}
				g.release = sync.OnceFunc(func() { close(g.hold) })
				return g
			}
			logging, other := newGate(), newGate()
			if !lag {
				other.release()
			}
			AddTool(server, &Tool{Name: "log"}, func(ctx context.Context, req *CallToolRequest, _ any) (*CallToolResult, any, error) {
				close(logging.reached)
				<-logging.hold
				if err := req.Session.Log(ctx, &LoggingMessageParams{Level: "warning", Data: "request log"}); err != nil {
					return nil, nil, err
				}
				return &CallToolResult{Content: []Content{&TextContent{Text: "ok"}}}, nil, nil
			})
			AddTool(server, &Tool{Name: "other"}, func(ctx context.Context, req *CallToolRequest, _ any) (*CallToolResult, any, error) {
				close(other.reached)
				<-other.hold
				return &CallToolResult{Content: []Content{&TextContent{Text: "ok"}}}, nil, nil
			})

			clientTransport, serverTransport := NewInMemoryTransports()
			serverSession, err := server.Connect(ctx, serverTransport, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { serverSession.Close() })
			session, err := NewClient(testImpl, nil).Connect(ctx, clientTransport, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { session.Close() })
			if version := session.InitializeResult().ProtocolVersion; version != protocolVersion20260728 {
				t.Fatalf("session protocol version = %s, want %s", version, protocolVersion20260728)
			}
			// Registered after the sessions' cleanups, so the handlers are let
			// go before the sessions close.
			t.Cleanup(logging.release)
			t.Cleanup(other.release)

			call := func(params *CallToolParams) chan error {
				done := make(chan error, 1)
				go func() {
					_, err := session.CallTool(ctx, params)
					done <- err
				}()
				return done
			}
			// wait waits for a tool to be reached: ch is closed by its
			// handler. A call in calls ending without that is a failure,
			// reported at once.
			//
			// A handler closes ch before its call can be answered. So when
			// the test lags and a call's result is there too, the tool was
			// reached first: the result is left for finish.
			wait := func(what string, ch <-chan struct{}, calls ...chan error) {
				t.Helper()
				var first, second chan error
				if len(calls) > 0 {
					first = calls[0]
				}
				if len(calls) > 1 {
					second = calls[1]
				}
				ended := func(call chan error, which string, err error) {
					t.Helper()
					select {
					case <-ch:
						call <- err // one result per call, just taken: does not block
					default:
						t.Fatalf("waiting for %s: the %s call returned: %v", what, which, err)
					}
				}
				timer := time.NewTimer(guard)
				defer timer.Stop()
				select {
				case <-ch:
				case err := <-first:
					ended(first, "logging", err)
				case err := <-second:
					ended(second, "other", err)
				case <-timer.C:
					t.Fatalf("timed out waiting for %s", what)
				}
			}
			finish := func(what string, done <-chan error) {
				t.Helper()
				timer := time.NewTimer(guard)
				defer timer.Stop()
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("%s: %v", what, err)
					}
				case <-timer.C:
					t.Fatalf("timed out waiting for %s", what)
				}
			}

			loggingCall := call(&CallToolParams{Name: "log", Meta: Meta{MetaKeyLogLevel: LoggingLevel("warning")}})
			wait("the logging tool", logging.reached, loggingCall)
			otherCall := call(&CallToolParams{Name: "other"})
			if behind {
				// The other call is answered before the test looks.
				timer := time.NewTimer(guard)
				select {
				case err := <-otherCall:
					otherCall <- err
				case <-timer.C:
					t.Fatal("timed out waiting for the other call")
				}
				timer.Stop()
			}
			wait("the other tool", other.reached, loggingCall, otherCall)
			if !lag {
				finish("the other call", otherCall)
			}
			logging.release()
			finish("the logging call", loggingCall)
			// The handler's Log call returned before the call was answered.
			mu.Lock()
			got := append([]LoggingLevel(nil), logged...)
			mu.Unlock()
			if len(got) != 1 || got[0] != "warning" {
				t.Fatalf("log messages sent = %v, want one warning: the request's log level was lost when another request was handled", got)
			}
			if lag {
				other.release()
				finish("the other call", otherCall)
			}
		})
	}
}
