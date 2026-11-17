// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

type installTestParams struct {
	ParamsBase
	TaskID string `json:"taskId,omitempty"`
}

type installTestResult struct {
	ResultBase
	Value string `json:"value"`
}

func addInstallTestMethod(server *Server, method string) error {
	return AddReceivingCustomMethod(server, method, func(_ context.Context, _ *ServerSession, params *installTestParams) (*installTestResult, error) {
		return &installTestResult{Value: "own:" + params.TaskID}, nil
	})
}

// installTestSession connects a client that has the program's own
// registration for the three task method names.
func installTestSession(t *testing.T, server *Server) *ClientSession {
	t.Helper()
	ctx := taskTestContext(t)
	clientTransport, serverTransport := NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "serverSession", serverSession.Close)
	client := NewClient(&Implementation{Name: "install-test-client", Version: "v1.0.0"}, nil)
	for _, method := range taskMethods {
		if err := AddSendingCustomMethod[*installTestParams, *installTestResult](client, method); err != nil {
			t.Fatal(err)
		}
	}
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "session", session.Close)
	return session
}

// discoverAdvertisesTasks reports what server/discover told the client.
func discoverAdvertisesTasks(t *testing.T, session *ClientSession) bool {
	t.Helper()
	result := session.InitializeResult()
	if result == nil || result.ProtocolVersion < protocolVersion20260728 || result.Capabilities == nil {
		t.Fatalf("session did not discover a %s server: %+v", protocolVersion20260728, result)
	}
	_, ok := result.Capabilities.Extensions[ExtensionTasks]
	return ok
}

// callInstallTestMethod calls method with a request that declares the
// extension. The program's own handler answers "own:<id>"; the extension's
// answers an unknown id with invalid params.
func callInstallTestMethod(t *testing.T, session *ClientSession, method string) (string, error) {
	t.Helper()
	params := &installTestParams{TaskID: "task-1"}
	params.SetMeta(tasksRequestMeta())
	result, err := CallCustomMethod[*installTestParams, *installTestResult](taskTestContext(t), session, method, params)
	if err != nil {
		return "", err
	}
	return result.Value, nil
}

func TestNewTaskServerRefusesRegisteredMethods(t *testing.T) {
	for _, method := range taskMethods {
		t.Run(method, func(t *testing.T) {
			server := NewServer(testImpl, nil)
			if err := addInstallTestMethod(server, method); err != nil {
				t.Fatal(err)
			}
			tasks, err := NewTaskServer(server, nil)
			if err == nil || !strings.Contains(err.Error(), `"`+method+`" is already registered as a custom method`) {
				t.Fatalf("NewTaskServer = %v, %v; want an error naming %q", tasks, err, method)
			}
			if server.tasks.Load() != nil {
				t.Fatal("refused install left a TaskServer on the server")
			}
			for _, owned := range taskMethods {
				if _, ok := server.receivingMethodInfos()[owned]; ok != (owned == method) {
					t.Fatalf("after the refused install, %q registered = %v", owned, ok)
				}
			}
			session := installTestSession(t, server)
			if discoverAdvertisesTasks(t, session) {
				t.Fatal("server/discover advertises tasks after a refused install")
			}
			if got, err := callInstallTestMethod(t, session, method); err != nil || got != "own:task-1" {
				t.Fatalf("the program's %s = %q, %v; want its own handler", method, got, err)
			}
		})
	}
}

func TestTaskServerReservesMethods(t *testing.T) {
	server := NewServer(testImpl, nil)
	if _, err := NewTaskServer(server, nil); err != nil {
		t.Fatal(err)
	}
	for _, method := range taskMethods {
		err := addInstallTestMethod(server, method)
		if err == nil || !strings.Contains(err.Error(), `"`+method+`" is reserved by the installed tasks extension`) {
			t.Fatalf("AddReceivingCustomMethod(%q) = %v, want a reserved-method error", method, err)
		}
	}
	if err := addInstallTestMethod(server, "tasks/inventory"); err != nil {
		t.Fatalf("a method the extension does not own was refused: %v", err)
	}
	session := installTestSession(t, server)
	if !discoverAdvertisesTasks(t, session) {
		t.Fatal("server/discover does not advertise the installed extension")
	}
	for _, method := range taskMethods {
		if _, err := callInstallTestMethod(t, session, method); errorCodeForTaskTest(err) != jsonrpc.CodeInvalidParams {
			t.Fatalf("%s error = %v, want the extension's invalid-params answer", method, err)
		}
	}
	if got, err := callInstallTestMethod(t, session, "tasks/inventory"); err == nil || got != "" {
		// The client did not register tasks/inventory for sending.
		t.Fatalf("unregistered sending method = %q, %v", got, err)
	}
}

// NewTaskServer and AddReceivingCustomMethod for an owned method, started
// together: exactly one of them is refused, whichever order they run in, and
// the server's answers agree with the winner. The assertion is on state; the
// common gate only makes both orders likely.
func TestNewTaskServerRacesCustomMethod(t *testing.T) {
	for range 50 {
		server := NewServer(testImpl, nil)
		start := make(chan struct{})
		var installErr, customErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, installErr = NewTaskServer(server, nil)
		}()
		go func() {
			defer wg.Done()
			<-start
			customErr = addInstallTestMethod(server, methodGetTask)
		}()
		close(start)
		wg.Wait()
		if (installErr == nil) == (customErr == nil) {
			t.Fatalf("NewTaskServer = %v and AddReceivingCustomMethod = %v; want exactly one refused", installErr, customErr)
		}
		session := installTestSession(t, server)
		installed := installErr == nil
		if got := discoverAdvertisesTasks(t, session); got != installed {
			t.Fatalf("server/discover advertises tasks = %v, installed = %v", got, installed)
		}
		got, err := callInstallTestMethod(t, session, methodGetTask)
		if installed {
			if errorCodeForTaskTest(err) != jsonrpc.CodeInvalidParams {
				t.Fatalf("installed: tasks/get = %q, %v; want the extension's answer", got, err)
			}
		} else if err != nil || got != "own:task-1" {
			t.Fatalf("not installed: tasks/get = %q, %v; want the program's handler", got, err)
		}
	}
}

// A session takes the server's method tables under the server's lock and
// indexes them after releasing it. Installing with a live session must not
// write the tables those sessions hold. Run with -race.
func TestNewTaskServerWithLiveSession(t *testing.T) {
	ctx := taskTestContext(t)
	server := NewServer(testImpl, nil)
	_, serverTransport := NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeTaskSession(t, "serverSession", serverSession.Close)
	sending := serverSession.sendingMethodInfos()
	receiving := server.receivingMethodInfos()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := NewTaskServer(server, nil); err != nil {
			t.Error(err)
		}
	}()
	_, sendsTasks := sending[notificationTaskStatus]
	_, receivesTasks := receiving[methodGetTask]
	waitTask(t, done, "NewTaskServer")
	if sendsTasks || receivesTasks {
		t.Fatalf("tables taken before the install changed: sends %v, receives %v", sendsTasks, receivesTasks)
	}
	if _, ok := server.receivingMethodInfos()[methodGetTask]; !ok {
		t.Fatal("tasks/get is not registered after the install")
	}
}
