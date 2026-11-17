// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	internaljson "github.com/modelcontextprotocol/go-sdk/internal/json"
	"github.com/modelcontextprotocol/go-sdk/internal/jsonrpc2"
)

// A taskSend replaces the terminal of the sending middleware chain for one
// request of the tasks extension. It is how the extension sends a method the
// session's method table does not know, or encodes and decodes one request
// differently, without registering anything on the Client or the Server.
//
// The sending chain's context is handed to user code: sending middleware gets
// it, and so do the elicitation, sampling and roots handlers a multi
// round-trip call runs. Such code may make further calls with that context, on
// this session or on another one. So the context value alone decides nothing:
// the override applies only to the request it was made for.
type taskSend struct {
	// session is the session the request was built for.
	session Session
	method  string
	// owns reports whether params are the params of the request that opted
	// in. For tools/call that is UseTasks (which survives the copy a multi
	// round-trip retry makes); for every other method it is pointer identity
	// with the params the extension put in the request.
	owns func(Params) bool
	// send sends req on req's own session and returns the result. It must not
	// capture a session or a connection.
	send func(ctx context.Context, req Request) (Result, error)
}

type taskSendKey struct{}

// withTaskSend returns a context carrying send. A nested task request sets
// its own value, which shadows the outer one for its own chain.
func withTaskSend(ctx context.Context, send taskSend) context.Context {
	return context.WithValue(ctx, taskSendKey{}, &send)
}

// taskSendFromContext returns the terminal for req, or nil when req has
// nothing to do with the extension. A nil result means the caller sends req
// exactly as it would without the extension.
//
// There are two terminals. The first is the one a task call put in ctx, for
// the request it was made for: same method, same session, and params the
// value owns. The second is for any other standard request of a client whose
// own capabilities declare the extension (see [requestDeclaresTasks]): it
// sends the request as usual and refuses a task as the answer, since only
// tools/call can be answered with one.
func taskSendFromContext(ctx context.Context, method string, req Request) func(context.Context, Request) (Result, error) {
	if send, ok := ctx.Value(taskSendKey{}).(*taskSend); ok && send.method == method && req.GetSession() == send.session && send.owns(req.GetParams()) {
		return send.send
	}
	if requestDeclaresTasks(method, req) {
		return func(ctx context.Context, req Request) (Result, error) {
			return sendDeclaredRequest(ctx, method, req)
		}
	}
	return nil
}

// requestDeclaresTasks reports whether req is a request of a standard method,
// other than tools/call, of a client that declares the extension itself: the
// client's ClientOptions.Capabilities name the extension, the session's
// protocol version has it, and the request carries the declaration in its
// _meta. Every request of such a client carries it, unless the caller gave
// the request client capabilities of its own that leave the extension out.
//
// The key is what the program says about itself. Client capabilities that
// are only in a request's _meta do not make this true: a relay that hands
// the _meta of the request it is serving on to another server never named
// the extension, and its requests are answered as they are without it.
// Nothing a server sends makes this true either.
//
// A method the program registered itself (AddSendingCustomMethod) is never
// one of these: the program defined that request and the type of its result,
// and gets the answer in that type as it does without the extension.
func requestDeclaresTasks(method string, req Request) bool {
	cs, ok := req.GetSession().(*ClientSession)
	if !ok || cs == nil || !cs.usesNewProtocol() || !clientDeclaresTasks(cs.client) {
		return false
	}
	if _, standard := serverMethodInfos[method]; !standard {
		return false
	}
	if method == methodCallTool || method == methodSubscriptionsListen || strings.HasPrefix(method, "notifications/") {
		return false
	}
	params := req.GetParams()
	if v := reflect.ValueOf(params); !v.IsValid() || v.Kind() == reflect.Pointer && v.IsNil() {
		return false
	}
	if _, ok := params.(*InitializeParams); ok {
		return false
	}
	meta := params.GetMeta()
	if caps, ok := meta[MetaKeyClientCapabilities].(*clientCapabilitiesV2); ok {
		// The value the client itself puts in every request: no decoding.
		if caps == nil {
			return false
		}
		_, declared := caps.Extensions[ExtensionTasks]
		return declared
	}
	return metaUsesTasks(meta)
}

// A taskCheckedResult decodes a result as its own type does, and notes
// whether the answer says it is a task.
//
// Both steps use the decoder every other result is decoded with (the
// connection's, which matches field names by their exact case): an answer
// that is not a task gives the value, or the error, it gives without the
// extension.
type taskCheckedResult struct {
	Result
	task bool
}

func (r *taskCheckedResult) UnmarshalJSON(data []byte) error {
	var wire struct {
		ResultType resultType `json:"resultType"`
	}
	// Lenient: an answer this cannot read is not a task, and the result's
	// own decoding reports what is wrong with it.
	if err := internaljson.Unmarshal(data, &wire); err == nil {
		r.task = wire.ResultType == resultTypeTask
	}
	return internaljson.Unmarshal(data, r.Result)
}

// errTaskForRequest is the error of a request answered with a task that
// cannot be: a client that uses the extension must take such an answer as an
// invalid response.
func errTaskForRequest(method string) error {
	return fmt.Errorf("calling %q: invalid response: the server answered with a task, which only %s can be answered with", method, methodCallTool)
}

// sendDeclaredRequest is the terminal of a standard request of a client that
// declares the extension (see [requestDeclaresTasks]): one that cannot be
// answered with a task. It sends the request as the
// standard terminal does, and returns an error if the answer is a task.
func sendDeclaredRequest(ctx context.Context, method string, req Request) (Result, error) {
	info, ok := req.GetSession().sendingMethodInfos()[method]
	if !ok {
		return nil, jsonrpc2.ErrNotHandled
	}
	result := &taskCheckedResult{Result: info.newResult()}
	if err := call(ctx, req.GetSession().getConn(), method, req.GetParams(), result); err != nil {
		return nil, err
	}
	if result.task {
		return nil, errTaskForRequest(method)
	}
	return result.Result, nil
}

// taskNotify writes a notification of the extension to req's connection, and
// reports a closed connection as the standard terminal does for every other
// notification.
func taskNotify(ctx context.Context, req Request, method string, params any) error {
	err := req.GetSession().getConn().Notify(ctx, method, params)
	if errors.Is(err, jsonrpc2.ErrConnClosing) {
		if cause := withoutClosingErrors(err); cause != nil {
			return fmt.Errorf("%w: %w", ErrConnectionClosed, cause)
		}
		return ErrConnectionClosed
	}
	return err
}

// ownsParams returns a [taskSend] owns function that matches params by
// identity.
func ownsParams(params Params) func(Params) bool {
	return func(p Params) bool { return p == params }
}

type taskNameKey struct{}

type taskName struct {
	method string
	name   string
}

// withTaskName returns the context a task request is written to the
// connection with: over Streamable HTTP the request carries Mcp-Name: name.
// It is called only inside a [taskSend] terminal, below every middleware and
// every user handler, so no context a program can see ever carries the value.
//
// A name that is not header-safe is Base64-wrapped, as the standard headers
// wrap the name of a tool, a prompt or a resource.
func withTaskName(ctx context.Context, method, name string) context.Context {
	if encoded, ok := encodeHeaderValue(name); ok {
		name = encoded
	}
	return context.WithValue(ctx, taskNameKey{}, taskName{method: method, name: name})
}

// taskNameFromContext returns the Mcp-Name value for an outgoing request of
// the given method, if the context was made by [withTaskName] for it.
func taskNameFromContext(ctx context.Context, method string) (string, bool) {
	name, ok := ctx.Value(taskNameKey{}).(taskName)
	if !ok || name.method != method {
		return "", false
	}
	return name.name, true
}
