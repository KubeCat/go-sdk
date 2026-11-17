// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package mcp_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// A resultCase is one tools/call result as it could arrive on the wire.
type resultCase struct {
	name string
	json string
}

const taskFieldsJSON = `"status":"working","statusMessage":"busy","createdAt":"2026-01-02T03:04:05Z","lastUpdatedAt":"2026-01-02T03:04:06Z","ttlMs":60000,"pollIntervalMs":500`

// resultCases is every resultType crossed with every result shape. None of
// these is something a program asked for by opting in to tasks: they are what
// a peer may send regardless.
func resultCases() []resultCase {
	resultTypes := []struct{ name, member string }{
		{"absent", ``},
		{"complete", `"resultType":"complete"`},
		{"input_required", `"resultType":"input_required"`},
		{"task", `"resultType":"task"`},
		{"mystery", `"resultType":"mystery"`},
	}
	const content = `"content":[{"type":"text","text":"hi"}]`
	shapes := []struct{ name, members string }{
		{"bare", ``},
		{"content", content},
		{"taskfields", `"taskId":"t-1",` + taskFieldsJSON},
		{"taskid7", `"taskId":7,` + taskFieldsJSON},
		{"notaskid", taskFieldsJSON},
		{"content+taskfields", content + `,"taskId":"t-1",` + taskFieldsJSON},
		{"ttlnull", `"taskId":"t-1","status":"working","ttlMs":null`},
		{"badtypes", `"taskId":"t-1","status":5,"createdAt":12,"ttlMs":"soon","pollIntervalMs":"often"`},
		{"meta+taskfields", `"_meta":{"fixture/key":"v"},"taskId":"t-1",` + taskFieldsJSON},
		{"full+taskfields", content + `,"structuredContent":{"a":1},"isError":true,"taskId":"t-1",` + taskFieldsJSON},
		{"inputreq-empty", `"inputRequests":{}`},
		{"inputreq-roots+taskfields", `"inputRequests":{"r1":{"method":"roots/list","params":{}}},"requestState":"st-1","taskId":"t-1",` + taskFieldsJSON},
	}
	var cases []resultCase
	for _, rt := range resultTypes {
		for _, shape := range shapes {
			var members []string
			for _, m := range []string{rt.member, shape.members} {
				if m != "" {
					members = append(members, m)
				}
			}
			cases = append(cases, resultCase{
				name: rt.name + "/" + shape.name,
				json: "{" + strings.Join(members, ",") + "}",
			})
		}
	}
	return cases
}

// roundTrip decodes in as a T and encodes it again.
func roundTrip[T any](in string) string {
	return guarded(func() string {
		v := new(T)
		if err := json.Unmarshal([]byte(in), v); err != nil {
			return fmt.Sprintf("unmarshalErr=%q", err.Error())
		}
		out, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("marshalErr=%q", err.Error())
		}
		// A second trip shows whether the encoding is a fixed point.
		v2 := new(T)
		if err := json.Unmarshal(out, v2); err != nil {
			return fmt.Sprintf("%s secondUnmarshalErr=%q", out, err.Error())
		}
		out2, err := json.Marshal(v2)
		if err != nil {
			return fmt.Sprintf("%s secondMarshalErr=%q", out, err.Error())
		}
		if string(out2) != string(out) {
			return fmt.Sprintf("%s secondTrip=%s", out, out2)
		}
		return string(out)
	})
}

func marshalValue(v any) string {
	return guarded(func() string {
		out, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("marshalErr=%q", err.Error())
		}
		return string(out)
	})
}

// TestCompatNonUserJSON round-trips shared types through encoding/json. No
// connection is involved: this is what a proxy, a cache or a log sees.
func TestCompatNonUserJSON(t *testing.T) {
	var log wireLog

	log.section("CallToolResult: wire JSON, unmarshal, marshal")
	for _, c := range resultCases() {
		log.addf("%s\n  in  %s\n  out %s", c.name, c.json, roundTrip[mcp.CallToolResult](c.json))
		var res mcp.CallToolResult
		if err := json.Unmarshal([]byte(c.json), &res); err == nil {
			log.addf("  go  needsInput=%v isError=%v content=%d structured=%v meta=%v inputRequests=%d requestState=%q",
				res.NeedsInput(), res.IsError, len(res.Content), res.StructuredContent != nil, res.Meta, len(res.InputRequests), res.RequestState)
		}
	}

	log.section("CallToolResult: Go values a program written for the unchanged SDK can construct")
	for _, v := range []struct {
		name  string
		value *mcp.CallToolResult
	}{
		{"zero", &mcp.CallToolResult{}},
		{"content", &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "hi"}}}},
		{"meta", &mcp.CallToolResult{Meta: mcp.Meta{"taskId": "t-1", "status": "working"}}},
		{"error", &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: "bad"}}}},
		{"structured-task-shaped", &mcp.CallToolResult{StructuredContent: map[string]any{"taskId": "t-1", "status": "working", "resultType": "task"}}},
	} {
		log.addf("%s\n  out %s", v.name, marshalValue(v.value))
	}

	metaTasks := `"_meta":{"io.modelcontextprotocol/clientCapabilities":` + nonUserCapsTasks + `}`
	section := func(title string, trip func(string) string, inputs ...string) {
		log.section("%s", title)
		for _, in := range inputs {
			log.addf("  in  %s\n  out %s", in, trip(in))
		}
	}

	section("CallToolParams", roundTrip[mcp.CallToolParams],
		`{"name":"t"}`,
		`{"name":"t","arguments":{"a":1}}`,
		`{`+metaTasks+`,"name":"t","arguments":{}}`,
		`{`+metaTasks+`,"name":"t","arguments":{},"task":{"ttl":1000}}`,
		`{"name":"t","useTasks":true,"UseTasks":true}`,
		`{"name":"t","taskId":"t-1"}`,
		`{"name":"t","requestState":"s","inputResponses":{}}`,
	)
	section("CallToolParamsRaw", roundTrip[mcp.CallToolParamsRaw],
		`{"name":"t"}`,
		`{`+metaTasks+`,"name":"t","arguments":{"a":1}}`,
		`{"name":"t","useTasks":true,"UseTasks":true,"task":{"ttl":1000}}`,
	)
	log.section("CallToolParams: Go values")
	for _, v := range []*mcp.CallToolParams{
		{},
		{Name: "t"},
		{Name: "t", Arguments: map[string]any{"a": 1}},
		{Name: "t", Meta: mcp.Meta{mcp.MetaKeyClientCapabilities: map[string]any{"extensions": map[string]any{nonUserTasksExt: map[string]any{}}}}},
	} {
		log.addf("  out %s", marshalValue(v))
	}

	subscriptionInputs := []string{
		`{}`,
		`{"toolsListChanged":true,"resourceSubscriptions":["fixture://note"]}`,
		`{"taskIds":["t-1"]}`,
		`{"taskIds":[]}`,
		`{"taskIds":null}`,
		`{"taskIds":"t-1"}`,
		`{"taskIds":[7]}`,
		`{"taskIds":{"t-1":true}}`,
		`{"resourceSubscriptions":["fixture://note"],"taskIds":["t-1","t-2"]}`,
	}
	section("NotificationSubscriptions", roundTrip[mcp.NotificationSubscriptions], subscriptionInputs...)
	var wrapped []string
	for _, in := range subscriptionInputs {
		wrapped = append(wrapped, `{"notifications":`+in+`}`)
	}
	section("SubscriptionsListenParams", roundTrip[mcp.SubscriptionsListenParams], append(wrapped, `{`+metaTasks+`,"notifications":{"taskIds":["t-1"]}}`)...)
	section("SubscriptionsAcknowledgedParams", roundTrip[mcp.SubscriptionsAcknowledgedParams], append(wrapped, `{"_meta":{"io.modelcontextprotocol/subscriptionId":3},"notifications":{"taskIds":["t-1"]}}`)...)
	section("SubscriptionsListenResult", roundTrip[mcp.SubscriptionsListenResult],
		`{"resultType":"complete","_meta":{}}`,
		`{"resultType":"task","_meta":{},"taskId":"t-1"}`,
	)
	log.section("NotificationSubscriptions: Go values")
	for _, v := range []*mcp.NotificationSubscriptions{
		{},
		{ToolsListChanged: true, PromptsListChanged: true, ResourcesListChanged: true, ResourceSubscriptions: []string{nonUserNoteURI}},
	} {
		log.addf("  out %s", marshalValue(v))
	}

	capabilityInputs := []string{
		`{}`,
		`{"tools":{"listChanged":true}}`,
		`{"extensions":{"io.modelcontextprotocol/tasks":{}}}`,
		`{"extensions":{"io.modelcontextprotocol/tasks":{"x":1},"fixture/ext":{}}}`,
		`{"tasks":{"list":{},"cancel":{},"requests":{"tools":{"call":{}}}}}`,
		`{"experimental":{"tasks":{}}}`,
	}
	section("ServerCapabilities", roundTrip[mcp.ServerCapabilities], capabilityInputs...)
	section("ClientCapabilities", roundTrip[mcp.ClientCapabilities], capabilityInputs...)
	section("DiscoverResult", roundTrip[mcp.DiscoverResult],
		`{"resultType":"complete","supportedVersions":["2026-07-28"],"capabilities":{"tools":{}}}`,
		`{"resultType":"complete","supportedVersions":["2026-07-28"],"capabilities":{"tools":{},"extensions":{"io.modelcontextprotocol/tasks":{}}}}`,
		`{"resultType":"task","taskId":"t-1","supportedVersions":["2026-07-28"],"capabilities":{"tasks":{}}}`,
		`{"supportedVersions":[],"capabilities":null}`,
	)
	section("InitializeResult", roundTrip[mcp.InitializeResult],
		`{"protocolVersion":"2025-11-25","capabilities":{"extensions":{"io.modelcontextprotocol/tasks":{}}},"serverInfo":{"name":"n","version":"v"}}`,
		`{"protocolVersion":"2025-11-25","capabilities":{"tasks":{"list":{}}},"serverInfo":{"name":"n","version":"v"}}`,
	)
	section("InitializeParams", roundTrip[mcp.InitializeParams],
		`{"protocolVersion":"2025-11-25","capabilities":{"extensions":{"io.modelcontextprotocol/tasks":{}}},"clientInfo":{"name":"n","version":"v"}}`,
	)
	section("Tool", roundTrip[mcp.Tool],
		`{"name":"t","inputSchema":{"type":"object"}}`,
		`{"name":"t","inputSchema":{"type":"object"},"execution":{"taskSupport":"optional"}}`,
	)
	section("ListToolsResult", roundTrip[mcp.ListToolsResult],
		`{"tools":[{"name":"t","inputSchema":{"type":"object"},"execution":{"taskSupport":"required"}}]}`,
		`{"resultType":"task","taskId":"t-1","tools":[]}`,
	)
	section("GetPromptResult", roundTrip[mcp.GetPromptResult],
		`{"messages":[]}`,
		`{"resultType":"task","taskId":"t-1",`+taskFieldsJSON+`}`,
	)
	section("ReadResourceResult", roundTrip[mcp.ReadResourceResult],
		`{"contents":[]}`,
		`{"resultType":"task","taskId":"t-1",`+taskFieldsJSON+`}`,
	)
	section("CreateMessageResult", roundTrip[mcp.CreateMessageResult],
		`{"resultType":"task","taskId":"t-1","role":"assistant","model":"m","content":{"type":"text","text":"x"}}`,
	)
	section("ElicitResult", roundTrip[mcp.ElicitResult],
		`{"resultType":"task","taskId":"t-1","action":"accept"}`,
	)

	emitTranscript(t, "json", &log)
}
