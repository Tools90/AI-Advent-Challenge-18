package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type MCPClient struct {
	session *mcp.ClientSession
	tools   []*mcp.Tool
}

func NewMCPCommandClient(ctx context.Context, serverName, command string, args ...string) (*MCPClient, error) {
	cmd := exec.Command(command, args...)
	client := mcp.NewClient(&mcp.Implementation{Name: "day17-agent", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		return nil, fmt.Errorf("MCP connect: %w", err)
	}
	toolsResult, err := session.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		_ = session.Close()
		return nil, fmt.Errorf("MCP tools/list: %w", err)
	}
	return &MCPClient{session: session, tools: toolsResult.Tools}, nil
}

func (m *MCPClient) Close() error {
	if m == nil || m.session == nil {
		return nil
	}
	return m.session.Close()
}

func (m *MCPClient) Tools() []*mcp.Tool {
	return append([]*mcp.Tool{}, m.tools...)
}

func (m *MCPClient) Call(ctx context.Context, name string, args map[string]any) (any, error) {
	result, err := m.session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		return nil, fmt.Errorf("MCP tools/call %s: %w", name, err)
	}
	if result.StructuredContent != nil {
		return result.StructuredContent, nil
	}
	texts := []string{}
	for _, content := range result.Content {
		if text, ok := content.(*mcp.TextContent); ok {
			texts = append(texts, text.Text)
		}
	}
	return map[string]any{"content": texts, "isError": result.IsError}, nil
}

func addMCPTools(tools map[string]Tool, client *MCPClient, serverName string) {
	if client == nil {
		return
	}
	for _, mcpTool := range client.Tools() {
		name := "mcp__" + serverName + "__" + mcpTool.Name
		schema := schemaMap(mcpTool.InputSchema)
		toolName := mcpTool.Name
		tools[name] = Tool{
			Schema: ToolSchema{
				Type: "function",
				Function: ToolFunction{
					Name:        name,
					Description: "[MCP " + serverName + "] " + mcpTool.Description,
					Parameters:  schema,
				},
			},
			Run: func(ctx context.Context, raw json.RawMessage) (any, error) {
				args := map[string]any{}
				if len(raw) > 0 {
					if err := json.Unmarshal(raw, &args); err != nil {
						return nil, fmt.Errorf("MCP arguments: %w", err)
					}
				}
				return client.Call(ctx, toolName, args)
			},
		}
	}
}

func schemaMap(value any) map[string]any {
	if schema, ok := value.(map[string]any); ok {
		return schema
	}
	data, err := json.Marshal(value)
	if err == nil {
		var schema map[string]any
		if json.Unmarshal(data, &schema) == nil && schema != nil {
			return schema
		}
	}
	return map[string]any{"type": "object", "properties": map[string]any{}}
}

func mcpStatus(client *MCPClient, serverName string, err error) map[string]any {
	status := "connected"
	tools := []string{}
	if err != nil {
		status = "error"
	} else if client != nil {
		for _, tool := range client.Tools() {
			tools = append(tools, tool.Name)
		}
	}
	return map[string]any{
		"status":     status,
		"server":     serverName,
		"tools":      tools,
		"error":      errorString(err),
		"checked_at": time.Now().Format(time.RFC3339),
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
