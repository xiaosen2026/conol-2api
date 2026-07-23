// OpenAI ↔ conol.ai 协议转换
package openai

import (
	"encoding/base64"
	"fmt"
	"log"
	"strings"
	"sync"

	"conol-proxy/conol"
)

// ConvertMessages OpenAI 消息 → conol.ai 消息
// ImageUpload 需要上传的图片信息
type ImageUpload struct {
	RawBytes  []byte
	MediaType string
	MsgIndex  int // 属于哪个消息
	PartIndex int // 消息中的哪个 part
}

func ConvertMessages(msgs []ChatMessage, model string) (conolMsgs []conol.Message, systemPrompt, lastUserText string, uploads []ImageUpload) {
	for _, m := range msgs {
		switch m.Role {
		case "system":
			systemPrompt = extractText(m.Content)
		case "user":
			text, parts := extractMultimodal(m.Content)
			lastUserText = text
			if len(parts) > 0 {
				hasImage := false
				for _, p := range parts {
					if p.ImageURL != nil { hasImage = true; break }
				}
				if hasImage {
					supportsImage := strings.HasPrefix(model, "gemini") || strings.Contains(model, "gemini")
					for _, p := range parts {
						if p.ImageURL != nil {
							if supportsImage {
								raw, mime := decodeBase64Image(p.ImageURL.URL)
								if raw != nil {
									conolMsgs = append(conolMsgs, conol.Message{
										Type: "image", MediaType: mime, Content: "",
									})
									uploads = append(uploads, ImageUpload{
										RawBytes: raw, MediaType: mime,
										MsgIndex: len(conolMsgs) - 1,
									})
								} else {
									conolMsgs = append(conolMsgs, conol.Message{
										Type: "image", MediaType: "image/png",
										Content: p.ImageURL.URL,
									})
								}
							} else {
								conolMsgs = append(conolMsgs, conol.Message{
									Content: "[Image attached — this model does not support images. Use gemini-3.5-flash for image analysis.]",
									Type: "text",
								})
							}
						} else if p.Text != "" {
							conolMsgs = append(conolMsgs, conol.Message{Content: p.Text, Type: "text"})
						}
					}
				} else {
					conolMsgs = append(conolMsgs, conol.Message{Content: text, Type: "text"})
				}
			} else {
				conolMsgs = append(conolMsgs, conol.Message{Content: text, Type: "text"})
			}
		case "assistant":
			text := extractText(m.Content)
			if text != "" {
				conolMsgs = append(conolMsgs, conol.Message{Content: text, Type: "text"})
			}
			for _, tc := range m.ToolCalls {
				conolMsgs = append(conolMsgs, conol.Message{
					Content: fmt.Sprintf("[ToolCall: %s(%s)]", tc.Function.Name, tc.Function.Arguments),
					Type:    "text",
				})
			}
		case "tool":
			conolMsgs = append(conolMsgs, conol.Message{
				Content: fmt.Sprintf("[ToolResult id=%s]: %s", m.ToolCallID, extractText(m.Content)),
				Type:    "text",
			})
		}
	}
	return
}

// BuildSystemPrompt 构建系统提示词（含工具定义）
func BuildSystemPrompt(req *ChatRequest) string {
	var sb strings.Builder

	// 用户自己的 system prompt
	for _, m := range req.Messages {
		if m.Role == "system" {
			sb.WriteString(extractText(m.Content))
			sb.WriteString("\n\n")
		}
	}

	// XML 输出格式 — 绕过 conol.ai agent 默认行为
	sb.WriteString("OUTPUT XML FORMAT (MANDATORY):\n")
	if len(req.Tools) > 0 {
		sb.WriteString("Text reply: <response><content>your text here</content></response>\n")
		sb.WriteString("Tool call: <response><tool_call><name>func_name</name><arguments>{\"arg\":\"val\"}</arguments></tool_call></response>\n\n")
		sb.WriteString("Available functions:\n")
		for _, t := range req.Tools {
			if t.Type == "function" {
				sb.WriteString(fmt.Sprintf("- %s: %s\n", t.Function.Name, t.Function.Description))
			}
		}
	} else {
		sb.WriteString("<response><content>your text here</content></response>\n")
	}
	sb.WriteString("\nRULES: Output ONLY the XML. No explanations. No markdown. Just XML.\n")
	return sb.String()
}

// ─── SSE → OpenAI Stream ───

type SSEConverter struct {
	mu              sync.Mutex
	sessionID       string
	created         int64
	model           string
	roleSent        bool
	reasoningSent   bool
	fullText        strings.Builder // 已发送给客户端的总文本
	latestText      string          // SSE 事件中最新的完整文本
	fullReasoning   strings.Builder // 已发送的推理文本
	latestReasoning string          // SSE 事件中最新的完整推理文本
	feedCount       int
	flushCount      int
	pendingReasoning bool // 等待思考分离
}

func NewSSEConverter(sid, model string, created int64) *SSEConverter {
	return &SSEConverter{
		sessionID: sid, created: created, model: model,
	}
}

// Feed 接收累积式的 SSE 事件，记录最新完整文本
func (c *SSEConverter) Feed(evt conol.SSEEvent) []StreamChunk {
	c.mu.Lock()
	defer c.mu.Unlock()

	text, reasoning := extractAllText(evt)
	c.feedCount++

	if text != "" || reasoning != "" {
		if reasoning != "" {
			c.pendingReasoning = false // 思考已到，不用等了
			if c.latestReasoning == "" && c.latestText != "" {
				c.latestText = strings.TrimPrefix(c.latestText, reasoning)
				log.Printf("[SSE feed#%d] trim思考-%d → 纯正文=%d",
					c.feedCount, len(reasoning), len(c.latestText))
			}
			c.latestReasoning = reasoning
		} else if evt.LogFrom > 0 && c.latestReasoning == "" {
			// 思考还没到，设置等待标志，先不刷 content
			c.pendingReasoning = true
		}
		if text != "" {
			c.latestText = text
		}
		log.Printf("[SSE feed#%d] text=%d reasoning=%d logFrom=%d pending=%v type=%s",
			c.feedCount, len(text), len(reasoning), evt.LogFrom, c.pendingReasoning, evt.Type)
	} else {
		if evt.Type == "done" {
			c.pendingReasoning = false
		}
		log.Printf("[SSE feed#%d] empty logFrom=%d type=%s", c.feedCount, evt.LogFrom, evt.Type)
	}

	return nil
}

// Flush 计算增量 delta 并发出（content + reasoning）
func (c *SSEConverter) Flush() []StreamChunk {
	c.mu.Lock()
	defer c.mu.Unlock()

	contentLen := c.fullText.Len()
	reasoningLen := c.fullReasoning.Len()
	hasContent := len(c.latestText) > contentLen && !c.pendingReasoning
	hasReasoning := len(c.latestReasoning) > reasoningLen

	if !hasContent && !hasReasoning {
		return nil
	}

	c.flushCount++
	var chunks []StreamChunk

	// 1. 先发推理内容 (reasoning_content)
	if hasReasoning {
		rdelta := c.latestReasoning[reasoningLen:]
		c.fullReasoning.WriteString(rdelta)
		log.Printf("[SSE flush#%d] reasoning +%d chars, total=%d", c.flushCount, len(rdelta), c.fullReasoning.Len())
		chunks = append(chunks, StreamChunk{
			ID: c.sessionID, Object: "chat.completion.chunk",
			Created: c.created, Model: c.model,
			Choices: []Choice{{Index: 0, Delta: &Delta{ReasoningContent: StrPtr(rdelta)}, FinishReason: nil}},
		})
	}

	// 2. 再发正文内容
	if hasContent {
		delta := c.latestText[contentLen:]
		c.fullText.WriteString(delta)

		// 首个正文 chunk 带 role
		if !c.roleSent {
			log.Printf("[SSE flush#%d] → role chunk", c.flushCount)
			chunks = append(chunks, StreamChunk{
				ID: c.sessionID, Object: "chat.completion.chunk",
				Created: c.created, Model: c.model,
				Choices: []Choice{{Index: 0, Delta: &Delta{Role: "assistant", Content: StrPtr("")}, FinishReason: nil}},
			})
			c.roleSent = true
		}

		log.Printf("[SSE flush#%d] content +%d chars, total=%d", c.flushCount, len(delta), c.fullText.Len())
		chunks = append(chunks, StreamChunk{
			ID: c.sessionID, Object: "chat.completion.chunk",
			Created: c.created, Model: c.model,
			Choices: []Choice{{Index: 0, Delta: &Delta{Content: StrPtr(delta)}, FinishReason: nil}},
		})
	}

	return chunks
}

// extractAllText 从 previews + logs 中提取 assistant 文本 + 推理文本
func extractAllText(evt conol.SSEEvent) (content, reasoning string) {
	var contentParts, reasoningParts []string
	for _, stage := range evt.Stages {
		// 1. 正文内容 (previews, role=assistant)
		for _, pv := range stage.Preview {
			for _, blk := range pv.Content {
				if blk.Text == "" || blk.Type != "text" {
					continue
				}
				if pv.Role == "assistant" {
					contentParts = append(contentParts, blk.Text)
				} else {
					reasoningParts = append(reasoningParts, blk.Text)
				}
			}
		}
		// 2. 思考/推理内容 (logs, type=thinking)
		for _, log := range stage.Logs {
			if log.Type != "thinking" {
				continue
			}
			for _, blk := range log.Content {
				if blk.Text != "" && blk.Type == "text" {
					reasoningParts = append(reasoningParts, blk.Text)
				}
			}
		}
	}
	return strings.Join(contentParts, ""), strings.Join(reasoningParts, "")
}

func (c *SSEConverter) FinishChunk() StreamChunk {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Printf("[SSE finish] total content=%d chars, feeds=%d flushes=%d", c.fullText.Len(), c.feedCount, c.flushCount)
	return StreamChunk{
		ID: c.sessionID, Object: "chat.completion.chunk",
		Created: c.created, Model: c.model,
		Choices: []Choice{{Index: 0, Delta: &Delta{}, FinishReason: StrPtr("stop")}},
	}
}

func (c *SSEConverter) NonStreamResponse() ChatResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	text := c.fullText.String()
	if text == "" {
		text = c.latestText
	}
	content, toolCall := extractXML(text)
	if content == "" {
		content = text
	}
	msg := &RespMessage{Role: "assistant", Content: content}
	if toolCall != nil {
		msg.ToolCalls = []ToolCall{*toolCall}
	}
	log.Printf("[SSE nonstream] raw=%d content=%d tool=%v", len(text), len(content), toolCall != nil)
	return ChatResponse{
		ID: c.sessionID, Object: "chat.completion",
		Created: c.created, Model: c.model,
		Choices: []Choice{{
			Index:   0,
			Message: msg,
			FinishReason: StrPtr("stop"),
		}},
	}
}

// ─── 消息内容提取 ───

func extractText(content interface{}) string {
	switch v := content.(type) {
	case string:
		return v
	case []interface{}:
		var parts []string
		for _, p := range v {
			if m, ok := p.(map[string]interface{}); ok {
				if t, _ := m["type"].(string); t == "text" {
					if txt, _ := m["text"].(string); txt != "" {
						parts = append(parts, txt)
					}
				}
			}
		}
		return strings.Join(parts, "\n")
	}
	return fmt.Sprintf("%v", content)
}

func extractMultimodal(content interface{}) (text string, parts []ContentPart) {
	switch v := content.(type) {
	case string:
		return v, nil
	case []interface{}:
		var texts []string
		for _, p := range v {
			m, ok := p.(map[string]interface{})
			if !ok {
				continue
			}
			switch m["type"] {
			case "text":
				if t, _ := m["text"].(string); t != "" {
					texts = append(texts, t)
					parts = append(parts, ContentPart{Type: "text", Text: t})
				}
			case "image_url":
				if img, _ := m["image_url"].(map[string]interface{}); img != nil {
					url, _ := img["url"].(string)
					detail, _ := img["detail"].(string)
					parts = append(parts, ContentPart{
						Type:     "image_url",
						ImageURL: &ImageURL{URL: url, Detail: detail},
					})
				}
			}
		}
		return strings.Join(texts, "\n"), parts
	}
	return "", nil
}

// decodeBase64Image 解码 data:image/xxx;base64,... 为原始字节
func decodeBase64Image(dataURL string) ([]byte, string) {
	if !strings.HasPrefix(dataURL, "data:") {
		return nil, ""
	}
	// data:image/png;base64,xxxx
	idx := strings.Index(dataURL, ";base64,")
	if idx < 0 {
		return nil, ""
	}
	mime := dataURL[5:idx] // image/png
	b64 := dataURL[idx+8:]
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, ""
	}
	return raw, mime
}

func StrPtr(s string) *string { return &s }

