// XML 解析器 — 从模型回复中提取 content 和 tool_call
package openai

import (
	"strings"
)

// extractXML 从 XML 格式响应中提取纯文本和工具调用
func extractXML(text string) (content string, toolCall *ToolCall) {
	// <response><content>...</content></response>
	if i := strings.Index(text, "<content>"); i >= 0 {
		if j := strings.Index(text, "</content>"); j > i {
			content = text[i+9 : j]
		}
	}
	// <response><tool_call><name>X</name><arguments>J</arguments></tool_call></response>
	if i := strings.Index(text, "<tool_call>"); i >= 0 {
		if j := strings.Index(text, "</tool_call>"); j > i {
			inner := text[i+11 : j]
			ns := strings.Index(inner, "<name>")
			ne := strings.Index(inner, "</name>")
			as := strings.Index(inner, "<arguments>")
			ae := strings.Index(inner, "</arguments>")
			if ns >= 0 && ne > ns && as >= 0 && ae > as {
				toolCall = &ToolCall{
					ID:   "call_xml",
					Type: "function",
					Function: FunctionCall{
						Name:      strings.TrimSpace(inner[ns+6 : ne]),
						Arguments: strings.TrimSpace(inner[as+11 : ae]),
					},
				}
			}
		}
	}
	return
}
