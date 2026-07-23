// conol.ai API 客户端（proxy专用轻量版）
package conol

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

const BaseURL = "https://conol.ai"

type Client struct {
	hc       *http.Client
	session  string // __Secure-better-auth.session_token
	passkey  string // __Secure-better-auth.better-auth-passkey
}

func New(sessionToken, passkey string) *Client {
	return &Client{
		hc:      &http.Client{Timeout: 120 * time.Second},
		session: sessionToken,
		passkey: passkey,
	}
}

func (c *Client) SessionToken() string { return c.session }
func (c *Client) Passkey() string      { return c.passkey }

// GetSession 验证 token 有效性并返回用户信息
func (c *Client) GetSession() (*SessionInfo, error) {
	var result struct {
		Session *SessionInfo `json:"session"`
		User    *UserInfo    `json:"user"`
	}
	if err := c.do("GET", "/api/auth/get-session", nil, &result); err != nil {
		return nil, err
	}
	if result.User != nil {
		result.Session.Email = result.User.Email
	}
	return result.Session, nil
}

// GetBillingBalance 查询余额
func (c *Client) GetBillingBalance() (*BillingBalance, error) {
	var result BillingBalance
	if err := c.do("GET", "/api/billing/balance", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

type BillingBalance struct {
	DailyCredits        float64 `json:"dailyCredits"`
	DailyRefilledAt     string  `json:"dailyRefilledAt"`
	ExtraCredits        float64 `json:"extraCredits"`
	SubscriptionAmount  float64 `json:"subscriptionAmount"`
	SubscriptionCredits float64 `json:"subscriptionCredits"`
	Total               float64 `json:"total"`
	UpdatedAt           string  `json:"updatedAt"`
	UserID              string  `json:"userId"`
}

// CreateSession 创建会话 (支持模型选择)
func (c *Client) CreateSession(messages []Message, systemPrompt string, timezone string, opts map[string]interface{}) (*CreateResp, error) {
	req := map[string]interface{}{
		"messages": messages,
		"source":   map[string]string{"type": "home"},
		"timezone": timezone,
	}
	if systemPrompt != "" {
		req["systemPrompt"] = systemPrompt
	}
	// 透传额外选项 (model, runtime, etc.)
	for k, v := range opts {
		req[k] = v
	}
	log.Printf("[CreateSession] model=%v, systemPrompt=%d chars", req["agentModel"], len(systemPrompt))
	var result CreateResp
	if err := c.do("POST", "/api/sessions", req, &result); err != nil {
		return nil, err
	}
	log.Printf("[CreateSession] ✅ sid=%s", result.SessionID)
	return &result, nil
}

// StreamMessages 流式读取会话消息
func (c *Client) StreamMessages(sessionID string, cb func(SSEEvent)) error {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	url := BaseURL + "/api/sessions/" + sessionID + "/messages?logDeltas=1"
	log.Printf("[SSE conn] → %s", url)

	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cookie", c.cookieHeader())
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Referer", BaseURL+"/home?chat_session="+sessionID)

	resp, err := c.hc.Do(req)
	if err != nil {
		log.Printf("[SSE conn] ❌ request error: %v", err)
		return err
	}
	defer resp.Body.Close()
	log.Printf("[SSE conn] HTTP %d", resp.StatusCode)

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	var buf strings.Builder
	eventCount := 0

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			d := strings.TrimPrefix(line, "data: ")
			if d == "[DONE]" {
				log.Printf("[SSE conn] ← [DONE] from server")
				break
			}
			buf.WriteString(d)
			continue
		}
		if line == "" && buf.Len() > 0 {
			var evt SSEEvent
			if json.Unmarshal([]byte(buf.String()), &evt) == nil {
				eventCount++
				// 统计有效内容
				totalText := 0
				for _, st := range evt.Stages {
					for _, pv := range st.Preview {
						for _, blk := range pv.Content {
							if blk.Text != "" {
								totalText += len(blk.Text)
							}
						}
					}
				}
				log.Printf("[SSE evt#%d] type=%s stages=%d text=%d", eventCount, evt.Type, len(evt.Stages), totalText)
				cb(evt)
					if evt.Type == "done" {
						log.Printf("[SSE conn] ← done event, closing stream")
						buf.Reset()
						break
					}
			} else {
				log.Printf("[SSE evt] ❌ parse fail: %s", truncateStr(buf.String(), 500))
			}
			buf.Reset()
		}
	}
	log.Printf("[SSE conn] 结束, events=%d, scannerErr=%v", eventCount, scanner.Err())
	// SSE 自然关闭或超时，正常返回
	return nil
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// cookieHeader 构建 Cookie 请求头
func (c *Client) cookieHeader() string {
	parts := []string{
		"conol_cookie_consent=%7B%22version%22%3A2%2C%22necessary%22%3Atrue%2C%22analytics%22%3Atrue%2C%22updatedAt%22%3A1784794812010%7D",
	}
	if c.passkey != "" {
		parts = append(parts, "__Secure-better-auth.better-auth-passkey="+c.passkey)
	}
	if c.session != "" {
		parts = append(parts, "__Secure-better-auth.session_token="+c.session)
	}
	return strings.Join(parts, "; ")
}

func (c *Client) do(method, path string, body interface{}, result interface{}) error {
	var bodyReader io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		bodyReader = bytes.NewReader(data)
	}
	req, _ := http.NewRequest(method, BaseURL+path, bodyReader)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Cookie", c.cookieHeader())
	req.Header.Set("Origin", BaseURL)
	req.Header.Set("Referer", BaseURL+"/home")

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(b[:min(len(b), 300)]))
	}
	if result != nil {
		return json.NewDecoder(resp.Body).Decode(result)
	}
	return nil
}

const userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"

// ─── 类型 ───

type SessionInfo struct {
	ID        string `json:"id"`
	UserID    string `json:"userId"`
	Token     string `json:"token"`
	Email     string `json:"-"`
	CreatedAt string `json:"createdAt"`
	ExpiresAt string `json:"expiresAt"`
}

type UserInfo struct {
	ID            string `json:"id"`
	Email         string `json:"email"`
	Name          string `json:"name"`
	EmailVerified bool   `json:"emailVerified"`
	Role          string `json:"role"`
	Banned        bool   `json:"banned"`
}

type Message struct {
	Content interface{} `json:"content"` // string 或 []ContentPart
	Type    string      `json:"type"`
}

type ContentPart struct {
	Text     string    `json:"text,omitempty"`
	Type     string    `json:"type,omitempty"`
	ImageURL *ImageURL `json:"image_url,omitempty"`
}

type ImageURL struct {
	URL    string `json:"url"`
	Detail string `json:"detail,omitempty"`
}

type CreateResp struct {
	SessionID string `json:"sessionId"`
}

type SSEEvent struct {
	Type               string  `json:"type"`
	AgentSessionID     string  `json:"agentSessionId,omitempty"`
	From               int     `json:"from,omitempty"`
		RuntimeVersion     string      `json:"runtimeVersion,omitempty"`
		LogFrom            int         `json:"logFrom,omitempty"`
	Stages             []Stage `json:"stages,omitempty"`
	PendingQuestionary interface{} `json:"pendingQuestionary,omitempty"`
}

type Stage struct {
	ID      string    `json:"id"`
	Purpose string    `json:"purpose"`
	Preview []Preview `json:"preview"`
	Logs    []Log     `json:"logs"`
	Tools   []ToolUse `json:"tools,omitempty"`
}

type Preview struct {
	Content   []ContentBlock `json:"content"`
	Role      string         `json:"role"`
	Timestamp string         `json:"timestamp"`
	Type      string         `json:"type"`
}

type ContentBlock struct {
	Text string `json:"text"`
	Type string `json:"type"`
}

type Log struct {
	Type    string         `json:"type"`
	Role    string         `json:"role,omitempty"`
	Content []ContentBlock `json:"content"`
}

type ToolUse struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Input    json.RawMessage `json:"input,omitempty"`
	Output   json.RawMessage `json:"output,omitempty"`
	Status   string `json:"status"`
}
