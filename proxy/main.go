// Conol.ai → OpenAI 兼容反向代理
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"conol-proxy/conol"
	"conol-proxy/openai"
)

type TokenPool struct {
	mu     sync.RWMutex
	clients []*conol.Client
	idx    atomic.Int64
}

func (p *TokenPool) Add(c *conol.Client) {
	p.mu.Lock()
	p.clients = append(p.clients, c)
	p.mu.Unlock()
}

func (p *TokenPool) Next() *conol.Client {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.clients) == 0 {
		return nil
	}
	i := int(p.idx.Add(1)-1) % len(p.clients)
	return p.clients[i]
}

func (p *TokenPool) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.clients)
}

func (p *TokenPool) All() []*conol.Client {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*conol.Client, len(p.clients))
	copy(out, p.clients)
	return out
}

func (p *TokenPool) Remove(c *conol.Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, cl := range p.clients {
		if cl == c {
			p.clients = append(p.clients[:i], p.clients[i+1:]...)
			return
		}
	}
}

// ─── 服务器 ───

type Server struct {
	pool *TokenPool
}

// creditErrorRE 匹配余额不足错误
var creditErrorRE = `"credit balance is too low"`

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeJSON(w, 405, map[string]string{"error": "method not allowed"})
		return
	}

	var req openai.ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	client := s.pool.Next()
	if client == nil {
		writeJSON(w, 503, map[string]string{"error": "no tokens available"})
		return
	}

	// 转换消息
	conolMsgs, systemPrompt, _, uploads := openai.ConvertMessages(req.Messages)
	if len(conolMsgs) == 0 {
		writeJSON(w, 400, map[string]string{"error": "no valid messages"})
		return
		// 上传图片到 /api/assets
		for _, up := range uploads {
			asset, err := client.UploadAsset(up.RawBytes, up.MediaType)
			if err != nil {
				log.Printf("❌ 图片上传失败: %v", err)
				writeJSON(w, 500, map[string]string{"error": "image upload failed: " + err.Error()})
				return
			}
			conolMsgs[up.MsgIndex].Content = asset.URL
			log.Printf("🖼 图片已上传: %s", asset.URL)
		}

	}

	// 构建系统提示词（含工具定义）
	if sp := openai.BuildSystemPrompt(&req); sp != "" {
		if systemPrompt != "" {
			systemPrompt = systemPrompt + "\n\n" + sp
		} else {
			systemPrompt = sp
		}
	}

	// 模型映射
	timezone := "Asia/Shanghai"
	agentModel := resolveAgentModel(req.Model)
		modelOpts := buildModelOpts(req.Model)
		log.Printf("🔍 模型解析: 用户=%s → agentModel=%s", req.Model, agentModel)

	// 创建 conol.ai 会话
	resp, err := client.CreateSession(conolMsgs, systemPrompt, timezone, modelOpts)
	if err != nil {
		log.Printf("❌ 创建会话失败: %v", err)
		s.pool.Remove(client)
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}

	created := time.Now().Unix()
	model := req.Model
	if model == "" {
		model = "deepseek-v4"
	}

	if req.Stream {
		s.handleStream(w, client, resp.SessionID, model, created)
	} else {
		s.handleNonStream(w, client, resp.SessionID, model, created)
	}
}

func (s *Server) handleStream(w http.ResponseWriter, client *conol.Client, sid, model string, created int64) {
	log.Printf("🌊 流式开始 sid=%s model=%s", sid, model)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, _ := w.(http.Flusher)

	conv := openai.NewSSEConverter(sid, model, created)

	// 定时刷新缓冲 (100ms)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	done := make(chan struct{})
	tickCount := 0

	go func() {
		client.StreamMessages(sid, func(evt conol.SSEEvent) {
			conv.Feed(evt)
		})
		log.Printf("🌊 SSE goroutine 结束 sid=%s", sid)
		close(done)
	}()

	// 主循环：定时 flush 或等待结束
loop:
	for {
		select {
		case <-ticker.C:
			tickCount++
			flushed := 0
			for _, chunk := range conv.Flush() {
				sendSSE(w, flusher, chunk)
				flushed++
			}
			if flushed > 0 {
				log.Printf("🌊 tick#%d: flushed %d chunks", tickCount, flushed)
			}
		case <-done:
			log.Printf("🌊 流结束 sid=%s, ticks=%d", sid, tickCount)
			// 最后一次清空缓冲
			for _, chunk := range conv.Flush() {
				sendSSE(w, flusher, chunk)
			}
			break loop
		}
	}

	sendSSE(w, flusher, conv.FinishChunk())
	fmt.Fprintf(w, "data: [DONE]\n\n")
	if flusher != nil {
		flusher.Flush()
	}
	log.Printf("🌊 流式完成 sid=%s", sid)
}

func (s *Server) handleNonStream(w http.ResponseWriter, client *conol.Client, sid, model string, created int64) {
	conv := openai.NewSSEConverter(sid, model, created)
	var creditErr string
	err := client.StreamMessages(sid, func(evt conol.SSEEvent) {
		conv.Feed(evt)
		// 检测额度错误
		for _, st := range evt.Stages {
			for _, pv := range st.Preview {
				if pv.Type == "error" {
					for _, blk := range pv.Content {
						if strings.Contains(blk.Text, "credit balance is too low") {
							creditErr = blk.Text
						}
					}
				}
			}
		}
	})
	resp := conv.NonStreamResponse()
	if err != nil {
		log.Printf("⚠️ SSE: %v", err)
	}
	if creditErr != "" {
		resp.Choices[0].Message.Content = fmt.Sprintf("[CREDIT LOW] %s", creditErr[:min(len(creditErr),200)])
		resp.Choices[0].FinishReason = openai.StrPtr("error")
	}
	if resp.Choices[0].Message.Content == "" {
		resp.Choices[0].Message.Content = "(empty response)"
	}
	writeJSON(w, 200, resp)
}

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	models := []map[string]interface{}{}
	for k, v := range modelMap {
		models = append(models, map[string]interface{}{
			"id": k, "object": "model", "owned_by": strings.Split(v, "/")[0],
		})
	}
	// 也支持 agentModel 直传
	writeJSON(w, 200, map[string]interface{}{
		"object": "list",
		"data":   models,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	c := s.pool.Next()
	info := map[string]interface{}{"status": "ok", "tokens_active": s.pool.Size()}
	if c != nil {
		if bal, err := c.GetBillingBalance(); err == nil {
			info["balance_total"] = bal.Total
			info["balance_daily"] = bal.DailyCredits
			info["balance_extra"] = bal.ExtraCredits
		}
	}
	writeJSON(w, 200, info)
}

func (s *Server) handleBilling(w http.ResponseWriter, r *http.Request) {
	c := s.pool.Next()
	if c == nil {
		writeJSON(w, 503, map[string]string{"error": "no tokens"})
		return
	}
	bal, err := c.GetBillingBalance()
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, bal)
}

// ─── 辅助 ───

// modelMap OpenAI 模型名 → conol.ai agentModel (从网页抓包验证)
	var modelPresetMap = map[string]string{
		"gpt-5.6-sol":   "pro",
		"gpt-5.6-terra": "pro",
		"gpt-5.6-luna":  "pro",
		"gpt-5.5-pro":   "pro",
	}

var modelMap = map[string]string{
	"deepseek-v4-pro":      "deepseek/deepseek-v4-pro",
	"deepseek-v4-flash":    "deepseek/deepseek-v4-flash",
	"claude-sonnet-4-6":    "claude-sonnet-4-6",
	"kimi-k3":              "moonshotai/kimi-k3",
	"gemini-3.5-flash":     "google/gemini-3.5-flash",
	"gemini-3.1-flash-lite":"google/gemini-3.1-flash-lite",
	"qwen-3.7-plus":        "qwen/qwen3.7-plus",
	"glm-5.2":              "z-ai/glm-5.2",
}

func resolveAgentModel(model string) string {
	if m, ok := modelMap[model]; ok {
		return m
	}
	// 包含 / 的直接透传 (如 openai/gpt-4o, anthropic/claude-xxx)
	if strings.Contains(model, "/") {
		return model
	}
	// 纯名称也透传 (如 claude-sonnet-4-6, gpt-5.5-pro)
	return model
}

func buildModelOpts(model string) map[string]interface{} {
	opts := map[string]interface{}{}
	// 优先 modelPreset（新版 API）
	if preset, ok := modelPresetMap[model]; ok {
		opts["modelPreset"] = preset
	} else {
		// 旧版 agentModel
		opts["agentModel"] = resolveAgentModel(model)
	}
	return opts
}

func sendSSE(w http.ResponseWriter, f http.Flusher, chunk interface{}) {
	b, _ := json.Marshal(chunk)
	fmt.Fprintf(w, "data: %s\n\n", string(b))
	if f != nil {
		f.Flush()
	}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func loadTokens(path string) []*conol.Client {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var clients []*conol.Client
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 3)
		if len(parts) < 1 || parts[0] == "" {
			continue
		}
		tk, pk := parts[0], ""
		if len(parts) >= 2 {
			pk = parts[1]
		}
		c := conol.New(tk, pk)
		if s, err := c.GetSession(); err == nil && s != nil {
			clients = append(clients, c)
			log.Printf("✅ %s", s.Email)
		} else {
			log.Printf("⚠️ 失效: %v", err)
		}
	}
	return clients
}

// ─── main ───

func main() {
	port := flag.Int("port", 8080, "监听端口")
	tokenFile := flag.String("tokens", "../tokens.txt", "Token 文件路径 (token|passkey|email)")
	flag.Parse()

	pool := &TokenPool{}
	paths := []string{*tokenFile, "../tokens.txt", "tokens.txt", "../token.txt"}
	for _, p := range paths {
		for _, c := range loadTokens(p) {
			pool.Add(c)
		}
		if pool.Size() > 0 {
			break
		}
	}

	if pool.Size() == 0 {
		log.Println("⚠️ 没有可用 token！请先运行 register/bridge.exe")
		log.Println("   tokens.txt 格式: token|passkey|email (一行一个)")
	}

	srv := &Server{pool: pool}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", srv.handleChat)
	mux.HandleFunc("/v1/models", srv.handleModels)
	mux.HandleFunc("/health", srv.handleHealth)
	mux.HandleFunc("/v1/billing", srv.handleBilling)

	// 定期健康检查 (每5分钟)
	go func() {
		for range time.Tick(5 * time.Minute) {
			for _, c := range pool.All() {
				if _, err := c.GetSession(); err != nil {
					log.Printf("🗑️ 移除失效token")
					pool.Remove(c)
				}
			}
		}
	}()

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("🚀 Conol.ai → OpenAI 反代 http://localhost%s", addr)
	log.Printf("📋 Token 池: %d 个", pool.Size())
	log.Printf("📍 端点: /v1/chat/completions  /v1/models  /health")
	log.Fatal(http.ListenAndServe(addr, mux))
}
