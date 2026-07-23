# Conol Proxy

OpenAI 兼容的反向代理，将 [conol.ai](https://conol.ai) 的 API 转换为标准 OpenAI Chat Completions 格式。

## 特性

- ✅ 完整 OpenAI `/v1/chat/completions` 兼容（流式 + 非流式）
- ✅ `/v1/models` 模型列表
- ✅ `/v1/billing` 余额查询
- ✅ `/health` 健康检查
- ✅ 多 token 轮询池
- ✅ SSE 流式缓冲输出（非透传）
- ✅ 思考/推理内容 → `reasoning_content`
- ✅ 支持模型：DeepSeek V4、GPT-5.6、Claude Sonnet、Gemini、Qwen、GLM、Kimi

## 快速开始

### 1. 准备 Token

从 conol.ai 登录后，在浏览器 DevTools → Application → Cookies 中复制 `__Secure-better-auth.session_token` 和 `__Secure-better-auth.better-auth-passkey`，写入 `tokens.txt`：

```
session_token|passkey|email@example.com
```

### 2. 启动代理

```bash
cd proxy
go run main.go -port 8080 -tokens ../tokens.txt
```

### 3. 调用

```bash
curl http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer conol-sk-any" \
  -d '{
    "model": "deepseek-v4-flash",
    "messages": [{"role": "user", "content": "你好"}],
    "stream": true
  }'
```

## 项目结构

```
├── main.go          # HTTP 服务器入口 + 模型映射
├── conol/           # conol.ai API 客户端
│   └── client.go    # SSE 解析 / CreateSession / 余额
├── openai/          # 协议转换
│   ├── convert.go   # SSE → OpenAI Stream (Feed/Flush)
│   └── types.go     # 类型定义
└── go.mod
```

## 支持的模型

| 模型名 | agentModel |
|--------|-----------|
| deepseek-v4-pro | deepseek/deepseek-v4-pro |
| deepseek-v4-flash | deepseek/deepseek-v4-flash |
| gpt-5.6-sol | modelPreset: pro |
| gpt-5.6-terra | modelPreset: pro |
| gpt-5.6-luna | modelPreset: pro |
| gpt-5.5-pro | modelPreset: pro |
| claude-sonnet-4-6 | claude-sonnet-4-6 |
| kimi-k3 | moonshotai/kimi-k3 |
| gemini-3.5-flash | google/gemini-3.5-flash |
| gemini-3.1-flash-lite | google/gemini-3.1-flash-lite |
| qwen-3.7-plus | qwen/qwen3.7-plus |
| glm-5.2 | z-ai/glm-5.2 |

## SSE 流式格式

严格遵循 OpenAI 规范：

```
data: {"delta":{"role":"assistant","content":""},"finish_reason":null}
data: {"delta":{"reasoning_content":"思考内容..."},"finish_reason":null}
data: {"delta":{"content":"回复内容"},"finish_reason":null}
data: {"delta":{},"finish_reason":"stop"}
data: [DONE]
```

- 首 chunk 带 `role: "assistant"` + 空 `content`
- 中间 chunk 带 `finish_reason: null`
- 思考模型自动输出 `reasoning_content`
- 100ms 缓冲窗口，非原始透传

## Token 格式

`tokens.txt` 每行一个账号：

```
session_token|passkey|email
```

可通过 `/health` 端点查看当前 token 池状态和余额。

## License

MIT
