# 语音 Agent 外壳（Voice Agent Shell）

## 项目目标

构建一个**针对实时语音模型优化的 Agent 外壳（runtime/shell）**，核心使用场景：

1. **日常知识问答** —— 低延迟、可随时打断的语音对话
2. **资料调查（research）** —— 语音驱动搜索、浏览、整理并口头/文字汇报结果

**产品交互范式：协同导读（Guided Co-reading）** —— 语音是 AI 的嘴和手指，UI 是共享桌面；AI 边说边指边生成带索引的实时笔记，用户随时打断/提问/回溯/修改。**所有设计决策前先读 `docs/产品形态-协同导读.md`**，它是本项目的最高交互准则。

**总原则：复杂度尽可能低，但不牺牲功能。** 特化做在外壳，不做在模型。

外壳不绑定单一模型，但**首选模型为阿里千问 Qwen Audio 3 实时系列**：

- 模型：`qwen-audio-3.0-realtime-plus`（阿里云百炼 Model Studio 提供）
- 接入方式：WebSocket 全双工实时 API（事件驱动 JSON 协议），客户端持续推送 PCM 音频流，服务端流式返回语音 + 文本
- 关键能力：端到端音频进/音频出、VAD 语音活动检测、打断（barge-in）、**Function Calling 支持**（工具调用的基础）、可选 AOQ/WebRTC 传输以获得弱网下的低延迟
- 文档入口：阿里云百炼 Model Studio → "Qwen-Audio Realtime" / "实时语音对话"

## 设计理念：类 pi 的可拓展性

参考 pi coding agent 的开放架构，外壳遵循"**内核极简 + 一切皆可插拔**"原则：

- **极简内核**：只负责音频管道、实时会话协议、事件循环、工具调度。不内置具体业务能力。
- **Skills（技能包）**：目录式技能（`SKILL.md` + 附属资源/脚本），按场景动态加载，例如 `web-research`、`news-briefing`、`smart-home`。
- **Custom Tools（自定义工具）**：通过统一工具描述（name/description/JSON Schema）注册为 Function Calling 工具，供语音模型调用。
- **Extensions / 事件钩子**：在会话生命周期（on_turn_start、on_tool_call、on_response、on_interrupt 等）注入自定义逻辑。
- **可替换的模型层**：Realtime Provider 抽象接口，Qwen Audio 3 是第一个实现；接口与 OpenAI Realtime 风格对齐，便于日后切换。
- **供应商中立（2026-08 定案）**：每个能力角色都是可替换的 Provider/工具，选型只定默认档，按数据不按品牌。当前默认——语音：`qwen-audio-3.0-realtime-plus`（备选 Grok Voice / Gemini Live 作海外线路）；视觉：`qwen3.7-flash`（备选 Gemini 3.1 Flash，一行配置切换）；搜索：第三方 API（Tavily / Exa / Brave 方向，M4 前正式选型）。详细依据见 `docs/调研-语音模型选型.md`。

## 目录结构（约定）

```
语音agent/
├── AGENTS.md                  # 本文件
├── src/
│   ├── core/                  # 内核：音频IO、VAD、实时协议、事件循环、工具调度
│   │   ├── audio/             # 采集、播放、回声消除、重采样（16/24kHz PCM）
│   │   ├── realtime/          # Provider 抽象 + Qwen Audio Realtime WebSocket 实现
│   │   ├── tools/             # 工具注册表、Function Calling 调度与结果回传
│   │   └── session/           # 会话状态机：idle → listening → thinking → speaking（可打断）
│   ├── skills/                # 技能包目录，每个子目录一个 SKILL.md
│   ├── extensions/            # 事件钩子扩展
│   └── main/                  # 入口：CLI / 桌面应用
├── config/                    # 用户配置：模型、音色、VAD 参数、API Key 引用
└── docs/                      # 协议笔记、调研记录
```

## 实时语音的关键工程约束

所有代码设计必须优先满足以下约束（这是与文本 Agent 的本质区别）：

1. **延迟预算**：用户说完 → 首个音频帧输出，目标 < 1s。任何工具调用不得阻塞音频管道，长任务走异步 + 口头进度反馈（"我去查一下"）。
2. **打断优先（barge-in）**：用户开口时立即停止播放当前回复、清空播放缓冲、向服务端发送打断事件，会话状态机回退到 listening。
3. **流式一切**：音频输入/输出、工具结果、事件全部流式处理；禁止"攒齐再处理"的批处理思维。
4. **工具调用的语音友好性**：工具返回需可被模型自然口述；避免返回超长 JSON，先摘要后细节。
5. **会话上下文管理**：实时会话上下文窗口有限，调研类任务的结果要落到磁盘（`docs/` 或会话工作区），语音里只保留指针。

## 编码约定

- **语言**：Python 3.11+（音频生态与异步模型最契合），异步使用 `asyncio` + `websockets`；音频采集/播放用 `sounddevice` 或 `pyaudio`，视平台而定。
- **配置**：API Key 等密钥一律走环境变量（`DASHSCOPE_API_KEY`），禁止入库。`config/` 里只放非敏感配置。
- **依赖**：`requirements.txt`（或 `pyproject.toml`）锁定版本。
- **日志**：实时事件（收发帧、工具调用、打断）必须有结构化日志，默认 DEBUG 级落盘，INFO 级上屏。
- **错误处理**：网络断连自动重连并恢复会话（session resumption）；音频设备故障降级为文本模式而非崩溃。
- **测试**：核心协议编解码与会话状态机需有单测；音频管道允许以录制样本做回放测试。

## 给编码 Agent 的工作指引

- 修改实时协议相关代码前，先读 `docs/` 中的 Qwen Audio Realtime 协议笔记；不确定的事件/字段名要查官方文档（阿里云百炼 → Qwen-Audio Realtime API 参考），不要凭记忆编造。
- 新增工具 = 在 `src/tools/` 建 `.py` 文件（模块提供 `register(registry, ctx)`，用 `@registry.register` 装饰器，描述文案要语音友好）；新增技能 = 在 `src/skills/<name>/` 建目录写 `SKILL.md`（带 frontmatter）。两者改动都会**热重载**（reload.py 监视，1s 轮询 + session.update 刷新），不用重启会话。
- 保持内核无业务逻辑：知识问答、调研流程属于技能/扩展，不进入 `core/`。
- 每次改动后运行 `python -m pytest`（若已建立测试）并做一次冒烟对话验证打断与工具调用链路。
