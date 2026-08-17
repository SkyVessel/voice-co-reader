# 语音协同导读（Voice Co-Reader）

一个实时语音 AI 助手的最小内核：**语音是 AI 的嘴和手指，UI 是共享桌面**。
对着电脑说话做知识问答和研究，AI 口述结论、把笔记变成屏幕上的卡片。

## 架构

```
你 ↔ 前台（qwen-audio realtime，听说+接待）
        ├─ 工具组：read / write / edit / bash / web_search(exa) / web_fetch
        ├─ 秘书（Secretary）：旁听对话原文，点单后让主力模型写笔记卡片
        └─ 主力（deepseek 等文本模型）：深度推理 / 长任务编排（异步委派）
```

- 四层内核：`core/`（事件总线 / provider / 会话 / 音频）+ 工具热加载 + Skills + Hooks
- 双前端：CLI（`src/main/cli.py`）与浏览器 UI（`src/main/bridge.py` + `ui/index.html`，orb-ui 律动球 + 笔记卡片流）
- 主动裁剪：对话历史超阈值自动归档落盘 + 服务端删除，长聊不爆 token
- 断线自动重连（指数退避）、音频设备跟随（插拔耳机自动切换）

## 运行

```bash
python -m venv .venv && .venv/bin/pip install websockets sounddevice ddgs trafilatura prompt_toolkit numpy
cp .env.example .env  # 填入 DASHSCOPE_API_KEY 等
.venv/bin/python -u -m src.main.cli      # CLI 模式
.venv/bin/python -u -m src.main.bridge   # 然后打开 ui/index.html
```

## 许可

保留所有权利（All Rights Reserved）。暂未选择开源许可证。
