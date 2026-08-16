"""Skills 加载器：SKILL.md 目录式技能（设计照抄 pi 的 skills.ts）。

目录结构：src/skills/<name>/SKILL.md，带 YAML frontmatter：
    ---
    name: web-research
    description: 联网调研并口述汇报
    ---
    （正文：方法论指引，注入 realtime 会话的 instructions）
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger("skills")


@dataclass
class Skill:
    name: str
    description: str
    content: str
    path: Path


def _parse_frontmatter(text: str) -> tuple[dict[str, str], str]:
    """极简 frontmatter 解析（只支持 key: value 平铺，不为它引入 yaml 依赖）。"""
    if not text.startswith("---"):
        return {}, text
    end = text.find("\n---", 3)
    if end == -1:
        return {}, text
    meta: dict[str, str] = {}
    for line in text[3:end].strip().splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            meta[k.strip()] = v.strip()
    return meta, text[end + 4:].strip()


def load_skills(skills_dir: str | Path) -> list[Skill]:
    skills: list[Skill] = []
    d = Path(skills_dir)
    if not d.is_dir():
        return skills
    for path in sorted(d.glob("*/SKILL.md")):
        try:
            meta, content = _parse_frontmatter(path.read_text(encoding="utf-8"))
            name = meta.get("name") or path.parent.name
            skills.append(Skill(
                name=name,
                description=meta.get("description", ""),
                content=content,
                path=path,
            ))
        except Exception:
            log.exception("加载技能失败: %s", path)
    return skills


def format_for_instructions(skills: list[Skill]) -> str:
    """对照 pi 的 formatSkillsForSystemPrompt：技能清单 + 正文注入 instructions。"""
    if not skills:
        return ""
    lines = ["\n\n# 可用技能\n"]
    for s in skills:
        lines.append(f"## {s.name}\n{s.description}\n\n{s.content}\n")
    return "\n".join(lines)
