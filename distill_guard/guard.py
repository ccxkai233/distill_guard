"""过滤 + 小量放行的核心判定。

设计对应的流量观察:
  1. "没缓存 / 非流 / 大输入小输出的 80% 以上都是"
       -> 形态门 _suspicious_shape(): 只有命中这个形态的请求才进入风控,
          流式或带 cache_control 的正常客户端零状态直通。
  2. "相同的请求, 很小的, 一个问题拆成很多次"
       -> 状态按模板指纹聚合(prefix_fp + simhash 桶兜底), 不按账号。
  3. "同时进的很小的请求, 只让一部分进来"
       -> 每个模板每 10s/60s 有一个免费额度, 超出后按比例放行,
          且永远保底 min_pass_rate(小量放行, 不做硬零)。
  4. "中间插正常请求"
       -> 模板可疑度只随时间半衰, 任何其他请求都稀释不了它;
          正常请求指纹不同, 根本碰不到可疑模板的状态。
  5. "很多报错是我主动返回的"
       -> reject 动作由网关直接返回一个仿真上游形态的错误体, 请求不落上游。
          上线前应抓真实上游错误原文回填 upstream_error_body/status 使其尽量贴近,
          注意经 JSONResponse 重新序列化后无法保证与上游逐字节一致(头部/格式会有差异)。
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Any

from .fingerprint import normalize, prefix_fp, sim_bucket, simhash64
from .state import MemoryState


@dataclass
class GuardConfig:
    shadow: bool = True            # 上线第一阶段只记日志不处置
    free_per_min: int = 6          # 同模板每分钟免费放行额度
    free_per_10s: int = 3          # 同模板 10 秒突发免费额度
    min_pass_rate: float = 0.03    # 永远保留的放行比例 —— "小量放行"
    susp_half_life: float = 600.0  # 可疑度半衰期(秒), 只随时间衰减
    susp_alpha: float = 0.04       # 每次额度溢出抬升可疑度的幅度
    delay_ms: int = 400            # 高可疑但放行的请求加抖动延迟

    # 形态门阈值
    small_out_tokens: int = 256
    big_in_tokens: int = 2000
    tiny_in_tokens: int = 400
    io_ratio: float = 8.0

    # 伪装错误应尽量贴近真实上游错误, 上线前用实际抓到的原文替换 body/status
    upstream_error_status: int = 529
    upstream_error_body: dict = field(
        default_factory=lambda: {
            "type": "error",
            "error": {"type": "overloaded_error", "message": "Overloaded"},
        }
    )


@dataclass
class ReqFeat:
    ts: float
    stream: bool
    has_cache_ctrl: bool
    in_tokens: int
    max_tokens: int | None  # 客户端未传时为 None, 不做兜底猜测
    n_messages: int
    prefix: str
    sh: int


@dataclass
class Decision:
    action: str          # "pass" | "delay" | "reject"
    suspicious: bool     # 是否命中形态门
    fp: str
    n10: int
    n60: int
    susp: float
    pass_rate: float     # 本次判定使用的放行比例(额度内为 1.0)


def _flat_text(body: dict[str, Any]) -> str:
    parts: list[str] = []
    sys_prompt = body.get("system") or ""
    if isinstance(sys_prompt, list):  # Anthropic block 形式
        sys_prompt = " ".join(
            b.get("text", "") for b in sys_prompt if isinstance(b, dict)
        )
    if sys_prompt:
        parts.append(str(sys_prompt))
    for m in body.get("messages") or []:
        c = m.get("content") if isinstance(m, dict) else None
        if isinstance(c, str):
            parts.append(c)
        elif isinstance(c, list):
            parts.extend(b.get("text", "") for b in c if isinstance(b, dict))
    return " ".join(p for p in parts if p)


def estimate_tokens(text: str) -> int:
    """无 tokenizer 的粗估: ASCII 约 4 字符/token, CJK 等按 1 字符/token。"""
    ascii_n = sum(1 for ch in text if ord(ch) < 128)
    return max(1, ascii_n // 4 + (len(text) - ascii_n))


def _has_cache_ctrl(obj: Any, depth: int = 0) -> bool:
    """结构化探测 cache_control 键。不能对原始 JSON 做子串匹配 ——
    否则在 prompt 正文里写一句 "cache_control" 就能骗过形态门。"""
    if depth > 6:
        return False
    if isinstance(obj, dict):
        if "cache_control" in obj:
            return True
        return any(_has_cache_ctrl(v, depth + 1) for v in obj.values())
    if isinstance(obj, list):
        return any(_has_cache_ctrl(v, depth + 1) for v in obj)
    return False


def extract(body: dict[str, Any], in_tokens: int | None = None, now: float | None = None) -> ReqFeat:
    raw = _flat_text(body)
    flat = normalize(raw)
    mt = body.get("max_tokens")
    max_tokens = int(mt) if isinstance(mt, (int, float)) and mt > 0 else None
    return ReqFeat(
        ts=now if now is not None else time.time(),
        stream=bool(body.get("stream")),
        has_cache_ctrl=_has_cache_ctrl(body),
        in_tokens=in_tokens if in_tokens is not None else estimate_tokens(raw),
        max_tokens=max_tokens,
        n_messages=len(body.get("messages") or []),
        prefix=prefix_fp(flat),
        sh=simhash64(flat),
    )


class DistillGuard:
    def __init__(self, state=None, cfg: GuardConfig | None = None, rng: random.Random | None = None):
        self.state = state or MemoryState()
        self.cfg = cfg or GuardConfig()
        self.rng = rng or random.Random()

    def _suspicious_shape(self, f: ReqFeat) -> bool:
        if f.stream or f.has_cache_ctrl:
            return False  # 正常客户端形态, 直通
        c = self.cfg
        small_out = f.max_tokens is not None and f.max_tokens <= c.small_out_tokens
        harvest = f.in_tokens >= c.big_in_tokens and small_out       # 大输入小输出
        skew = f.max_tokens is not None and f.in_tokens >= c.io_ratio * f.max_tokens
        shard = f.in_tokens <= c.tiny_in_tokens and 1 <= f.n_messages <= 2  # 拆碎的小请求(至少 1 条消息, 排除空 body)
        return harvest or skew or shard

    async def _load_susp(self, fp: str, now: float) -> float:
        val, ts = await self.state.get_susp(fp)
        if val <= 0.0:
            return 0.0
        val *= 0.5 ** (max(now - ts, 0.0) / self.cfg.susp_half_life)
        return min(val, 1.0)

    async def _raise_susp(self, fp: str, now: float, susp: float) -> float:
        new = susp + self.cfg.susp_alpha * (1.0 - susp)
        await self.state.set_susp(fp, new, now)
        return new

    async def check(
        self,
        body: dict[str, Any] | None = None,
        in_tokens: int | None = None,
        *,
        feat: ReqFeat | None = None,
        now: float | None = None,
    ) -> Decision:
        now = now if now is not None else time.time()
        f = feat or extract(body or {}, in_tokens, now)
        fp = f.prefix

        if not self._suspicious_shape(f):
            return Decision("pass", False, fp, 0, 0, 0.0, 1.0)

        n10 = await self.state.bump(f"fp10:{fp}", now, 10)
        n60 = await self.state.bump(f"fp60:{fp}", now, 60)
        nb60 = await self.state.bump(f"sb60:{sim_bucket(f.sh):05x}", now, 60)
        n60e = max(n60, nb60 // 2)  # simhash 桶按半权兜底轻度改写的模板

        susp = await self._load_susp(fp, now)
        free60 = max(1, round(self.cfg.free_per_min * (1.0 - 0.8 * susp)))
        free10 = max(1, round(self.cfg.free_per_10s * (1.0 - 0.8 * susp)))

        if n60e <= free60 and n10 <= free10:
            action = "delay" if susp >= 0.5 else "pass"
            return Decision(action, True, fp, n10, n60e, susp, 1.0)

        # 超出免费额度: 抬升该模板可疑度, 按比例小量放行
        susp = await self._raise_susp(fp, now, susp)
        # 上限钳到 1.0: 当仅 10s 突发超额(n60e<=free60)时 free60/n60e 会 >1,
        # 作为"放行比例"必须 <=1。下限保留 min_pass_rate(小量放行)。
        rate = min(1.0, max(self.cfg.min_pass_rate, free60 / n60e * (1.0 - 0.7 * susp)))
        if self.rng.random() < rate:
            return Decision("delay", True, fp, n10, n60e, susp, rate)
        return Decision("reject", True, fp, n10, n60e, susp, rate)
