"""计数与可疑度存储。

MemoryState: 单机/测试用。
RedisState:  多网关实例共享, 任何 Redis 异常一律 fail-open(返回零计数),
             风控故障不能放大成平台故障。

计数窗口用"当前桶 + 上一桶"两个固定桶近似滑动窗口:
返回值覆盖最近 win ~ 2*win 秒, 宁可略微高估也不在桶边界漏计。
"""

from __future__ import annotations

import logging
import time

log = logging.getLogger("distill_guard")


class MemoryState:
    def __init__(self) -> None:
        self._counts: dict[tuple[str, int, int], int] = {}
        self._susp: dict[str, tuple[float, float]] = {}

    async def bump(self, key: str, now: float, win: int) -> int:
        b = int(now // win)
        cur = self._counts.get((key, win, b), 0) + 1
        self._counts[(key, win, b)] = cur
        prev = self._counts.get((key, win, b - 1), 0)
        if len(self._counts) > 200_000:
            self._prune(now)
        return cur + prev

    async def get_susp(self, key: str) -> tuple[float, float]:
        return self._susp.get(key, (0.0, 0.0))

    async def set_susp(self, key: str, val: float, ts: float) -> None:
        self._susp[key] = (val, ts)

    def _prune(self, now: float) -> None:
        self._counts = {
            (k, w, b): c for (k, w, b), c in self._counts.items() if b >= int(now // w) - 1
        }


class RedisState:
    def __init__(self, redis) -> None:
        self.r = redis

    async def bump(self, key: str, now: float, win: int) -> int:
        b = int(now // win)
        k_cur = f"dg:c:{key}:{b}"
        k_prev = f"dg:c:{key}:{b - 1}"
        try:
            pipe = self.r.pipeline()
            pipe.incr(k_cur)
            pipe.expire(k_cur, win * 2 + 5)
            pipe.get(k_prev)
            cur, _, prev = await pipe.execute()
            return int(cur) + int(prev or 0)
        except Exception:
            log.warning("redis bump failed, fail-open", exc_info=True)
            return 0

    async def get_susp(self, key: str) -> tuple[float, float]:
        try:
            row = await self.r.hmget(f"dg:s:{key}", "v", "ts")
            if not row or row[0] is None:
                return 0.0, 0.0
            return float(row[0]), float(row[1] or 0.0)
        except Exception:
            log.warning("redis get_susp failed, fail-open", exc_info=True)
            return 0.0, 0.0

    async def set_susp(self, key: str, val: float, ts: float) -> None:
        try:
            pipe = self.r.pipeline()
            pipe.hset(f"dg:s:{key}", mapping={"v": val, "ts": ts})
            pipe.expire(f"dg:s:{key}", 86_400)
            await pipe.execute()
        except Exception:
            log.warning("redis set_susp failed, fail-open", exc_info=True)


def wall_clock() -> float:
    return time.time()
