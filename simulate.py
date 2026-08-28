"""离线验证: 四类流量过一遍 guard, 检查"过滤 + 小量放行"行为。

  A. 采集脚本: 同模板换数字的小请求, 5 rps 打 2 分钟
  B. 正常聊天: 流式、内容各异
  C. 正常 RAG: 非流式但输出预算正常、模板各异
  D. 采集脚本中间插正常请求, 验证可疑度不被稀释
  E. 采集脚本轻度改写模板(加随机词), 验证 simhash 桶兜底

python simulate.py  (无需 Redis / 外部依赖)
"""

import asyncio
import random
from collections import Counter

from distill_guard import DistillGuard, GuardConfig, MemoryState

RNG = random.Random(7)
T0 = 1_000_000.0


def scraper_body(mutate: bool = False) -> dict:
    q = f"{RNG.randint(10000, 99999)} + {RNG.randint(10000, 99999)}"
    extra = f" {RNG.choice(['please', 'now', 'thanks', 'quickly'])}" if mutate else ""
    return {
        "model": "x",
        "stream": False,
        "max_tokens": 32,
        "messages": [{
            "role": "user",
            "content": f"Calculate and respond with ONLY the number.{extra} Q: {q}",
        }],
    }


def chat_body(i: int) -> dict:
    return {
        "model": "x",
        "stream": True,
        "max_tokens": 2000,
        "messages": [
            {"role": "user", "content": f"帮我看看这段报错是什么意思, 场景{i}: " + "细节 " * RNG.randint(5, 60)},
            {"role": "assistant", "content": "好的, 我看一下。"},
            {"role": "user", "content": f"补充信息 {i}: " + "上下文 " * RNG.randint(5, 40)},
        ],
    }


def rag_body(i: int) -> dict:
    return {
        "model": "x",
        "stream": False,
        "max_tokens": 800,
        "messages": [{
            "role": "user",
            "content": f"根据以下文档回答问题{i}。" + f"文档段落{i} " * 300 + f"问题: 场景{i}的结论是什么?",
        }],
    }


async def run(name: str, guard: DistillGuard, bodies_with_ts) -> Counter:
    c: Counter = Counter()
    last = None
    for ts, body in bodies_with_ts:
        d = await guard.check(body, now=ts)
        c[d.action] += 1
        last = d
    total = sum(c.values())
    admitted = c["pass"] + c["delay"]
    print(f"{name:<38} 共{total:>4}  放行{admitted:>4} ({admitted/total:5.1%})  "
          f"拒绝{c['reject']:>4}  末态可疑度={last.susp:.2f}")
    return c


async def main() -> None:
    print("=" * 96)

    # A. 采集脚本: 5 rps * 120s = 600 条同模板小请求
    guard = DistillGuard(MemoryState(), GuardConfig(), rng=random.Random(1))
    await run("A 采集脚本(同模板小请求, 5rps*2min)", guard,
              [(T0 + i / 5, scraper_body()) for i in range(600)])

    # B. 正常聊天(流式多轮), 走同一个 guard 实例
    await run("B 正常聊天(流式, 内容各异)", guard,
              [(T0 + i * 2.0, chat_body(i)) for i in range(60)])

    # C. 正常 RAG(非流式但输出预算正常, 模板各异)
    await run("C 正常RAG(非流式, max_tokens=800)", guard,
              [(T0 + i * 3.0, rag_body(i)) for i in range(40)])

    # D. 插正常请求洗风险: 10 条采集 + 5 条聊天交替, 共 300 条
    guard_d = DistillGuard(MemoryState(), GuardConfig(), rng=random.Random(2))
    mixed = []
    t = T0
    for _ in range(20):
        for _ in range(10):
            mixed.append((t, scraper_body())); t += 0.3
        for i in range(5):
            mixed.append((t, chat_body(int(t)))); t += 1.0
    await run("D 采集+插正常请求(10异常:5正常交替)", guard_d, mixed)
    d_scraper = await run("D 续: 洗完后再发同模板 60 条", guard_d,
                          [(t + i, scraper_body()) for i in range(60)])
    assert d_scraper["reject"] > 0, "插正常请求不应洗掉模板可疑度"

    # E. 轻度改写模板: 前缀指纹会变, 看 simhash 桶能兜住多少
    guard_e = DistillGuard(MemoryState(), GuardConfig(), rng=random.Random(3))
    await run("E 采集脚本(每条加随机词改写模板)", guard_e,
              [(T0 + i / 5, scraper_body(mutate=True)) for i in range(600)])

    print("=" * 96)
    print("预期: A/E 放行为小比例(小量放行), B/C 全放行, D 的正常请求不影响采集模板拦截。")


if __name__ == "__main__":
    asyncio.run(main())
