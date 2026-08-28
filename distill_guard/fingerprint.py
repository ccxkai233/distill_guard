"""文本归一化与模板指纹。

目标: 让"同一模板不同 payload"的请求归一后落到同一个指纹,
用于识别"一个问题拆成很多次 / 批量换参数"的采集流量。
"""

from __future__ import annotations

import hashlib
import re
from typing import Iterable

_WS = re.compile(r"\s+")
# 会被抹掉的高熵 payload: 长 hex / 数字 / base64 片段
_VAR = re.compile(r"\b(?:[0-9a-f]{8,}|\d{3,}|[A-Za-z0-9+/]{40,}={0,2})\b")

# 指纹只看前 20k 字符, 超长 prompt 不允许拖慢同步路径
MAX_FP_CHARS = 20_000


def normalize(text: str) -> str:
    text = text[:MAX_FP_CHARS].lower()
    text = _VAR.sub("\x00", text)
    return _WS.sub(" ", text).strip()


def _shingles(text: str, k: int = 4) -> Iterable[str]:
    toks = text.split()
    if len(toks) < k:
        yield text
        return
    for i in range(len(toks) - k + 1):
        yield " ".join(toks[i : i + k])


def simhash64(text: str) -> int:
    v = [0] * 64
    for sh in _shingles(text):
        h = int.from_bytes(hashlib.blake2b(sh.encode(), digest_size=8).digest(), "big")
        for i in range(64):
            v[i] += 1 if (h >> i) & 1 else -1
    out = 0
    for i in range(64):
        if v[i] > 0:
            out |= 1 << i
    return out


def hamming(a: int, b: int) -> int:
    return bin(a ^ b).count("1")


def prefix_fp(text: str, n: int = 240) -> str:
    """模板骨架指纹: 归一化文本前 n 字符的哈希。"""
    return hashlib.blake2b(text[:n].encode(), digest_size=8).hexdigest()


def sim_bucket(sh: int, bits: int = 20) -> int:
    """SimHash 高位分桶。轻度改写的模板大概率仍落同桶, 作为前缀指纹的兜底。"""
    return sh >> (64 - bits)
