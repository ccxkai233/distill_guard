"""反向代理网关: 判定 -> (shadow 只记日志 | reject 返假错 | delay 抖动) -> 转发上游。

只有 POST + 非空 JSON 对象 body 的请求走风控判定(补全类端点都是 POST);
其余方法(GET /v1/models 之类)、空 body、非 JSON / 非对象 body 一律原样透传,
不进风控、不记状态。

日志只记指纹和统计, 不落 prompt 原文。

注意: 第三方依赖在模块顶层导入。因为本文件用了 PEP 563(from __future__ import
annotations), 路由函数 relay 的 `request: Request` 注解会被延迟成字符串, FastAPI
只能从**模块全局**里解析它 —— 所以 Request 必须在模块级可见, 不能只在函数内 import,
否则每个请求都会被当成缺少必填 query 参数而返回 422。
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.background import BackgroundTask

from .guard import DistillGuard, GuardConfig
from .state import MemoryState, RedisState

log = logging.getLogger("distill_guard")

_HOP = {
    "host", "content-length", "connection", "keep-alive", "proxy-authenticate",
    "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade",
}


def create_app(upstream_base: str, redis_url: str | None = None, cfg=None):
    if redis_url:
        import redis.asyncio as aioredis

        state = RedisState(aioredis.from_url(redis_url, decode_responses=True))
    else:
        state = MemoryState()

    cfg = cfg or GuardConfig()
    guard = DistillGuard(state, cfg)
    client = httpx.AsyncClient(base_url=upstream_base, timeout=600.0)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        await client.aclose()  # 关闭连接池, 释放上游连接

    app = FastAPI(lifespan=lifespan)
    app.state.guard = guard

    async def _forward(method: str, path: str, raw: bytes, request: Request):
        headers = {k: v for k, v in request.headers.items() if k.lower() not in _HOP}
        upstream_req = client.build_request(
            method, "/" + path, content=raw, headers=headers,
            params=str(request.query_params),
        )
        upstream_resp = await client.send(upstream_req, stream=True)
        resp_headers = {k: v for k, v in upstream_resp.headers.items() if k.lower() not in _HOP}
        return StreamingResponse(
            upstream_resp.aiter_raw(),
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            background=BackgroundTask(upstream_resp.aclose),
        )

    @app.get("/healthz")
    async def healthz():
        # 本地存活探针, 不转发上游。注册在 catch-all 之前才能命中。
        return {"status": "ok", "shadow": cfg.shadow}

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
    async def relay(path: str, request: Request):
        raw = await request.body()

        # 非 POST / 空 body: 不进风控, 原样透传(models 列表、健康检查等)
        if request.method != "POST" or not raw:
            return await _forward(request.method, path, raw, request)

        try:
            body = json.loads(raw)
        except Exception:
            body = None
        # 非 JSON 对象或空对象不是补全请求, 直接透传, 不进风控
        # (避免把 {} 当成"拆碎的小请求"误判为可疑)
        if not isinstance(body, dict) or not body:
            return await _forward("POST", path, raw, request)

        d = await guard.check(body)
        log.info(json.dumps({
            "path": path, "action": d.action, "shadow": cfg.shadow,
            "suspicious": d.suspicious, "fp": d.fp, "n10": d.n10, "n60": d.n60,
            "susp": round(d.susp, 3), "pass_rate": round(d.pass_rate, 3),
        }, ensure_ascii=False))

        if not cfg.shadow:
            if d.action == "reject":
                return JSONResponse(cfg.upstream_error_body,
                                    status_code=cfg.upstream_error_status)
            if d.action == "delay":
                await asyncio.sleep(cfg.delay_ms / 1000 * random.uniform(0.6, 1.4))

        return await _forward("POST", path, raw, request)

    return app
