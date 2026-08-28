"""反向代理网关: 判定 -> (shadow 只记日志 | reject 返假错 | delay 抖动) -> 转发上游。

只有 POST + JSON body 的请求走风控判定(补全类端点都是 POST);
其余方法(GET /v1/models 之类)与非 JSON body 原样透传, 不进风控、不记状态。

日志只记指纹和统计, 不落 prompt 原文。
"""

from __future__ import annotations

import asyncio
import json
import logging
import random

log = logging.getLogger("distill_guard")

_HOP = {
    "host", "content-length", "connection", "keep-alive", "proxy-authenticate",
    "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade",
}


def create_app(upstream_base: str, redis_url: str | None = None, cfg=None):
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse, StreamingResponse
    from starlette.background import BackgroundTask
    import httpx

    from .guard import DistillGuard, GuardConfig
    from .state import MemoryState, RedisState

    if redis_url:
        import redis.asyncio as aioredis

        state = RedisState(aioredis.from_url(redis_url, decode_responses=True))
    else:
        state = MemoryState()

    cfg = cfg or GuardConfig()
    guard = DistillGuard(state, cfg)
    client = httpx.AsyncClient(base_url=upstream_base, timeout=600.0)

    app = FastAPI()
    app.state.guard = guard

    async def _forward(method: str, path: str, raw: bytes, request: "Request"):
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

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
    async def relay(path: str, request: Request):
        raw = await request.body()

        # 非 POST / 空 body: 不进风控, 原样透传(models 列表、健康检查等)
        if request.method != "POST" or not raw:
            return await _forward(request.method, path, raw, request)

        try:
            body = json.loads(raw)
            if not isinstance(body, dict):
                body = {}
        except Exception:
            body = {}

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
