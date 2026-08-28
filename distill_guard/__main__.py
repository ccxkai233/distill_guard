import logging
import os

import uvicorn

from .guard import GuardConfig
from .proxy import create_app


def _cfg_from_env() -> GuardConfig:
    """默认值来自 GuardConfig; 环境变量存在时才覆盖对应项。"""
    cfg = GuardConfig(shadow=os.environ.get("DG_SHADOW", "1") != "0")
    overrides = {
        "DG_FREE_PER_MIN": ("free_per_min", int),
        "DG_FREE_PER_10S": ("free_per_10s", int),
        "DG_MIN_PASS_RATE": ("min_pass_rate", float),
        "DG_SUSP_HALF_LIFE": ("susp_half_life", float),
        "DG_DELAY_MS": ("delay_ms", int),
        "DG_BIG_IN_TOKENS": ("big_in_tokens", int),
        "DG_SMALL_OUT_TOKENS": ("small_out_tokens", int),
    }
    for env, (field, cast) in overrides.items():
        v = os.environ.get(env)
        if v not in (None, ""):
            setattr(cfg, field, cast(v))
    return cfg


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg = _cfg_from_env()
    upstream = os.environ.get("DG_UPSTREAM", "https://api.anthropic.com")
    redis_url = os.environ.get("DG_REDIS_URL")
    log = logging.getLogger("distill_guard")
    log.info("distill_guard start: upstream=%s shadow=%s redis=%s free_per_min=%d",
             upstream, cfg.shadow, bool(redis_url), cfg.free_per_min)

    app = create_app(upstream_base=upstream, redis_url=redis_url, cfg=cfg)
    uvicorn.run(app, host=os.environ.get("DG_HOST", "0.0.0.0"),
                port=int(os.environ.get("DG_PORT", "8080")))


if __name__ == "__main__":
    main()
