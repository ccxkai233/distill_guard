# distill_guard

一个放在 LLM API 网关前置的**防蒸馏 / 反批量采集**反向代理。

核心定位是 **过滤 + 小量放行**,而不是"发现即封号":对疑似批量采集的流量只放一小部分进上游,其余返回与上游逐字节一致的假错误;对正常聊天 / RAG 流量零干扰、零开销直通。

判定**按模板指纹聚合,不绑定账号**——号池分摊的多个账号只要发的是同一个模板,会落进同一个指纹桶被一起限,因此不需要单独做账号画像或号池识别。

---

## 它解决什么

针对的是"一个问题拆成很多次、同模板换参数批量打"的采集流量。这类请求的可观察特征,以及对应的处置机制:

| 观察到的流量特征 | 本项目的机制 |
| --- | --- |
| 没缓存、非流式、大输入小输出的基本都是采集 | **形态门**:只有命中这个形态的请求才进入风控;流式或带 `cache_control` 的请求零状态直通 |
| 相同的请求,很小,一个问题拆成很多次 | **模板指纹**(归一化后前缀哈希):把"换参数不换模板"的请求归并到同一个桶计数 |
| 同时进来的很多小请求,只让一部分进 | **每模板额度**:每个模板每 10s / 60s 有免费额度,超出后按比例放行,并永远保底放行一小部分 |
| 中间插正常请求想洗掉风险 | **只随时间半衰减的可疑度**:任何其他请求都稀释不了它;正常请求指纹不同,碰不到可疑模板的状态 |
| 很多报错是网关主动返回的 | **主动假错**:被拒请求根本不到上游,返回与真实上游逐字节一致的错误响应 |

明确的**非目标**:不追求 100% 判定某个请求是不是蒸馏;不对抗"每条都注入随机内容改写模板"的高熵变体(见文末[已知边界](#已知边界))。

---

## 判定流程

```
请求进来
  │
  ├─ 流式 / 带 cache_control ? ──是──▶ pass(直通, 不记状态)
  │
  否(命中形态门: 大输入小输出 / 输入输出比高 / 拆碎的小请求)
  │
  ▼
按模板指纹累加 10s、60s 计数, 读取该模板的可疑度(已按时间半衰减)
  │
  ├─ 未超免费额度 ? ──是──▶ 可疑度<0.5 → pass / 否则 → delay
  │
  否(超额)
  │
  ▼
抬升该模板可疑度, 计算放行比例 rate(随可疑度下降, 但不低于 min_pass_rate)
  │
  ├─ 随机数 < rate ? ──是──▶ delay(放行但加抖动)
  │                     └否──▶ reject(返回假错, 不到上游)
```

`pass` / `delay` 都会转发上游,区别只是 `delay` 会加一段随机抖动延迟;`reject` 直接返回假错。

---

## 项目结构

```
distill_guard/
├── distill_guard/
│   ├── __init__.py        # 对外导出 DistillGuard / GuardConfig / Decision 等
│   ├── __main__.py        # python3 -m distill_guard 入口
│   ├── fingerprint.py     # 归一化 + 前缀指纹 + SimHash 桶
│   ├── state.py           # 计数与可疑度存储: MemoryState / RedisState(fail-open)
│   ├── guard.py           # 核心判定: 形态门 + 额度放行 + 时间半衰减可疑度
│   └── proxy.py           # FastAPI 反向代理网关
├── simulate.py            # 离线验证脚本(无需 Redis / 外部服务)
└── README.md
```

三个核心模块 `fingerprint` / `state`(内存后端)/ `guard` 只依赖 Python 标准库。只有作为网关跑(`proxy.py` / `__main__.py`)或用 Redis 后端时才需要第三方依赖。

---

## 安装

```bash
# 作为网关运行所需
pip3 install fastapi uvicorn httpx

# 仅当使用 Redis 多实例共享状态时额外需要(redis>=4.2 自带 redis.asyncio)
pip3 install "redis>=4.2"
```

只把 `DistillGuard` 当库嵌进自己的网关、且用内存后端时,无需任何第三方依赖。

---

## 快速开始:作为网关运行

**第一步一定是影子模式(shadow)**——只判定、只记日志,不做任何处置,照常转发全部请求。用它观察几天真实流量,确认正常业务不进风控、放行比例合理,再开处置。

```bash
# 影子模式(默认): 只记日志, 不拦截
DG_SHADOW=1 \
DG_UPSTREAM=https://api.anthropic.com \
python3 -m distill_guard
```

日志是每请求一行 JSON,只含指纹和统计,**不落 prompt 原文**:

```json
{"path":"v1/messages","action":"reject","shadow":true,"suspicious":true,
 "fp":"9c1f...","n10":9,"n60":142,"susp":0.87,"pass_rate":0.03}
```

`shadow:true` 时 `action` 记的是"如果开处置会怎么做",实际请求仍全部转发。观察确认无误后,把 `DG_SHADOW` 置 0 开处置:

```bash
DG_SHADOW=0 python3 -m distill_guard
```

客户端把原来指向上游的 base URL 改成指向本网关即可,请求体、鉴权头原样透传。

### 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DG_UPSTREAM` | `https://api.anthropic.com` | 上游 API 地址 |
| `DG_SHADOW` | `1` | `1`=影子模式只记日志;`0`=开处置 |
| `DG_REDIS_URL` | 空 | 设置后用 Redis 多实例共享状态;不设则用单机内存 |
| `DG_HOST` | `0.0.0.0` | 监听地址 |
| `DG_PORT` | `8080` | 监听端口 |
| `DG_FREE_PER_MIN` | `6` | 同模板每分钟免费放行数 |
| `DG_FREE_PER_10S` | `3` | 同模板 10 秒突发免费数 |
| `DG_MIN_PASS_RATE` | `0.03` | 保底放行比例(拉满可疑度也放这么多) |
| `DG_SUSP_HALF_LIFE` | `600` | 可疑度半衰期(秒) |
| `DG_DELAY_MS` | `400` | delay 动作的抖动延迟基准(毫秒) |
| `DG_BIG_IN_TOKENS` | `2000` | 形态门:大输入阈值(token) |
| `DG_SMALL_OUT_TOKENS` | `256` | 形态门:小输出阈值(token) |

> 常用阈值都可用上面的环境变量覆盖。其余不常改的项(假错状态码 `upstream_error_status` / body、`io_ratio`、`tiny_in_tokens`、`susp_alpha`)仍在 `GuardConfig` 里,改源码或参照下面「作为库嵌入」自己构造 `GuardConfig` 传进去。

### 客户端接入(下游)

只改客户端的 base URL,其余代码不动;鉴权头由网关原样透传,网关不碰你的 Key。流式(SSE)、GET 端点(如 `/v1/models`)也照常透传。

**Anthropic SDK**

```python
from anthropic import Anthropic
client = Anthropic(api_key="sk-ant-...", base_url="http://你的网关:8080")
```

**OpenAI SDK**

```python
from openai import OpenAI
client = OpenAI(api_key="sk-...", base_url="http://你的网关:8080/v1")
```

**curl**

```bash
curl http://你的网关:8080/v1/messages \
  -H "x-api-key: sk-ant-..." \
  -H "anthropic-version: 2023-06-01" \
  -H "content-type: application/json" \
  -d '{"model":"claude-sonnet-4-5","max_tokens":32,
       "messages":[{"role":"user","content":"Q: 12345 + 67890 只回数字"}]}'
```

path 是透传的:客户端打 `/v1/messages`,网关转发到 `DG_UPSTREAM/v1/messages`。换上游只改 `DG_UPSTREAM`(Anthropic `https://api.anthropic.com`、OpenAI `https://api.openai.com`、或自建地址)。

---

## Docker 部署

仓库带了 `Dockerfile` 和 `docker-compose.yml`(网关 + Redis 一把起)。

单独构建运行:

```bash
docker build -t distill_guard .
docker run -p 8080:8080 \
  -e DG_UPSTREAM=https://api.anthropic.com \
  -e DG_SHADOW=1 \
  distill_guard
```

用 compose 连 Redis 一起起(生产推荐,多实例共享状态):

```bash
docker compose up -d          # 默认影子模式, 只记日志
docker compose logs -f gateway
```

调上游、开处置、改阈值都在 `docker-compose.yml` 的 `environment` 下改;开处置把 `DG_SHADOW` 改成 `"0"` 再 `docker compose up -d`。

---

## 配置项(GuardConfig)

`guard.py` 中的 `GuardConfig`,全部有默认值:

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `shadow` | `True` | 影子模式开关 |
| `free_per_min` | `6` | 同模板每分钟免费放行额度 |
| `free_per_10s` | `3` | 同模板 10 秒突发免费额度 |
| `min_pass_rate` | `0.03` | 永远保底的放行比例(即使可疑度拉满也放这么多) |
| `susp_half_life` | `600.0` | 可疑度半衰期(秒),只随时间衰减 |
| `susp_alpha` | `0.04` | 每次额度溢出抬升可疑度的幅度 |
| `delay_ms` | `400` | 放行但高可疑的请求所加的抖动延迟基准 |
| `small_out_tokens` | `256` | 形态门:小输出阈值 |
| `big_in_tokens` | `2000` | 形态门:大输入阈值 |
| `tiny_in_tokens` | `400` | 形态门:拆碎小请求的输入阈值 |
| `io_ratio` | `8.0` | 形态门:输入/输出 token 比阈值 |
| `upstream_error_status` | `529` | 假错的 HTTP 状态码 |
| `upstream_error_body` | overloaded_error | 假错的响应体 |

> **上线前务必核对假错的状态码和 body**,替换成你实际上游返回的原文。伪装错误必须和真实上游逐字节一致,否则对手一测就能区分出"这是被网关拦的"还是"上游真的过载"。

**形态门**(`_suspicious_shape`)命中任一条即算可疑,进入风控:
- 大输入小输出:`in_tokens ≥ big_in_tokens` 且 `max_tokens ≤ small_out_tokens`
- 输入输出比高:`in_tokens ≥ io_ratio × max_tokens`
- 拆碎的小请求:`in_tokens ≤ tiny_in_tokens` 且消息数 ≤ 2

`stream=true` 或请求里带 `cache_control` 的一律不进风控,直通。

---

## 作为库嵌入自己的网关

不想用自带的 FastAPI 代理,可以只用判定内核:

```python
import asyncio
from distill_guard import DistillGuard, GuardConfig, MemoryState

guard = DistillGuard(MemoryState(), GuardConfig(shadow=False))

async def handle(body: dict):
    d = await guard.check(body)          # body 是解析好的请求 JSON(dict)
    if d.action == "reject":
        return fake_upstream_error()     # 返回你的假错, 不转发
    if d.action == "delay":
        await asyncio.sleep(0.4)         # 可选的抖动
    return await forward_upstream(body)  # pass / delay 都转发
```

用 Redis 后端共享多实例状态:

```python
import redis.asyncio as aioredis
from distill_guard import DistillGuard, GuardConfig, RedisState

state = RedisState(aioredis.from_url("redis://localhost:6379", decode_responses=True))
guard = DistillGuard(state, GuardConfig(shadow=False))
```

`check()` 也接受已经算好的 token 数或注入的时间戳:

```python
d = await guard.check(body, in_tokens=8342)      # 用你自己的 tokenizer 结果
d = await guard.check(body, now=1_700_000_000.0)  # 注入时间, 便于回放/测试
```

### 判定结果 Decision

| 字段 | 说明 |
| --- | --- |
| `action` | `"pass"` / `"delay"` / `"reject"` |
| `suspicious` | 是否命中形态门(未命中即正常流量) |
| `fp` | 该请求的模板前缀指纹 |
| `n10` / `n60` | 该模板近 10s / 60s 的计数 |
| `susp` | 该模板当前可疑度(0~1) |
| `pass_rate` | 本次判定使用的放行比例(额度内为 `1.0`) |

---

## 离线验证

`simulate.py` 用模拟时钟把四类流量各跑一遍,不需要 Redis 或任何外部服务:

```bash
python3 simulate.py
```

参考输出(节选):

```
A 采集脚本(同模板小请求, 5rps*2min)   共 600  放行  37 ( 6.2%)  拒绝 563  末态可疑度=0.99
B 正常聊天(流式, 内容各异)            共  60  放行  60 (100.0%)  拒绝   0  末态可疑度=0.00
C 正常RAG(非流式, max_tokens=800)     共  40  放行  40 (100.0%)  拒绝   0  末态可疑度=0.00
D 续: 洗完后再发同模板 60 条           共  60  放行   3 ( 5.0%)  拒绝  57  末态可疑度=0.97
```

要点:采集流量(A)只放行个位数百分比,正常聊天和 RAG(B/C)100% 放行,中间插正常请求也洗不掉采集模板的拦截(D)。

---

## 存储与多实例

- **MemoryState**:单机 / 测试用,状态在进程内,自带上限裁剪。
- **RedisState**:多网关实例共享同一份计数和可疑度。**任何 Redis 异常一律 fail-open**(当作零计数放行),风控组件故障不会放大成平台不可用。

计数用"当前桶 + 上一桶"两个固定时间桶近似滑动窗口,覆盖最近 `win ~ 2×win` 秒,在桶边界宁可略微高估也不漏计。Redis key 都带 TTL 自动过期,无需清理任务。

---

## 上线流程建议

1. **影子模式跑几天**(`DG_SHADOW=1`):只看日志,确认正常业务基本不进风控、采集模板的 `susp` 能涨上去。
2. 按日志里的 `n60` / `pass_rate` 分布微调 `free_per_min`、`min_pass_rate` 等阈值。
3. **核对假错**:把 `upstream_error_status` / `upstream_error_body` 换成真实上游原文。
4. `DG_SHADOW=0` 开处置,先盯投诉和误伤,再逐步收紧额度。

---

## 已知边界

判定的最强一环是模板前缀指纹,它能吃掉"换数字 / UUID / 长 hex / base64 等参数"的变体(归一化会把这些抹成占位符)。SimHash 桶作为兜底,对**轻度**改写(长模板里加几个词)仍能聚合。

但对"短模板 + 每条都注入随机内容"的高熵变体,前缀指纹会各不相同、短文本下 SimHash 也会被随机词打散,此时会退化到放行——按当前需求,这类**不在拦截目标内**。真实采集脚本几乎都带固定的长 prompt 骨架,属于能拦住的一类。
