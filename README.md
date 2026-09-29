# 一次性内容投递箱 API（One-Time Drop Box）

发送者创建带过期时间的 **文本** 或 **小文件** 投递，得到一个取件令牌；
收件人凭令牌取件，**内容只能被成功读取一次**。并发取件时仅有一个请求得到内容，
其余请求以及「已撤回 / 已过期 / 已消费」都会得到明确、可区分的结果。

此外支持**盲投递请求（Blind Submission Request）**：收件人先创建一个**空投递槽**，
对外只分发一次性**提交令牌**，自己保留私有**取件令牌**。任何拿到提交令牌的人都可以
投递一份文本或小文件，但**并发下只有一个提交者能成功写入**；取件令牌在内容提交前
查询得到 `pending`，提交后沿用同一套原子单次取件语义。

- **FastAPI**（异步 HTTP + 自动 OpenAPI 文档）
- **PostgreSQL**：投递内容与审计的唯一可信存储，**原子消费/原子提交的最终裁决者**
- **Redis**：终态快速拒绝缓存（best-effort，宕机不影响正确性）
- 令牌只存 SHA-256 哈希；明文令牌仅在创建响应中出现一次

---

## 一条命令启动

需要 Docker（含 Compose 插件）：

```bash
docker compose up --build
```

启动后：

| 服务 | 地址 |
| --- | --- |
| **API（对外端口）** | **http://localhost:8000** |
| Swagger 交互文档 | http://localhost:8000/docs |
| 健康检查 | http://localhost:8000/health |
| PostgreSQL | 仅集群内部 `db:5432`（不对宿主机暴露） |
| Redis | 仅集群内部 `redis:6379`（不对宿主机暴露） |

停止并清理数据卷：

```bash
docker compose down -v
```

> API 对外只暴露 **8000** 一个端口。数据库与 Redis 不映射到宿主机。

---

## 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/v1/deliveries` | 创建文本投递（JSON） |
| `POST` | `/api/v1/deliveries/file` | 创建小文件投递（multipart/form-data） |
| `POST` | `/api/v1/pickup/{token}` | 凭令牌取件（**只能成功一次**） |
| `POST` | `/api/v1/pickups/batch` | **批量取件**：1～20 个普通/盲槽取件令牌一次性领齐（整批原子消费） |
| `POST` | `/api/v1/revoke/{token}` | 发送者撤回投递 |
| `POST` | `/api/v1/blind-slots` | **盲投递**：收件人创建空槽，返回提交令牌 + 私有取件令牌 |
| `POST` | `/api/v1/blind-slots/submit/{submit_token}` | **盲投递**：凭提交令牌写入文本（并发仅一人成功） |
| `POST` | `/api/v1/blind-slots/submit/{submit_token}/file` | **盲投递**：凭提交令牌写入小文件 |
| `POST` | `/api/v1/blind-slots/pickup/{token}` | **盲投递**：收件人取件（提交前 `pending`，提交后原子单次） |
| `POST` | `/api/v1/blind-slots/revoke/{token}` | **盲投递**：收件人凭取件令牌撤回空槽/已提交投递 |
| `GET` | `/api/v1/audit/export` | 按时间范围导出审计链记录（**NDJSON 流**，只读） |
| `GET` | `/api/v1/audit/verify` | 校验库内审计链完整性（只读） |
| `POST` | `/api/v1/audit/verify` | 校验一份导出的 NDJSON（按行顺序，可发现乱序） |
| `GET` | `/health` | 健康检查 |

### 取件/撤回的明确结果

取件失败时返回统一结构 `{"result": <状态>, "detail": <中文说明>}`，状态与状态码如下：

| `result` | 含义 | HTTP 状态码 |
| --- | --- | --- |
| `success` | 取件成功（文本返回 JSON，文件返回二进制流） | `200` |
| `consumed` | 令牌已被成功取件，内容已不可再读 | `410 Gone` |
| `expired` | 投递已过期 | `410 Gone` |
| `revoked` | 投递已被发送者撤回（取件）/ 无法撤回（已被取走见下） | 取件 `409 Conflict` |
| `unknown` | 令牌不存在 | `404 Not Found` |

撤回接口：成功 `200 {"result":"revoked"}`；已被取走或已过期返回 `410`
（`result` 为 `consumed` / `expired`）；令牌不存在 `404`。

### 盲投递的额外结果

| `result` | 出现接口 | 含义 | HTTP 状态码 |
| --- | --- | --- | --- |
| `pending` | 盲槽取件 | 空槽已创建，但尚无提交者写入内容（不消费取件令牌，可反复查询） | `200` |
| `submitted` | 盲槽提交 | 空槽已被**另一个提交者**成功写入，提交令牌只能成功使用一次 | `409 Conflict` |

盲槽提交失败：`submitted` / `revoked` 返回 `409`，`expired` 返回 `410`，
提交令牌不存在（或拿取件令牌来提交）返回 `404 unknown`。盲槽取件/撤回的
其余结果与普通投递一致。

---

## 使用示例

### 1) 创建文本投递

```bash
curl -s -X POST http://localhost:8000/api/v1/deliveries \
  -H 'Content-Type: application/json' \
  -d '{"text": "这是读一次就消失的秘密", "ttl_seconds": 3600}'
```

响应（`token` 仅此一次返回，请立即保存）：

```json
{
  "delivery_id": "8f1c…-…-…",
  "token": "U9rX…取件令牌…",
  "expires_at": "2026-09-21T10:00:00Z",
  "url": "http://localhost:8000/api/v1/pickup/U9rX…"
}
```

### 2) 取件（第一次成功）

```bash
curl -s -X POST "http://localhost:8000/api/v1/pickup/U9rX…"
```

```json
{
  "result": "success",
  "delivery_id": "8f1c…",
  "kind": "text",
  "content": "这是读一次就消失的秘密",
  "content_sha256": "…",
  "consumed_at": "2026-09-21T09:05:11Z"
}
```

再取一次：

```bash
curl -s -X POST "http://localhost:8000/api/v1/pickup/U9rX…" -i
# HTTP/1.1 410 Gone
# {"result":"consumed","detail":"该令牌已被成功取件，内容仅可读取一次。"}
```

### 3) 创建小文件并取件

```bash
# 创建（默认上限 1 MiB）
curl -s -X POST http://localhost:8000/api/v1/deliveries/file \
  -F 'file=@/path/to/report.pdf' \
  -F 'ttl_seconds=600'

# 取件（成功返回二进制流，响应头带校验值）
curl -X POST "http://localhost:8000/api/v1/pickup/<token>" \
  -D - -o downloaded.pdf
# X-Drop-Result: success
# X-Content-SHA256: <sha256>
# Content-Disposition: attachment; filename="report.pdf"; …
```

### 4) 撤回

```bash
curl -s -X POST "http://localhost:8000/api/v1/revoke/<token>"
# 成功：   200 {"result":"revoked", ...}
# 已取走： 410 {"result":"consumed", ...}
# 已过期： 410 {"result":"expired", ...}
```

撤回后再取件得到 `409 {"result":"revoked"}`。

---

## 批量取件（多份文本一次性领齐）

收件人常常需要把多份文本在一次调用里全部取走。`POST /api/v1/pickups/batch`
接受一个按顺序排列的取件令牌列表（普通文本投递与**已提交盲槽**文本均可），
在**单个数据库事务内整批裁决**：

- 请求体 `{"tokens": [...]}`，长度 **1～20**；空列表、超过 20 项或存在
  **重复令牌**一律在接口层拒绝（`422`），整批不进入裁决、不消费任何内容。
- 只有全部对应记录都满足「**已提交（盲槽）、未到期、未消费、未撤回，且为
  文本**」时，才一起消费，并**按输入顺序**返回每份的投递 ID、文本与内容
  哈希。
- 任一项不满足时，**整批失败**（`409`）：只返回各失败项的输入位置
  （`position`，**从 0 开始**）与原因 `reason`，不返回任何内容，也不消费
  其余项——失败批次绝不留下部分消费。
- **文件投递无法批量领取**，对应项返回 `type_mismatch`（普通文件与盲槽文件
  都一样），文件仍可走单件下载接口；盲槽**提交令牌**没有取件权限，按
  `unknown` 处理。
- **到期时刻等于裁决时刻视为过期**（`expires_at <= now`）。

批量失败原因 `reason` 一览：

| `reason` | 含义 |
| --- | --- |
| `unknown` | 取件令牌不存在（盲槽提交令牌无取件权限，同样按未知处理） |
| `pending` | 盲槽尚无提交内容，处于待提交状态 |
| `expired` | 已过期（到期时刻等于裁决时刻也算） |
| `consumed` | 该内容已被成功领取，仅可读取一次 |
| `revoked` | 已被撤回 |
| `type_mismatch` | 该令牌对应文件投递，批量取件仅交付文本 |

### 9) 一次性领取多份文本（全部成功）

```bash
curl -s -X POST http://localhost:8000/api/v1/pickups/batch \
  -H 'Content-Type: application/json' \
  -d '{"tokens": ["U9rX…令牌A…", "Wd9X…盲槽取件令牌B…", "m2pL…令牌C…"]}'
```

成功（`200`，`items` 顺序与输入一致；`batch_id` 仅用于审计关联）：

```json
{
  "result": "success",
  "batch_id": "5c8e0b7a-…-…",
  "count": 3,
  "consumed_at": "2026-09-29T09:10:00.000000Z",
  "items": [
    {"position": 0, "delivery_id": "8f1c…", "kind": "text",
     "content": "第一份文本", "content_sha256": "…"},
    {"position": 1, "delivery_id": "3b2e…", "kind": "text",
     "content": "盲槽里那份文本", "content_sha256": "…"},
    {"position": 2, "delivery_id": "a77d…", "kind": "text",
     "content": "第三份文本", "content_sha256": "…"}
  ]
}
```

整批成功后，每个令牌都与单件取件一样进入 `consumed` 终态，再次单件或批量
取件都得到 `consumed`。

### 10) 任一项不满足：整批失败，零消费、零内容

```bash
# 第 1 项是文件、第 3 项已过期
curl -s -X POST http://localhost:8000/api/v1/pickups/batch \
  -H 'Content-Type: application/json' \
  -d '{"tokens": ["fileToken", "goodToken", "expiredToken"]}'
```

```json
// HTTP 409 Conflict（响应中没有任何 content 字段）
{
  "result": "failed",
  "batch_id": "9ad1…",
  "failures": [
    {"position": 0, "reason": "type_mismatch"},
    {"position": 2, "reason": "expired"}
  ],
  "detail": "批量取件存在不可领取项，整批未消费任何内容。"
}
```

注意第 2 项（`goodToken`）虽然本身可领，但因同批他项失败而**保持未消费**，
随后用单件取件仍能成功取出。空列表 / 超过 20 项 / 令牌重复则直接 `422`：

```bash
curl -i -X POST http://localhost:8000/api/v1/pickups/batch \
  -H 'Content-Type: application/json' \
  -d '{"tokens": ["same", "same"]}'
# HTTP/1.1 422 Unprocessable Entity
```

### 批量取件的并发与原子性

批量裁决复用单件取件的数据库裁决模式，并把整批放进**同一个事务**：

```sql
-- 先按固定 id 顺序锁定本批全部命中行（普通投递 + 盲槽）
SELECT … FROM deliveries
 WHERE token_hash = ANY(:hashes) ORDER BY id FOR UPDATE;
-- 锁内逐项分类；全部为「active 文本且未到期」时，一条白名单 UPDATE 整批消费
UPDATE deliveries SET status='consumed', consumed_at=:now
 WHERE id IN (:batch_ids) AND status='active' AND expires_at > :now;
```

- 重叠批次、单件取件、撤回并发时，PostgreSQL 行锁把对同一行的裁决串行化：
  每份内容**至多被成功领取一次**；固定按 id 加锁使批次之间不会互相死锁。
- 条件 UPDATE 影响行数不等于批大小，说明锁等待期间有并发赢家改写了其中某
  行——整事务**回滚重试**，因此失败/重试都不会出现“消费了一半”的提交。
- 快速拒绝路径（Redis 已有某项终态标记）只做**只读分类**，不锁行、不消费；
  正确性始终以数据库为唯一裁决者，**Redis 不可用时自动回落**，原子性不变。

---

## 盲投递请求（收件人开槽，提交者竞写一份）

适用场景：收件人希望别人**主动投递**一份内容给自己，但事先不知道谁会投递；
需要保证自己是唯一能取件的人，并且多个候选提交者中只有一个能写入。

### 5) 收件人创建空槽

```bash
curl -s -X POST http://localhost:8000/api/v1/blind-slots \
  -H 'Content-Type: application/json' \
  -d '{"ttl_seconds": 3600}'
```

响应中**两个令牌都只出现这一次**，请分别保存：

```json
{
  "slot_id": "3b2e…",
  "submit_token": "kH7q…一次性提交令牌…",
  "pickup_token": "Wd9X…收件人私有取件令牌…",
  "expires_at": "2026-09-21T11:00:00Z",
  "submit_url": "http://localhost:8000/api/v1/blind-slots/submit/kH7q…",
  "pickup_url": "http://localhost:8000/api/v1/blind-slots/pickup/Wd9X…",
  "revoke_url": "http://localhost:8000/api/v1/blind-slots/revoke/Wd9X…"
}
```

收件人把 `submit_url`（或 `submit_token`）通过任意渠道发给一个或多个候选提交者，
**自己保留 `pickup_token`**。提交者无法从提交令牌推知取件令牌，也无法取件。

### 6) 提交者投递内容（文本）

```bash
curl -s -X POST "http://localhost:8000/api/v1/blind-slots/submit/kH7q…" \
  -H 'Content-Type: application/json' \
  -d '{"text": "这是只会被收件人读到一次的盲投内容"}'
```

成功（`200`；注意响应**不包含**任何取件地址或取件令牌）：

```json
{
  "result": "success",
  "slot_id": "3b2e…",
  "kind": "text",
  "content_sha256": "…",
  "submitted_at": "2026-09-21T10:20:33Z"
}
```

小文件走 multipart：

```bash
curl -s -X POST "http://localhost:8000/api/v1/blind-slots/submit/kH7q…/file" \
  -F 'file=@/path/to/sealed.pdf'
```

若另一个提交者已经抢先写入，后续提交得到明确结果，且槽中内容永远是赢家那份：

```json
// HTTP 409 Conflict
{"result": "submitted", "detail": "该空槽已有提交者成功写入，提交令牌只能成功使用一次。"}
```

收件人撤回后提交：`409 {"result":"revoked"}`；空槽过期后提交：`410 {"result":"expired"}`。

### 7) 收件人取件（提交前 pending，提交后单次成功）

内容尚未提交时，取件令牌不报错、不消费，返回明确的 `pending`（HTTP 200）：

```bash
curl -s -X POST "http://localhost:8000/api/v1/blind-slots/pickup/Wd9X…"
# {"result":"pending","slot_id":"3b2e…","expires_at":"2026-09-21T11:00:00Z"}
```

提交完成后，同一接口即按普通取件的规则**原子单次**返回内容
（文本为 JSON，文件为带 `X-Content-SHA256` 头的二进制流）；再次取件得到
`410 {"result":"consumed"}`。

### 8) 收件人撤回

```bash
# 空槽阶段撤回：阻止一切后续提交
# 已提交待取件阶段撤回：内容不再可被取走
curl -s -X POST "http://localhost:8000/api/v1/blind-slots/revoke/Wd9X…"
# 200 {"result":"revoked", ...}；重复撤回 200 {"result":"already_revoked"}
```

### 两类令牌的权限隔离

- 提交令牌与取件令牌分别独立生成（均为 32 字节 `secrets.token_urlsafe`），
  **分别做 SHA-256 哈希、存于不同列、各有独立唯一索引**；
- 提交接口只按提交令牌列定位盲槽；取件/撤回只按取件令牌列且仅匹配盲槽；
- **任何交叉使用都返回 `404 unknown`**：拿取件令牌提交、拿提交令牌取件/撤回、
  把盲槽令牌用于普通投递接口、把普通投递令牌用于盲槽接口，均无任何权限；
- Redis 终态缓存也按令牌类型使用独立键前缀，一个接口的终态不会在另一个
  接口上被命中；
- 提交成功响应刻意不回传取件地址，提交者无法借此获知取件能力。

---

## 并发原子消费是如何保证的

这是本服务的核心。正确性**只依赖 PostgreSQL 的单条条件更新**，不依赖应用层锁：

```sql
UPDATE deliveries
SET status = 'consumed', consumed_at = now
WHERE token_hash = :token_hash
  AND status = 'active'
  AND expires_at > now
RETURNING id, content, …;
```

- 多个并发请求执行同一条语句时，PostgreSQL 用**行锁**把对该行的更新串行化；
- 只有**第一个**事务满足 `status='active' AND 未过期`，它把状态改成 `consumed`
  并通过 `RETURNING` 拿到内容（唯一赢家）；
- 其余事务的 `WHERE` 不再匹配，返回 0 行，随后在新事务里读取当前终态，
  得到明确的 `consumed` / `revoked` / `expired`；
- 未命中的事务**不会在旧快照里回写任何状态**，因此不可能覆盖赢家刚提交的终态；
- 内容仅在赢家事务中随 `RETURNING` 取出一次，响应后不落缓存。

**Redis 的角色（非正确性依赖）**：只缓存终态标记（`consumed` / `revoked` /
`expired`），用于对已知失效令牌快速拒绝、减轻数据库压力；**绝不缓存明文内容**，
采用 Lua「仅当不存在时设置」保证终态不被互相覆盖，并设置 TTL。Redis 不可用时
请求自动回落到数据库判定，原子消费依旧成立。

**过期**：取件/撤回时惰性提升 + 后台协程每 30 秒批量兜底标记过期。

### 撤回与取件竞争

撤回使用同样的单条条件更新（目标状态 `revoked`）。它与取件互斥：
要么内容被取走（撤回得到 `consumed`），要么撤回先成功（取件得到 `revoked`），
不会出现“既取走又撤回”。

### 批量取件的整批原子性

批量取件把「锁行 → 锁内分类 → 整批条件 UPDATE」收进**同一个事务**
（SQL 见上文「批量取件的并发与原子性」）：

- 先以 `… ORDER BY id FOR UPDATE` 固定顺序锁定本批命中的全部行
  （普通投递与盲槽同表，仅按取件令牌列定位），再在锁保护下逐项分类；
- 任一项为 `unknown/pending/expired/consumed/revoked/type_mismatch` 时
  立即回滚，只在**独立事务**里惰性提升到期行，返回各项原因——本批没有
  任何状态迁移，其余可领项原样保留；
- 全部可领时用一条 id 白名单 UPDATE 整批置 `consumed`。在 READ COMMITTED
  下，若锁等待期间有单件取件/撤回/另一批次抢先改写了其中某行，该语句影响
  行数会少于批大小，事务整体回滚并重做分类/重试，因此**部分消费不可能被
  提交**；
- 重叠批次都按同一 id 顺序申请行锁，批次之间不会循环等待（死锁）；
  与单件操作的极端交错若被 PostgreSQL 死锁检测器中止，应用层整事务重试。

### 盲槽提交的并发裁决与状态机

盲槽复用同一张 `deliveries` 表与同一种裁决模式。空槽是一行
`status='pending'`、内容为 `NULL` 的记录，提交就是一条把**内容写入与状态迁移
合并在一起**的条件更新：

```sql
UPDATE deliveries
SET status = 'active',
    kind = :kind, content = :content, content_size = :size,
    content_sha256 = :digest, submitted_at = now
WHERE submit_token_hash = :submit_token_hash
  AND status = 'pending'
  AND expires_at > now
RETURNING id, submitted_at, …;
```

- 多个提交者并发执行时，行锁只放行**一个**事务，槽原子地从 `pending` 变为
  `active` 且内容一次写入——不会出现“状态已激活但内容为空”的半成品；
- 其余提交者 `WHERE` 不再匹配（槽已是 `active`，或紧接取件后为 `consumed`），
  统一得到明确的 `submitted`；撤回/过期竞争分别得到 `revoked` / `expired`；
- 提交前的取件请求只读到 `pending`，**不做任何状态迁移**，因此可反复查询；
  提交后的取件请求与普通投递完全相同（`active → consumed` 条件更新，单次）；
- 撤回对 `pending` 与 `active` 两种状态都成立；过期清扫同时覆盖这两种状态。

盲槽完整状态机（粗体为盲投递新增）：

```
                 提交（条件更新，唯一赢家）
 pending ───────────────────────────────► active ──取件──► consumed
    │                                        │
    ├────── 撤回 ──────► revoked ◄───────────┘
    └── 过期 ──► expired ◄──── active 到期（提交后未取件同样过期）
```

---

## 审计与可验证完整性（哈希链）

所有关键动作写入 `audit_logs` 表，并且**每一条新日志（包括未知令牌等失败
请求）都在同一个 PostgreSQL 事务中加入一条只增哈希链**：

- 动作：`create` / `pickup` / `batch_pickup` / `revoke`，以及盲投递的
  `blind_create` / `blind_submit` / `blind_pickup` / `blind_revoke`
- 结果：`success` / `consumed` / `revoked` / `expired` / `unknown` /
  `pending` / `submitted` / `failed` / `type_mismatch`
  （`batch_pickup` 对每项各记一条：整批失败时，本可领取但因同批他项失败
  而未消费的项记 `failed`，真正失败项记其原因）
- 关联 `delivery_id`（可空）、`token_hash`、客户端 IP、User-Agent、时间与少量详情
- **未知令牌的尝试也留痕并入链**（`delivery_id` 为空、仅记录哈希），便于暴力探测审计
- 盲槽创建审计记录取件令牌哈希，并在 `detail.submit_token_hash` 中留存提交令牌
  哈希；提交类审计只记录提交令牌哈希，取件/撤回类审计只记录取件令牌哈希
- 批量取件的每条审计在 `detail` 中记录 `position`（输入位置）、`batch_id`
  与 `batch_size` 用于关联同一批裁决；`batch_id` 只是随机 UUID，不含任何
  令牌或内容信息

明文令牌、明文内容不写入审计，也不会出现在任何导出/校验数据中。

### 每条记录的链字段

| 字段 | 含义 |
| --- | --- |
| `seq` | 全链单调递增序号（从 1 开始，唯一约束兜底） |
| `event_digest` | 规范化事件摘要的 SHA-256 |
| `prev_hash` | 前一条记录的链哈希；序号 1 的 `prev_hash` 为固定创世哈希 |
| `entry_hash` | 本条链哈希：`SHA256(prev_hash ‖ "|" ‖ event_digest)` |

**规范化事件摘要**对业务字段（格式版本、序号、UTC 微秒时间、动作、结果、
`delivery_id`、`token_hash`、IP、UA、详情）做确定性序列化：UTF-8、键排序、
紧凑 JSON，再取 SHA-256。因此任一字段被改写，摘要都会失配。

> 链只能阻止“非重算式”篡改。拥有数据库写权限的攻击者若从某点起重算全部
> 后续哈希，表面校验仍可通过——因此**导出接口返回的 `X-Chain-End-Hash`
> 应由外部独立留存**（如每次导出后另行存档），作为日后校验的信任锚。

### 并发写入：链序由数据库裁决

链游标是 `audit_chain_state` 表中的单行 `(last_seq, last_hash)`。追加一条
审计时，在请求所在事务内：

1. 读取当前游标，计算新序号/摘要/链哈希；
2. **先**执行条件更新抢占游标：
   `UPDATE audit_chain_state SET last_seq=new, last_hash=… WHERE id=1 AND last_seq=:read`；
3. 仅当更新命中（`rowcount=1`）才插入审计行并提交；未命中说明他人已推进
   游标，本事务没有产生任何写入，重读游标后重试。

这与原子消费使用完全相同的裁决模式：PostgreSQL（READ COMMITTED）用行锁
把对游标单行的更新串行化，竞争事务的 `WHERE` 在锁释放后按最新行版本重新
判定，只有一个事务命中。链的全序因此由数据库裁决，不依赖应用层锁。

### 导出 NDJSON（只读）

```bash
# 全量导出（左闭右开时间范围，两个参数均可省略）
curl -s -D headers.txt -o audit.ndjson \
  "http://localhost:8000/api/v1/audit/export?start=2026-09-21T00:00:00Z&end=2026-09-22T00:00:00Z"

cat headers.txt | grep -i x-chain
# X-Chain-Genesis-Hash: <64 位十六进制>
# X-Chain-Start-Hash:   <范围内第一条记录的链哈希，空范围为空>
# X-Chain-End-Hash:     <范围内最后一条记录的链哈希，空范围为空>
# X-Chain-Record-Count: <记录数>
```

- 每行一条记录（`application/x-ndjson`），字段仅为审计元数据 + 链哈希，
  **不含投递明文内容**；
- 计数、起止哈希与流内容在同一个 **READ ONLY + REPEATABLE READ** 事务中
  取得，共享同一快照（驱动层拒绝任何写操作）；
- 建议每次导出后把响应头中的 `X-Chain-End-Hash` 与记录数独立存档。

导出的 NDJSON 长这样（一行）：

```json
{"action":"create","client_ip":null,"created_at":"2026-09-21T09:05:11.123456Z","delivery_id":"8f1c…","detail":{"kind":"text","size":30,"ttl_seconds":3600},"entry_hash":"…","event_digest":"…","prev_hash":"…","result":"success","seq":1,"token_hash":"…","user_agent":null}
```

### 校验：五类问题，指出第一处

```bash
# 校验库内全链（GET 版按库内 seq 顺序；只读，不产生新审计记录）
curl -s http://localhost:8000/api/v1/audit/verify
# {"ok":true,"count":12,"first_seq":1,"last_seq":12,"start_hash":"…","end_hash":"…",
#  "error":null,"seq":null,"position":null,"detail":null}

# 校验一份导出文件（POST 版严格按上传行顺序，可发现乱序）
curl -s -X POST --data-binary @audit.ndjson \
  -H 'Content-Type: application/x-ndjson' \
  http://localhost:8000/api/v1/audit/verify
```

`ok=false` 时按检查顺序返回**第一处**错误，`error` 取值：

| `error` | 含义 | `seq` / `position` |
| --- | --- | --- |
| `duplicate` | 同一序号重复出现 | 重复序号 / 所在行 |
| `out_of_order` | 记录乱序（序号回退或提前） | 越界序号 / 所在行 |
| `missing` | 缺行（序号跳号且期望序号未在后续出现） | 缺失序号 / 发现位置 |
| `field_tampered` | 规范化摘要不匹配，字段被改写 | 被改记录序号 / 所在行 |
| `chain_break` | 链哈希或前指针不匹配 | 断裂处序号 / 所在行 |

- `GET` 全链校验从创世哈希锚定；带时间范围时自动以库中该片段首条的前一条
  记录作为锚点（片段之前缺行不在该范围校验内，请用全链校验获得权威结论）。
- `POST` 校验以序号 1 开头时自动锚定创世哈希；校验任意片段时可通过
  `?anchor_hash=<前一条链哈希>` 显式提供信任锚（通常取自导出响应头）。
- 上传内容仅用于重算校验，**不会入库**；请求体上限 10 MiB。

### 数据模型要点

- 令牌为 32 字节 `secrets.token_urlsafe(32)`，库中仅存其 **SHA-256**；
- 盲槽额外存一列 `submit_token_hash`（提交令牌哈希，独立唯一索引，普通投递为 NULL）；
  空槽阶段 `content` 为 NULL、`kind='pending'`、`status='pending'`，
  提交成功的同一条 UPDATE 写入内容并置 `status='active'`、`submitted_at`；
- 内容存 PostgreSQL `bytea`，同时记录大小与 SHA-256；
- 状态机：`pending（盲槽）→ active → consumed | revoked | expired`，
  其中 `pending/active → revoked | expired`，单向不可逆；
- `(status, expires_at)` 与 `token_hash` 建有索引，支撑并发判定与清扫；
- `audit_logs` 额外保存 `seq / event_digest / prev_hash / entry_hash`
  （链字段），链游标存于单行表 `audit_chain_state`，表结构在启动时
  自动创建并种子化创世游标。

---

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql+asyncpg://dropbox:dropbox@db:5432/dropbox` | 异步数据库 DSN |
| `REDIS_URL` | `redis://redis:6379/0` | Redis 地址 |
| `MAX_CONTENT_BYTES` | `1048576`（1 MiB） | 单条文本/文件大小上限 |
| `TTL_MIN_SECONDS` | `60` | 允许的最短有效期 |
| `TTL_MAX_SECONDS` | `604800`（7 天） | 允许的最长有效期 |
| `REDIS_FLAG_TTL_SECONDS` | `2592000`（30 天） | Redis 终态标记保留时长 |
| `SWEEP_INTERVAL_SECONDS` | `30` | 后台过期清扫间隔 |

本地裸跑（不用 Docker）可复制 `.env.example` 为 `.env` 后自行准备 PG/Redis，
再 `uvicorn app.main:app --reload`。

---

## 本地开发与测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest
```

测试套件使用文件型 SQLite（WAL，多连接真实争抢写锁）+ `fakeredis`，
包含 **50 路并发取件仅有一个赢家**、**取件/撤回并发互斥**、撤回、过期、
文件一次性下载、未知令牌与审计留痕，以及**盲投递全流程**（空槽 pending、
一次性提交、**50 路并发提交仅一人成功其余均为 submitted**、提交/撤回竞争、
撤回阻断提交与取件、pending/active 过期、两类令牌八个方向交叉使用全部
`unknown`、提交成功响应不回传取件能力）、**批量取件**（空/超限/重复整批
422、六类失败原因与位置、文件与盲槽提交令牌处理、到期时刻等于裁决时刻判
过期、失败批次零消费其余仍可领取、重叠批次/单件取件/撤回高并发下每份恰好
成功一次、Redis 不可用回落、逐项入审计链且无明文）和**哈希链入链/导出
NDJSON/五类篡改检测（缺行、乱序、重复、字段改写、链断裂并指出第一处）/
40 路并发写入序号严格连续/链游标竞态失配重试**等用例。另在真实
PostgreSQL 上验证过 100 路并发取件恰好 1 个赢家、30 组取件/撤回互斥。

---

## 安全说明与边界

- 取件令牌是唯一的“能力凭证”：知道令牌即可取件，因此应通过私密渠道发送；
  库中不存明文令牌，泄露数据库无法反推取件地址。
- 盲投递的提交令牌与取件令牌是两种**权限不同**的能力凭证：提交令牌只能竞写
  一次内容，取件令牌才能查询/取件/撤回；二者分别哈希、分别索引，接口层按列
  与状态严格限定，任何交叉使用一律 `unknown`。公开分发提交令牌是安全的（在
  信任“先到先得”语义的前提下），取件令牌必须由收件人保密。
- 默认内容上限 1 MiB，面向“文本或小文件”；大文件投递不在设计范围内。
- 本服务不含发送者身份认证/速率限制，生产部署建议放在鉴权网关之后并自行限流。
- 表结构在启动时自动 `CREATE TABLE`（简单场景不引入迁移工具）。
