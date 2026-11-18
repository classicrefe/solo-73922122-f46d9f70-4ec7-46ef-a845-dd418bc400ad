# HLS 直播连续录音采集

电台语料采集工具：把直播 HLS 流录成连续录音包，交给离线转写。
FastAPI + SQLite 采集网页，输入媒体清单地址即开始追随录制；停止后把应收片段
全部落盘，产出可离线播放的 ZIP（本地 M3U8 + 原始 TS 片段，不引用远端）。

## 运行

```bash
docker compose up --build
# 网页:  http://localhost:8000
# 直播源: http://localhost:8080
```

网页中可开始录制、停止、查看进度、下载 ZIP。直播源地址示例
（网页内也有提示）：

| 用途 | 地址 |
|---|---|
| 普通滑窗直播 | `http://livesource:8080/live.m3u8` |
| 慢下载（每段延迟 1.5s） | `http://livesource:8080/live.m3u8?delay_ms=1500` |
| 长片段（32 MiB，限速 2 MiB/s） | `http://livesource:8080/live.m3u8?size_mb=32&rate_kbps=2048` |
| 传输中断（只发 500 KB 即断开） | `http://livesource:8080/live.m3u8?size_mb=8&truncate_at=500000` |
| 缺段（指定序号返回 404） | `http://livesource:8080/live.m3u8?missing=12,13` |
| 窗口跳号（越过未发现序号） | `http://livesource:8080/live.m3u8?jump_at=10&jump_by=8` |
| VOD（带 ENDLIST） | `http://livesource:8080/vod.m3u8` |
| 非法样例（master/加密/不连续） | `/master.m3u8` `/enc.m3u8` `/disc.m3u8` |

## 行为规则（与需求对应）

- **接受的清单**：仅单层媒体清单（拒绝 master）、未加密（拒绝非 NONE 的
  `EXT-X-KEY`）、无不连续标记、MPEG-TS（片段须为 `.ts`，拒绝 `EXT-X-MAP`），
  片段地址在清单内与整个录制任务内均不得复用。
- **追随滑窗**：从首份清单首段（其 `EXT-X-MEDIA-SEQUENCE`）开始，按媒体序号
  判定连续性；新清单起点越过 `last_seq+1` 即判失败并列出缺段序号。
- **停止**：受理时把边界固定为最后已接收清单的尾段并立即持久化，此后不再
  延长范围（拉取途中的清单若晚于停止到达会被丢弃）；重启后边界不变。
- **完成**：停止边界内全部片段保存后才生成并开放 ZIP；ZIP 内含本地
  `playlist.m3u8`（`EXT-X-ENDLIST`，仅引用包内文件）与原始片段。打包前再次
  核对边界内每段在磁盘上的实际字节数，并只允许 `finalizing` 进入 `done`。
- **失败**：应收片段 404、窗口跳号、源违反约束、持续拉取/下载失败均判失败，
  失败任务列出缺段且不提供下载，不会跳过后报成功。停止时边界内存在不可取得
  的序号（如窗口越过从未发现、没有片段地址）同样判失败并列出缺段。
- **流式下载**：片段以分块方式边收边写临时 `.part` 文件（固定 256 KiB 缓冲），
  内存占用不随片段大小增长；校验 `Content-Length`，传输中断/截断视为失败并
  重试。仅当整段收完并 `fsync` 后原子 rename 为最终文件名，DB 才登记完成；
  因此中途崩溃或断连不会留下被当作"完成"的半截文件。
- **持久化**：片段与 ZIP 存持久卷（`/data`，compose 命名卷），SQLite 只存
  进度（任务状态、边界、每段状态）。落盘用临时文件 + 原子 rename；
  重启时对账磁盘与 DB——已落盘未登记的片段收养为完成，登记完成但文件丢失或
  字节数不符的重新下载，残留 `.part` 与未完成 ZIP 清理；已保存片段即使滑出
  清单也保留并入包。重启后仍缺失且源里还能取得的序号会补收；确不可取得的
  （无片段地址）保持缺段并阻止打包/下载。
- **并发**：每路录制为独立的异步轮询 + 下载任务（asyncio/httpx），支持两路
  （及以上）同时录制；慢下载/长片段不阻塞另一路、停止受理与查询。

## API

- `POST /api/jobs` `{url}` 开始录制
- `POST /api/jobs/{id}/stop` 停止（固定边界）
- `GET /api/jobs` / `GET /api/jobs/{id}` 进度（状态、窗口、缺段、错误）
- `GET /api/jobs/{id}/download` 下载 ZIP（未完成返回 409）

任务状态：`recording` →（停止）→ `finalizing` → `done` / `failed`。

## 目录

```
app/          采集网页与录制引擎（FastAPI）
  src/playlist.py   严格清单解析与校验
  src/recorder.py   轮询、下载、定界、打包、重启恢复
  src/db.py         SQLite 进度存储
livesource/   本地伪直播源（滑窗/延迟/缺段/跳号/非法清单样例）
docker-compose.yml
```

## 测试

本仓库代码已在本地以与容器相同的命令（`uvicorn src.main:app`）完成端到端
自测：基本录制-停止-打包、ZIP 内容校验（本地清单/连续序号/无远端引用/
片段字节一致/ffmpeg 可解码）、两路并发与慢源不阻塞、缺段 404 失败、窗口
跳号失败、非法清单拒绝、ENDLIST 自动定界、崩溃（`os._exit`）后重启续录、
停止边界重启不变、落盘与登记之间崩溃的收养恢复；新增长片段相关：32 MiB
片段下载期间 RSS 峰值仅小幅高于基线（旧实现每段 +约 96 MiB，新实现约
+2 MiB，且 256 MiB 片段仍约 +2 MiB）、长片段下载中停止/查询 <1s、传输
截断判失败不留完成段、长传输中途崩溃后 `.part` 不被当完成段、重启清理并
补收缺失段、不可取得缺段时禁止下载。`tests/test_e2e.py` 46 项、
`tests/test_extra.py` 3 项，共 49 项全部通过。

本地运行测试（无 Docker 时）：

```bash
# 1) 启动伪直播源（需先有可播放的 TS 种子片段，可用 ffmpeg 生成）
SEED_TS=/path/to/seed.ts python -m uvicorn src.main:app --port 8080   # cwd=livesource
# 2) 运行测试
SRC=http://127.0.0.1:8080 SEED_TS=/path/to/seed.ts APP_DIR=./app python tests/test_e2e.py
SRC=http://127.0.0.1:8080 SEED_TS=/path/to/seed.ts APP_DIR=./app python tests/test_extra.py
```

测试钩子（仅 `RECORDER_TEST_HOOKS=1` 时挂载，compose 不启用）：
`POST /api/_test/crash` 立即崩溃；`POST /api/_test/arm_after_write/{job}/{seq}`
在指定片段落盘后、登记前崩溃。

## 已知限制

- 单进程部署（任务状态在内存中，重启时从 SQLite + 磁盘恢复）；不支持多
  worker 水平扩展。
- 片段只做传输层完整性校验（Content-Length 与实际字节数一致；无
  Content-Length 时要求总长为 TS 包长 188 的整数倍），不做码流级校验。
- 非 404 的持续性下载错误重试 5 次后判失败；清单持续拉取失败 15 次后判失败。
- 片段确已滑出源窗口且当时未收到地址的序号无法补收，任务会列出缺段并保持
  不可下载。
- 本地直播源各片段内容相同（种子片段循环），时间戳每段重置；ffmpeg 解码
  校验通过，拼接播放时个别播放器可能提示时间戳不连续。
- 长片段按原样存入 ZIP（`ZIP_STORED`），ZIP 体积约等于片段总大小；按字节
  流式打包，内存不随总大小增长。
