# cc-etl-lab

CC_Codex 二期（26.11）**数据工程**产品线：**抽取转换加载台**（FastAPI + Vue 3 SPA + SQLite）。

与 `cc-data-lab`（Flask + HTMX 采集控制台）错开：本线面向 **ETL 管道编排 / 抽取落库**，前端为 Vue 单页，暖色仓运风格。

## 领域

- **所属领域**：数据工程  
- **子方向**：抽取-转换-加载 / 数据采集与落库  

## 技术栈

| 层 | 选型 |
|----|------|
| 后端 | Python 3.12、FastAPI、Uvicorn、SQLite |
| 前端 | Vue 3（CDN Composition）、双栏管道台 |
| 风格 | 暖琥珀仓运底、圆角强调、等宽源地址 |
| 测试 | pytest + httpx |

## 基线能力

登录、管道列表、管道详情、执行抽取、执行落库。后续单轮题在此叠加增量水位、清洗口径、并发争用、跨日重算等。

## 双源合并

- **源对照册**：每条管道登记主源 / 次源，两源各自抽取成功后才允许合并。
- **合并键与裁决在服务端**：`merge_key` 为空禁止开跑；同键不同值由服务端按锁定策略裁决，预览只读回显。
- **冲突策略当天锁定**：`whole_row_fail`（整行失败进冲突筐）或 `keep_primary`（保留主源值并旁注次源差异）；锁定后当天改策略返回 409。
- **策略漂移拦截**：发起合并时携带的策略与锁定 / 生效策略不一致，返回 400「合并策略走歪了」。
- **合并争抢**：同一轮合并只成先到那笔，后到请求 409 并记入抢占记录。
- **落库前置**：双源未合并完成不得落库。
- **总览 / 预览**：总览展示主源条数、次源条数、冲突数、策略名；预览可展开冲突行与合并抢占记录。

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/overview` | 管道总览（四指标） |
| GET | `/api/pipelines/{id}/overview` | 单管道总览（含中文指标） |
| GET | `/api/pipelines/{id}/sources` | 源对照册 |
| POST | `/api/pipelines/{id}/sources/{role}/extract` | 单源抽取（角色：主源 / 次源） |
| GET | `/api/pipelines/{id}/merge` | 合并快照（预览 + 冲突筐 + 抢占记录） |
| GET | `/api/pipelines/{id}/merge/preview` | 合并预览 |
| POST/PUT | `/api/pipelines/{id}/merge-config` | 合并键与冲突策略（当天锁） |
| POST | `/api/pipelines/{id}/merge` | 执行合并 |
| POST | `/api/pipelines/{id}/load` | 落库（未合并完成返回 409） |


## 本地

```powershell
cd claude-workspace\cc-etl-lab
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest
python run.py
```

http://127.0.0.1:6000/login · **admin / 123456**

## Docker

```powershell
docker compose up -d --build
```

端口 **6000**。冒烟：`python ..\scripts\_smoke_lab_access.py --dir .`
