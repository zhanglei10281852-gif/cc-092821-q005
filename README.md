# 种质资源入库与活力复检服务

本项目是面向种质资源库的 Python 后端服务，用于登记采集或引进材料、建立种子批次、管理低温库位和容器移动、执行发芽活力检测、生成复检日程并处理环境与质量告警。档案、库存、检测和发放审批都保存在本地 SQLite 中，关键写入带版本或幂等键，适合在单个 Linux 应用容器内运行。

## 运行环境

- Python 3.11
- FastAPI 与 Uvicorn
- SQLite 3，由 Python 标准库提供

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `data/germplasm.db`，也可以通过 `GERMPLASM_DATABASE_PATH` 指向其他 `.db`、`.sqlite` 或 `.sqlite3` 文件。

## 初始化与启动

```bash
python -m app.cli init-db
python -m app.cli check-db
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查为 `GET /api/system/health`。首次使用可调用 `POST /api/auth/bootstrap` 创建管理员，再通过 `POST /api/auth/login` 取得 Bearer 会话令牌。种质业务接口统一位于 `/api/germplasm`。

## 测试与构建检查

```bash
python -m pytest
python -m compileall -q app tests
```

下面两条命令分别检查 HTTP 入口和完整的入库演示链路：

```bash
python -m app.cli smoke
python -m app.cli demo
```

## 业务边界

- `app/germplasm/accessions.py` 管理来源、资源档案、护照信息与接收状态。
- `app/germplasm/inventory.py` 管理批次、库位容量、容器摆放、移动、领用和冻结。
- `app/germplasm/viability.py` 管理检测规程、取样、重复计数、活力结果与复检日程。
- `app/germplasm/quality.py` 管理温湿度读数、偏离告警和种质发放审批。
- `app/germplasm/environment.py` 把连续越界读数归并为环境异常事件，结合摆放历史评估受影响批次，人工确认后原子写入质量冻结、提前复检和审计。
- `app/api`、`app/services` 和 `app/repositories` 提供身份、权限、审计、后台作业及维护能力。

## 环境异常事件

低温库制冷短暂停机后设备常乱序补传数小时读数，逐条偏离告警无法回答"哪些容器经历了完整异常窗口、哪些批次要提前复检"。环境事件子系统按以下方式处理：

- **事件归并**：检测作业按库位重扫全部读数，把相邻间隔不超过规则 `max_gap_minutes` 的连续越界读数归并为带开始、峰值（温/湿）和结束的事件。乱序补传后重扫即可确定性地扩展窗口、更新峰值或合并事件；事件指纹（规则、设定值、读数集合）不变时零写入，相同读数和重复评估都不会产生额外事件。
- **规则版本**：事件规则（越界阈值、归并间隔、冻结/复检时长阈值、提前复检到期天数）按版本保存，读数按观测时间套用当时有效的版本；事件永久保留其 `rule_id`，新规则不改写历史事件。
- **影响评估**：用容器摆放历史（`placed_at`/`removed_at`）与事件窗口求区间重叠，列出每个受影响容器/批次及暴露分钟数，按严重级别和暴露时长建议 `hold`（质量冻结）、`early_retest`（提前复检）或 `monitor`（观察）。
- **人工确认**：值班员按事件乐观锁版本逐批次确认，系统建议可被人工覆盖并留痕。冻结（`lot_holds`）、异常复核检测（`viability_tests`）、事件动作与审计记录在单事务内原子写入；动作按影响行幂等，重复提交绝不重复冻结或重复建检测。已确认/驳回事件的窗口冻结，迟到读数只补充挂接，不会让已关闭事故被重复处理。
- **可恢复**：读数入库自动登记 `environment.detect` 后台作业，同库位补传洪峰只保留一个待执行作业；作业带租约，进程崩溃或重启后调用 `POST /api/germplasm/environment/jobs/run-pending`（或 `python -m app.cli environment-jobs`）继续未完成的评估。
- **追溯**：`GET /api/germplasm/environment/events/{id}` 一次返回事件、规则版本、读数序列、受影响容器/批次、处置动作（冻结、检测）及人工覆盖，配合审计记录可从事故一路追到处置结果。

## 一致性约定

SQLite 连接启用外键、WAL、忙等待和即时写事务。资源档案、库位、容器摆放、检测任务和发放申请采用版本号防止旧请求覆盖新状态；入库、移库、取样和传感读数使用业务键去重。活力检测保留采用的规程版本和每个重复的观察计数，完成后可依据作物及风险策略生成下一次复检日期。会话令牌只保存摘要，审计记录不保存明文密码或令牌。
