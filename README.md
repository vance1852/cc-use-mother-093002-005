# 协调航空物流异常链路协作服务

本项目提供跨境数字贸易合作业务共享的服务端基础能力：负责合作机构、业务节点、操作者和结构化参考资料的登记，内置角色权限、请求幂等、SQLite 事务与哈希串联审计。

在此之上，`cargo` 模块实现**航空、分拣、仓储与海关共同使用的异常协同服务**，把主运单、子批次、包装单元、申报版本、仓位租约、航段容量、监管决定和责任交接串成连续链路，解决"整票冻结导致无争议货物也无法继续"的问题。

## 目录

- src/digital_trade_foundation/：
  - service/storage/audit/api/acceptance：基础登记、权限、幂等、审计、HTTP 路由与离线验收；
  - cargo.py：航空物流异常协同领域服务（事实只增、状态实时推导）；
  - cargo_acceptance.py：异常场景离线端到端验收；
  - clock.py：系统时钟、固定时钟与可推进时钟（验收/测试用）。
- tests/：基础规则、航空域不变量、接口路由和端到端验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 航空异常协同服务的关键规则

- **连续链路**：主运单 → 子批次（拆分层级）→ 包装单元；申报有版本，监管决定有生效时间与序号，仓位租约、航段占位、责任交接、费用均为只增事实。
- **部分决定**：监管决定只作用于显式 `package_ids` 覆盖的包装，并沿拆分血缘传播到现存叶包装；不向兄弟分支扩散。合并件的各来源监管状态分别追踪，任一来源未放行整件不得离场。
- **真实业务顺序**：到港 → 拆分/合并 → 入库 → 申报 → 查验/补件/扣留/放行 → 配舱确认 → 提货交接 → 离场；退运必须有生效的退运决定。
- **迟到决定**：决定按自身 `effective_at` 生效，晚到的决定只改变尚未完成（未离场、未交接）的路径；已经发生的交接与费用事实永不回写。
- **数量守恒**：拆分要求子件数量恰好等于来源；合并不变量按血缘无向连通分量校验（初始总量 = 现存叶节点总量）。
- **重复不重复扣减**：扫描 `scan_token` 与请求 `request_id` 双重幂等；重复到港/查验/上架/发运不会再次登记事实或扣减容量。
- **位置唯一**：包装主键唯一约束保证一个包装同时只有一个库位；部分唯一索引保证同时只有一个生效航段占位（held/confirmed），改签先作废旧占位（rebooked 事实保留）再占新航段；容量在写事务中校验。
- **敏感申报**：标记 `sensitive` 的申报内容仅向具备该票海关职责的操作者开放，其他人只能看到哈希。
- **异常席视图**：`shipment_status` 实时推导在途状态、监管状态、锁定容量、保管链与待办次序；`delay_impact` 一次算出延误牵连的包装、航段、费用归属、承诺风险，并给出**只覆盖被牵连包装**的恢复路线（rebook/补件/退运），不扩大冻结范围。所有状态由事实推导，服务重启后同样准确。

## 测试

    PYTHONPATH=src python3 -m unittest discover -s tests -v

## 构建检查

    python3 -m compileall -q src tests

## 离线验收

基础服务：

    PYTHONPATH=src python3 -m digital_trade_foundation.acceptance

航空异常协同（拆分、部分扣留/放行、补件、迟到查验决定、改签、交接费用不可变、恢复路线与重启一致性）：

    PYTHONPATH=src python3 -m digital_trade_foundation.cargo_acceptance

成功时输出一行 status 为 ok 的 JSON 并以退出码 0 结束。

## HTTP 服务

    PYTHONPATH=src python3 -m digital_trade_foundation.api --database digital_trade.sqlite3 --host 127.0.0.1 --port 8080

健康检查使用 GET /health。写入接口通过 X-Actor-Id 标识操作者，所有写接口都要求 `request_id` 实现幂等。

航空域主要接口（均在 `/cargo` 前缀下，POST 为写操作）：

| 路径 | 说明 |
| --- | --- |
| POST /cargo/shipments | 建立主运单 |
| POST /cargo/duties | 授予承运/分拣/仓储/海关/异常席职责 |
| POST /cargo/packages | 登记包装单元 |
| POST /cargo/packages/split · /merge | 拆分（守恒校验）、合并 |
| POST /cargo/arrivals · /inspections | 到港、查验（支持 scan_token 去重） |
| POST /cargo/declarations · /supplements | 申报版本、补件（可标 sensitive） |
| POST /cargo/decisions · /cargo/inspections/clear | 监管决定（可带早于当前的 effective_at 表示迟到）、核销查验 |
| POST /cargo/locations · /cargo/leases · /cargo/placements · /cargo/pickups | 仓位、租约、上架、提货 |
| POST /cargo/segments · /cargo/allocations · /cargo/allocations/confirm · /cargo/allocations/release · /cargo/segments/close | 航段、占位、确认、释放、截关 |
| POST /cargo/rebookings · /cargo/departures | 改签（旧占位作废+新占位+改签费）、离场 |
| POST /cargo/handoffs · /cargo/returns · /cargo/fees | 责任交接、退运、费用登记 |
| GET /cargo/shipments/{id}/status | 全链路在途视图与待办次序 |
| GET /cargo/shipments/{id}/delay-impact | 延误牵连面与不扩大冻结的恢复路线 |
| GET /cargo/declarations/{id} | 单个申报（敏感内容按职责脱敏） |
