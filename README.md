# 协调航空物流异常链路协作基础服务

本项目提供跨境数字贸易合作业务共享的服务端基础能力，负责合作机构、业务节点、操作者和结构化参考资料的登记，内置角色权限、请求幂等、SQLite 事务与哈希串联审计。各领域模块可以在这些稳定边界上扩展自己的状态、规则和接口。

## 航空枢纽异常协同服务（air_exception）

在基础服务边界上实现航空、分拣、仓储与海关共同使用的异常协同链路：

- **连续链路**：主运单 → 扫描包装 → 到港批次 → 拆分/合并 → 仓位租约/航段占位 → 申报版本 → 监管决定 → 查验/补件/放行/改签/交接/退运，全程留痕；
- **部分决定**：扣留、查验、补件、退运等监管决定只作用于显式列出的包装单元，系统自动把被覆盖包裹拆入监管批次，未涉及单元继续流转；
- **数量守恒**：拆分/合并只改变包裹的批次归属，运单件数始终守恒；包装单元在任意时刻只属于一个批次，并发占位不会让同一包装出现在两个位置（有效租约、有效占位均有唯一约束）；
- **幂等扫描**：所有写接口凭 request_id 幂等回放，重复扫描按包裹自然键去重，不会重复扣减库存或容量；
- **迟到决定**：监管决定按自身生效时间只调整尚未完成的路径（取消待办环节、释放占位与租约），已经发生的交接与费用事实保持原样，仅在决定效果中列为 preserved_facts 提示；
- **敏感申报**：申报内容只对海关与管理角色开放，其余角色只能看到版本与状态元数据；
- **恢复后视图**：全部状态落在 SQLite，异常席看板（在途批次、锁定容量、按监管优先排序的待办）在系统重启后依旧准确；
- **延误影响接口**：计算一次延误牵连的包装、航段、费用与承诺，并给出不扩大冻结范围的恢复路线（被扣留单元明确排除在改签建议之外）。

## 目录

- src/digital_trade_foundation/：基础服务的领域模型、SQLite 存储、权限服务、审计链、HTTP 路由和离线验收；
- src/digital_trade_foundation/air_exception/：航空枢纽异常协同服务（链路模型、规则、HTTP 路由、离线验收）；
- tests/：基础规则、事务边界、接口路由、异常协同链路和端到端验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

    PYTHONPATH=src python3 -m unittest discover -s tests -v

## 构建检查

    python3 -m compileall -q src tests

## 离线验收

    PYTHONPATH=src python3 -m digital_trade_foundation.acceptance
    PYTHONPATH=src python3 -m digital_trade_foundation.air_exception.acceptance

基础服务验收登记合作机构、操作者、业务节点和参考资料并核对幂等回执与审计链；异常协同验收演练完整的异常链路（到港、拆分、合并、占位、迟到监管决定、查验、补件、部分放行、交接、延误改签、退运、出港），并模拟系统恢复后核对异常席视图与审计链。成功时各输出一行 status 为 ok 的 JSON 并以退出码 0 结束。

## HTTP 服务

    PYTHONPATH=src python3 -m digital_trade_foundation.api --database digital_trade.sqlite3 --host 127.0.0.1 --port 8080
    PYTHONPATH=src python3 -m digital_trade_foundation.air_exception.api --database air_exception.sqlite3 --host 127.0.0.1 --port 8081

健康检查使用 GET /health。写入接口通过 X-Actor-Id 标识操作者，服务重启后 SQLite 中的业务状态和审计历史继续保留。异常协同服务的接口挂在 /air/ 前缀下（航段、仓位、运单、扫描、到港、拆分、合并、申报、监管决定、租约、占位、改签、交接、退运、异常席看板与延误影响），基础服务接口保持不变。
