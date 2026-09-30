# 协调航空物流异常链路协作基础服务

本项目提供跨境数字贸易合作业务共享的服务端基础能力，负责合作机构、业务节点、操作者和结构化参考资料的登记，内置角色权限、请求幂等、SQLite 事务与哈希串联审计。各领域模块可以在这些稳定边界上扩展自己的状态、规则和接口。

## 目录

- src/digital_trade_foundation/：领域模型、SQLite 存储、权限服务、审计链、HTTP 路由和离线验收；
- tests/：基础规则、事务边界、接口路由和端到端验收测试。

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

验收命令会在临时 SQLite 数据库中登记合作机构、操作者、业务节点和参考资料，核对幂等回执与审计链，成功时输出一行 status 为 ok 的 JSON 并以退出码 0 结束。

## HTTP 服务

    PYTHONPATH=src python3 -m digital_trade_foundation.api --database digital_trade.sqlite3 --host 127.0.0.1 --port 8080

健康检查使用 GET /health。写入接口通过 X-Actor-Id 标识操作者，服务重启后 SQLite 中的业务状态和审计历史继续保留。
