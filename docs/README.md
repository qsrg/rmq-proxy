# 文档索引

本文档目录只保留当前仍有参考价值的设计、问题分析和实施计划。已删除围绕旧 `mq-proxy-sdk` 模块和过期 example 结构的阶段性总结，避免误导读者按不存在的类或模块操作。

## 当前实现相关

- [connection-issues-and-solutions.md](connection-issues-and-solutions.md)：客户端连接、心跳、Broker 反向请求和连接清理问题分析。
- [rocketmq-consumer-proxy-implementation-design.md](rocketmq-consumer-proxy-implementation-design.md)：原生 RocketMQ 消费者请求代理的目标架构、关键语义和测试矩阵。
- [specs/2026-04-28-rocketmq-protocol-adaptation.md](specs/2026-04-28-rocketmq-protocol-adaptation.md)：RocketMQ 4.9.8 协议适配清单。
- [superpowers/plans/2026-06-10-async-pull-implementation.md](superpowers/plans/2026-06-10-async-pull-implementation.md)：异步 Pull 链路改造计划。

## 历史设计和后续方向

- [specs/2026-04-28-mq-proxy-design.md](specs/2026-04-28-mq-proxy-design.md)：早期 MQ Proxy 整体设计，包含部分历史规划，不完全等同于当前模块清单。
- [superpowers/specs/2026-06-12-storage-compute-separated-mq-design.md](superpowers/specs/2026-06-12-storage-compute-separated-mq-design.md)：存储计算分离 MQ 的后续架构设想。

## 模块文档

- [../README.md](../README.md)：项目入口、当前模块和快速启动。
- [../mq-proxy-standalone/README.md](../mq-proxy-standalone/README.md)：Standalone 打包、配置、TLS 和部署说明。
- [../mq-proxy-example/README.md](../mq-proxy-example/README.md)：原生 RocketMQ 客户端示例和压测工具。
