## rocketmq源码位置

- rocketmq源码位置：/Users/wcf/java-project/rocketmq-4.9.8
- 不确定的问题需要提问或探讨

## 本机启动rocketmq服务

- 启动namesrv: cd /Users/wcf/apps/rocketmq-all-4.9.8-bin-release;sh bin/mqnamesrv
- 启动broker: cd /Users/wcf/apps/rocketmq-all-4.9.8-bin-release;sh bin/mqbroker -c conf/broker.conf
- 注意：本机Java 17，runbroker.sh已做兼容修改（去掉UseBiasedLocking、降内存至1g）；重新安装RocketMQ后需再次修改
- 如果broker启动失败，先删除 `/Users/wcf/data/rocketmq/store/` 下的 `commitlog/ consumequeue/ index/ abort checkpoint lock` 再重试

## 项目依赖

- java8

## 编码规范

- 永远遵守 Karpathy 规范。任务执行前后，使用 Karpathy 自查
