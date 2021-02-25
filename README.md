## 说明
go编写 基于binlog event解析出执行sql。使用channel解耦。
binlog2sql 在我本地解析500M binlog 无法使用。
联系 244776516@qq.com
使用方式：
- 1.可以直接源码运行。
- 2.也可以编译成二进制文件
	`./binlog2sql4go -h 127.0.0.1 -u root
            		-p mysql  -file-name=binlog.0001.log -start-time=2020-01-01 00:00:00 -stop-time=2020-01-01 02:00:00`
### 命令行参数说明
pkg/Stucts.go/Binlog2sqlStruct 可以看到所有的参数。