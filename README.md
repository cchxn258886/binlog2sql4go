## 说明
go编写 基于binlog event解析出*反向*执行sql。
binlog2sql 在我本地解析500M binlog 无法使用。
mysql5.* 和8.0.* 均做了适配。
>使用方式：
- 1.可以直接源码运行。
- 2.也可以编译成二进制文件
	`./binlog2sql4go -h 127.0.0.1 -u root
            		-p mysql  -file-name=binlog.0001.log -start-time=2020-01-01 00:00:00 -stop-time=2020-01-01 02:00:00`

### 命令行参数说明
pkg/Stucts.go/Binlog2sqlStruct 可以看到所有的参数。

### 二进制执行
取消掉注释 /* 到 */ 。注释掉没有被注释的代码。
### 源码执行
修改一下参数。有注释
