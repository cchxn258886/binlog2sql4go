package main

import (
	"binlog2sql4go/pkg"
)

func main() {
	//var binlog2sql = new pkg.Binlog2sql{1,}
	/*	var ip string;
			var port int;
			var username string;
			var password string;
			var startTime string;
			var stopTime string;
			var fileName string;
			var help bool;
			var databaseName string;
			var posPoint int;
			flag.BoolVar(&help,"help",false,"help")
			flag.StringVar(&ip,"h","127.0.0.1","ip")
			flag.IntVar(&port,"P",3306,"数据库端口")
			flag.StringVar(&username,"u","","用户名")
			flag.StringVar(&password,"p","","密码")
			flag.StringVar(&startTime,"start-time","","开始时间")
			flag.StringVar(&stopTime,"stop-time","","结束时间")
			flag.StringVar(&fileName,"file-name","","文件名 绝对路径")
			flag.StringVar(&databaseName,"database","information_schema","数据库schema")
			flag.IntVar(&posPoint,"pos",0,"posPoint")
			flag.Parse()
			if (help){
				flag.PrintDefaults()
				var helpString = ` 示例： ./binlog2sql4go -h 127.0.0.1 -u root
		-p mysql  -file-name=binlog.0001.log -pos=0`
				var helpString2 = `示例： ./binlog2sql4go -h 127.0.0.1 -u root
		-p mysql  -file-name=binlog.0001.log -start-time=2020-01-01 00:00:00 -stop-time=2020-01-01 02:00:00`
				println(helpString)
				println(helpString2)
				os.Exit(0)
			}
			if (username == "" || password == "" ){
				panic("传入参数有误,请传入username.password")
			}
			if (posPoint == 0 && startTime=="" && stopTime == ""){
				panic("pospoint和XXtime至少需要填一项")
			}

			var binlog2sql =  pkg.Binlog2sqlStruct{
				ip,username,password,port,startTime,stopTime,fileName,posPoint,databaseName}*/
	//mysql.Position{Name: "on.000005", Pos: 1193})
	var binlog2sql = pkg.Binlog2sqlStruct{
		"172.17.1.112", "test", "test",
		3306, "2021-02-23 14:42:00", "2021-02-2314:47:43", "on.000005", 0,
		"information_schema", true}
	//var binlog2sql = pkg.Binlog2sqlStruct{
	//	"172.18.12.50", "jira", "jira",
	//	3306, "2021-02-21 00:00:00", "2021-02-21 08:00:00", "binlog.000024", 163030192, "information_schema"}
	pkg.ParseBinlogd(&binlog2sql)
}
