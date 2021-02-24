package pkg

import (
	"context"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

var (
	dbConnect  *sqlx.DB
	tempChan   = make(chan string, 10)
	schemaName string
	tableName  string
	returnFlag bool
)

func ParseBinlogd(in *Binlog2sqlStruct) {
	//init databaseConnect

	if in.PosPoint != 0 && in.StartTime != "" {
		in.PosPoint = 0
		//println("pospoint change to 0:",in.PosPoint)
	}
	if in.StopTime == "" {
		in.StopTime = "9999-99-99 99:99:99"
	}
	dbConnect = in.MysqlConnect()
	//cmdString := fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where table_name ='%s'  and TABLE_SCHEMA='%s';","aa",in.DatabaseName)
	//fmt.Println(cmdString)
	cmdString := "show master logs;"
	in.MysqlCMD(cmdString)
	ctx, cancel := context.WithCancel(context.Background())
	go in.reverseEvent(ctx, cancel)
	println("启动goroutine")
	streamer := in.parseBinlog()
	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			println("err", err.Error())
			os.Exit(1)
		}
		r, w, _ := os.Pipe()
		ev.Dump(w)
		w.Close()
		output, _ := ioutil.ReadAll(r)
		strOutput := string(output)
		transferEvent(strOutput)
		//if returnFlag == true {
		//	//cancel()
		//}
	}
}
func (bin *Binlog2sqlStruct) MysqlConnect() *sqlx.DB {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s?parseTime=true&parseTime=true&timeout=5s",
		bin.Username, bin.Password, "tcp", bin.Ip, bin.Port, bin.DatabaseName)
	dbConnect, err := sqlx.Open("mysql", dsn)
	if err != nil {
		println(errors.New("连接错误"))
		os.Exit(1)
	}
	fmt.Println(dsn)
	err = dbConnect.Ping()
	if err != nil {
		panic("connect database time out!!")
	}
	dbConnect.SetMaxOpenConns(20)
	dbConnect.SetMaxIdleConns(5)
	dbConnect.SetConnMaxLifetime(30)
	return dbConnect
}

func (bin *Binlog2sqlStruct) MysqlCMD(cmd string) {
	var logName = map[string]string{}
	var mysqlVersionInfo = make([]MysqlVersion, 0)
	err := dbConnect.Select(&mysqlVersionInfo, "select version();")
	if err != nil {
		println("err", err.Error())
		panic("get mysqlVersion")
	}
	mysqlVersion := mysqlVersionInfo[0]
	if strings.Contains(mysqlVersion.Version, "5.7") {
		var binlogInfo5 []BinlogInfo5 = make([]BinlogInfo5, 0)
		err = dbConnect.Select(&binlogInfo5, cmd)
		if err != nil {
			println("err:", err.Error())
			panic("mysqlCmd panic")
		}
		for k, v := range binlogInfo5 {
			logName[v.LogName] = string(rune(k))
		}
		if _, ok := logName[bin.FileName]; !ok {
			println("file is not exits。pls check.")
			os.Exit(1)
		}
	}

	if strings.Contains(mysqlVersion.Version, "8.0") {
		var binlogInfo8 []BinlogInfo8 = make([]BinlogInfo8, 0)
		err = dbConnect.Select(&binlogInfo8, cmd)
		if err != nil {
			println("err:", err.Error())
			panic("mysqlCmd panic")
		}
		for k, v := range binlogInfo8 {
			logName[v.LogName] = string(rune(k))
		}
		if _, ok := logName[bin.FileName]; !ok {
			println("file is not exits。pls check.")
			os.Exit(1)
		}
	}

}

func (bin *Binlog2sqlStruct) parseBinlog() *replication.BinlogStreamer {
	rand.Seed(time.Now().UnixNano())
	serverid := rand.Int()
	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(serverid),
		Flavor:   "mysql",
		Host:     bin.Ip,
		Port:     uint16(bin.Port),
		User:     bin.Username,
		Password: bin.Password,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	//这个地方 需要修正filename 和pos 点
	//streamer, _ := syncer.StartSync(mysql.Position{Name: "on.000005", Pos: 1193})
	streamer, _ := syncer.StartSync(mysql.Position{Name: bin.FileName, Pos: uint32(bin.PosPoint)})
	if streamer == nil {
		panic("connect database fail pls check needed info！！")
	}
	return streamer
}

func (bin *Binlog2sqlStruct) reverseEvent(ctx context.Context, cancelFunc context.CancelFunc) {
	for {
		select {
		case v, _ := <-tempChan:
			//这里发送的就是一节一节的string
			eventFlag := strings.Split(v, "\n")[0]
			dateInfo := strings.Split(v, "\n")[1]
			//时间判断 start-time stop-time
			//println("vvvvv",v)
			temppos := strings.Index(dateInfo, "Date:")
			//println("temp",dateInfo)
			bin.StartTime = strings.Replace(bin.StartTime, " ", "", -1)
			if bin.StartTime > dateInfo[temppos+5:] {
				//抛弃掉不用的event。
				continue
			}
			bin.StopTime = strings.Replace(bin.StopTime, " ", "", -1)
			//fmt.Println("dateInfo[temppos+5:]",bin.StopTime,dateInfo[temppos+5:])
			if strings.Replace(bin.StopTime, " ", "", -1) < dateInfo[temppos+5:] && bin.GenerateOSFile {
				//抛弃掉不用的event
				//os.Exit(1);
				cancelFunc()
				//close(tempChan)
				//returnFlag = true
				continue
			}
			//println("dateaad",v,dateInfo)
			if strings.Contains(eventFlag, "TableMapEvent") {
				//TODO 表结构解析
				schemaName, tableName = tableMetaDataParse(bin, v)
				//fmt.Printf("schemaName:%s,tableName:%s \n",schemaName,tableName)
			}
			if strings.Contains(eventFlag, "DeleteRows") {
				//TODO delete reverse delete ----> insert
				deleteSqlParse(bin, v, schemaName, tableName, dateInfo)
			} else if strings.Contains(eventFlag, "UpdateRows") {
				//TODO delete reverse
				//println("update",eventFlag)
				updateSqlParse(bin, v, schemaName, tableName, dateInfo)
			} else if strings.Contains(eventFlag, "WriteRows") {
				//TODO delete reverse insert --> delete
				//println("insert",eventFlag)
				insertSqlParse(bin, v, schemaName, tableName, dateInfo)
			}
		}

	}
}

func tableMetaDataParse(bin *Binlog2sqlStruct, inStr string) (string, string) {
	stringSlice := strings.Split(inStr, "\n")
	var schemaName string
	var tableName string
	for _, v := range stringSlice {
		if strings.Contains(v, "Schema") {
			schemaTempPos := strings.Index(v, ":")
			schemaName = v[schemaTempPos+1:]
			//println("databaseName:",schemaName)
		}
		if strings.Contains(v, "Table") && !strings.Contains(v, "===") {
			//println("table",v)
			tempPost := strings.Index(v, ":")
			if "Table" == v[:tempPost] {
				tableName = v[tempPost+1:]
				//println("tableName:",tableName)
			}
		}
	}
	return schemaName, tableName
}
func (bin *Binlog2sqlStruct) getMetaTableInfoFromDatabase(schemaName, tableName string) []InformationSchema {
	//dbConnect := binlog2sql.MysqlConnect()
	//select * from COLUMNS where TABLE_SCHEMA='aa' and table_name='aaa'
	cmdString := fmt.Sprintf("select COLUMN_NAME from COLUMNS where TABLE_SCHEMA='%s' and table_name='%s'\n ", schemaName, tableName)
	information := make([]InformationSchema, 0)
	err := dbConnect.Select(&information, cmdString)
	if err != nil {
		println("err", err.Error())
		os.Exit(1)
	}
	return information
}

func deleteSqlParse(bin *Binlog2sqlStruct, inStr, schemaName, tableName, dateInfo string) {
	var build strings.Builder
	buildBaseString := fmt.Sprintf("insert into %s.%s(", schemaName, tableName)
	build.WriteString(buildBaseString)
	inStr = strings.Replace(inStr, " ", "", -1)
	var valueMap = make(map[int]string, 0)
	sqlEventsValuesPos := strings.Index(inStr, "--")
	inStr = inStr[sqlEventsValuesPos+2:]
	valueSlice := strings.Split(inStr, "\n")
	for _, v := range valueSlice {
		if len(v) == 0 || strings.EqualFold(v, "--") {
			continue
		}
		valueIndexPos := strings.Index(v, ":")
		mapKey, err := strconv.Atoi(v[0:valueIndexPos])
		if err != nil {
			println("panic sql", v, valueIndexPos)
			println("errrrr,", err.Error())
			panic("atoi fail")
		}
		valueMap[mapKey] = v[valueIndexPos+1:]
	}
	//println("schemaName:",schemaName)
	//println("tablename:",tableName)
	information := bin.getMetaTableInfoFromDatabase(schemaName, tableName)
	//insert into schemaname.tablename(id,age,name  ) values()
	for k, v := range information {
		if k == len(valueMap)-1 {
			build.WriteString(v.ColumnName)
			continue
		}
		build.WriteString(v.ColumnName)
		build.WriteString(",")
	}
	build.WriteString(") ")
	build.WriteString("values")
	build.WriteString("(")
	for k := range valueMap {
		if k == len(valueMap)-1 {
			build.WriteString(valueMap[k])
			continue
		}
		build.WriteString(valueMap[k])
		build.WriteString(",")
	}
	build.WriteString(");")
	var debugString = build.String()
	if strings.Contains(debugString, "<nil>") {
		debugString = nilStringDeal(debugString)
	}
	if bin.GenerateOSFile {
		rootDir, _ := os.Getwd()
		_ = path.Join(rootDir, "logs", "binlog2sql4go.log")
		//os.OpenFile(pathString,os.O_APPEND|os.O_CREATE|os.O_WRONLY,0644)
	}
	fmt.Println("build string:", dateInfo, build.String())
}

//TODO update sql reverse
func updateSqlParse(bin *Binlog2sqlStruct, inStr, schemaName, tableName, dateInfo string) {
	var build strings.Builder
	buildBaseString := fmt.Sprintf("update %s.%s set ", schemaName, tableName)
	build.WriteString(buildBaseString)
	inStr = strings.Replace(inStr, " ", "", -1)
	var firstPos = strings.Index(inStr, "--")
	inStr = inStr[firstPos+2:]
	var valueMap = make(map[int]string, 0)
	var oldDataMap = make(map[int]string, 0)
	//sqlEventsValuesPos := strings.Index(inStr,"Values:")
	//inStr = inStr[sqlEventsValuesPos+7:]
	valueSlice := strings.Split(inStr, "--")

	oldDataTempString := valueSlice[0]
	oldDataTempStringSlice := strings.Split(oldDataTempString, "\n")
	for k, v := range oldDataTempStringSlice {
		if len(v) == 0 {
			continue
		}
		mhPos := strings.Index(v, ":")
		oldDataMap[k] = v[mhPos+1:]
	}
	newDataTempString := valueSlice[1]
	newDataTempStringSlice := strings.Split(newDataTempString, "\n")
	for k, v := range newDataTempStringSlice {
		if len(v) == 0 {
			continue
		}
		mhPos := strings.Index(v, ":")
		valueMap[k] = v[mhPos+1:]
	}
	//println("schemaName:",schemaName)
	//println("tablename:",tableName)
	information := bin.getMetaTableInfoFromDatabase(schemaName, tableName)
	//update bb set id=4,age=4 where id=1 and age=1;
	//update schema.tablename set column1=oldvalue,.... where column1 = nowvalue and
	for k, v := range information {
		if k == len(information)-1 {
			build.WriteString(v.ColumnName)
			build.WriteString("=")
			build.WriteString(oldDataMap[k+1])
			continue
		}
		build.WriteString(v.ColumnName)
		build.WriteString("=")
		build.WriteString(oldDataMap[k+1])
		build.WriteString(",")
	}
	build.WriteString(" where ")
	for k, v := range information {
		if k == len(information)-1 {
			build.WriteString(v.ColumnName)
			build.WriteString("=")
			build.WriteString(valueMap[k+1])
			continue
		}
		build.WriteString(v.ColumnName)
		build.WriteString("=")
		build.WriteString(valueMap[k+1])
		build.WriteString(" and ")
	}
	build.WriteString(";")
	var debugString = build.String()
	if strings.Contains(debugString, "<nil>") {
		debugString = nilStringDeal(debugString)
	}
	if bin.GenerateOSFile {

	}
	fmt.Println("build string:", dateInfo, build.String())
}

func insertSqlParse(bin *Binlog2sqlStruct, inStr, schemaName, tableName, dateInfo string) {
	var build strings.Builder
	var andCountNum = 0
	//delete from schemaname.tablename where column=x,column=y;
	buildBaseString := fmt.Sprintf("delete from %s.%s where ", schemaName, tableName)
	build.WriteString(buildBaseString)
	inStr = strings.Replace(inStr, " ", "", -1)
	var valueMap = make(map[int]string, 0)
	sqlEventsValuesPos := strings.Index(inStr, "--")
	inStr = inStr[sqlEventsValuesPos+2:]
	valueSlice := strings.Split(inStr, "\n")
	for _, v := range valueSlice {
		if len(v) == 0 || strings.EqualFold(v, "--") {
			continue
		}
		valueIndexPos := strings.Index(v, ":")
		mapKey, err := strconv.Atoi(v[0:valueIndexPos])
		if err != nil {
			panic("atoi fail")
		}
		//if strings.EqualFold(v[valueIndexPos+1:],"\"\"") || v[valueIndexPos+1:]=="<nil>"{
		if strings.EqualFold(v[valueIndexPos+1:], "\"\"") {
			continue
		}
		valueMap[mapKey] = v[valueIndexPos+1:]
	}
	//println("schemaName:",schemaName)
	//println("tablename:",tableName)
	information := bin.getMetaTableInfoFromDatabase(schemaName, tableName)
	//delete from schemaname.tablename where column=x,column=y;
	for k, v := range information {
		if k == len(information)-1 {
			columnValue, ok := valueMap[k]
			if !ok {
				build.WriteString(";")
			} else {
				build.WriteString(v.ColumnName)
				build.WriteString("=")
				build.WriteString(columnValue)
				build.WriteString(";")
				break
			}
		}
		columnValue, ok := valueMap[k]
		if !ok {
			break
		}
		build.WriteString(v.ColumnName)
		build.WriteString("=")
		build.WriteString(columnValue)
		if andCountNum < len(valueMap)-1 {
			build.WriteString(" and ")
			andCountNum += 1
		}
	}
	var debugString = build.String()
	if strings.Contains(debugString, "<nil>") {
		debugString = nilStringDeal(debugString)
	}
	if bin.GenerateOSFile {

	}
	fmt.Println("build string:", dateInfo, debugString)
}

func transferEvent(inString string) {
	inString = strings.Replace(inString, " ", "", -1)
	//println("inString",inString)
	if strings.Contains(inString, "RowsEventV2") || strings.Contains(inString, "TableMapEvent") {
		tempChan <- inString
	}
	//returnFlag = true;
}

func nilStringDeal(inStr string) string {
	var resultString string
	resultString = strings.Replace(inStr, "<nil>", "null", -1)
	return resultString
}
func writeOSFile(inStr string) {

}
