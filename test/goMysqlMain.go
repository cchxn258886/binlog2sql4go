package main

import (
	"binlog2sql4go/pkg"
	"context"
	"fmt"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

var (
	tempChan   chan string
	schemaName string
	tableName  string
)

func maint() {
	tempChan = make(chan string, 10)
	//go test1();
	//cfg := replication.BinlogSyncerConfig {
	//	ServerID: 100,
	//	Flavor:   "mysql",
	//	Host:     "172.18.12.50",
	//	Port:     3306,
	//	User:     "jira",
	//	Password: "jira",
	//}
	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     "172.17.1.112",
		Port:     3306,
		User:     "test",
		Password: "test",
	}
	syncer := replication.NewBinlogSyncer(cfg)

	// Start sync with specified binlog file and position
	//streamer, _ := syncer.StartSync(mysql.Position{Name: "binlog.000023", Pos: 993919094})
	streamer, _ := syncer.StartSync(mysql.Position{Name: "on.000005", Pos: 1193})

	println(streamer) //nil point

	// or you can start a gtid replication like
	// streamer, _ := syncer.StartSyncGTID(gtidSet)
	// the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
	// the mariadb GTID set likes this "0-1-100"
	go reverseEvent()
	println("启动goroutine")
	for {
		ev, err := streamer.GetEvent(context.Background())
		if err != nil {
			println("err", err)
		}
		// Dump event
		//println("rawData:",ev.RawData)
		//file,err :=os.OpenFile("/Users/chen/Desktop/myproject/log.log",os.O_APPEND|os.O_CREATE|os.O_WRONLY,644)
		//if err != nil{
		//	panic("can not write event to local file.pls check ")
		//}
		r, w, _ := os.Pipe()
		ev.Dump(w)
		w.Close()
		//rd := bufio.NewReader(r);
		//line, err := rd.ReadString('\n')
		//if err != nil || err == io.EOF {
		//	println("err", err)
		//	break
		//}
		//println("line:", line)
		output, _ := ioutil.ReadAll(r)
		strOutput := string(output)
		transferEvent(strOutput)
		//解析出表结构 需要先解析出表结构
		/*		if (strings.Contains(strOutput,"TableMapEvent")){

				}*/
		//具体信息
		/*		if (strings.Contains(strOutput,"RowsEventV2")){
				firstDobuleLinePos := strings.Index(strOutput,"--")
				secondDobuleLinePos := strings.LastIndex(strOutput,"--")
				firstValues := strOutput[firstDobuleLinePos+2:secondDobuleLinePos]
				secondValues := strOutput[secondDobuleLinePos+2:]
				println(firstValues)
				println(secondValues)
			}*/
	}
	// or we can use a timeout context
	//for {
	//	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	//	ev, err := s.GetEvent(ctx)
	//	cancel()
	//
	//	if err == context.DeadlineExceeded {
	//		// meet timeout
	//		continue
	//	}
	//
	//	ev.Dump(os.Stdout)
	//}
}

func transferEvent(inString string) {
	inString = strings.Replace(inString, " ", "", -1)
	//println("inString",inString)
	if strings.Contains(inString, "RowsEventV2") || strings.Contains(inString, "TableMapEvent") {
		tempChan <- inString
	}
}

func reverseEvent() {
	//var getChanStringSlice = make([]string,10);
	for {
		select {
		case v, _ := <-tempChan:
			eventFlag := strings.Split(v, "\n")[0]
			if strings.Contains(eventFlag, "TableMapEvent") {
				//TODO 表结构解析
				schemaName, tableName = tableMetaDataParse(v)
				//fmt.Printf("schemaName:%s,tableName:%s \n",schemaName,tableName)
			}
			if strings.Contains(eventFlag, "DeleteRows") {
				//TODO delete reverse delete ----> insert
				deleteSqlParse(v, schemaName, tableName)
			} else if strings.Contains(eventFlag, "UpdateRows") {
				//TODO delete reverse
				//println("update",eventFlag)
				updateSqlParse(v, schemaName, tableName)
			} else if strings.Contains(eventFlag, "WriteRows") {
				//TODO delete reverse insert --> delete
				//println("insert",eventFlag)
				insertSqlParse(v, schemaName, tableName)
			}
		default:
			{
			}
		}
	}
}
func tableMetaDataParse(inStr string) (string, string) {
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

func deleteSqlParse(inStr, schemaName, tableName string) {
	var build strings.Builder
	buildBaseString := fmt.Sprintf("insert into %s.%s(", schemaName, tableName)
	build.WriteString(buildBaseString)
	inStr = strings.Replace(inStr, " ", "", -1)
	var valueMap = make(map[int]string, 0)
	sqlEventsValuesPos := strings.Index(inStr, "--")
	inStr = inStr[sqlEventsValuesPos+2:]
	valueSlice := strings.Split(inStr, "\n")
	for _, v := range valueSlice {
		if len(v) == 0 {
			continue
		}
		valueIndexPos := strings.Index(v, ":")
		mapKey, err := strconv.Atoi(v[0:valueIndexPos])
		if err != nil {
			panic("atoi fail")
		}
		valueMap[mapKey] = v[valueIndexPos+1:]
	}
	println("schemaName:", schemaName)
	println("tablename:", tableName)
	information := getMetaTableInfoFromDatabase(schemaName, tableName)
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
	fmt.Println("build string:", build.String())
}

//TODO update sql reverse
func updateSqlParse(inStr, schemaName, tableName string) {
	var build strings.Builder
	buildBaseString := fmt.Sprintf("update %s.%s set ", schemaName, tableName)
	build.WriteString(buildBaseString)
	inStr = strings.Replace(inStr, " ", "", -1)
	var firstPos = strings.Index(inStr, "--")
	inStr = inStr[firstPos+2:]
	//instr = "asdfaf -- qweadsfa"
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
	println("schemaName:", schemaName)
	println("tablename:", tableName)
	information := getMetaTableInfoFromDatabase(schemaName, tableName)
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
		build.WriteString(",")
	}

	build.WriteString(";")
	fmt.Println("build string:", build.String())
}

func insertSqlParse(inStr, schemaName, tableName string) {
	var build strings.Builder
	//delete from schemaname.tablename where column=x,column=y;
	buildBaseString := fmt.Sprintf("delete from %s.%s where ", schemaName, tableName)
	build.WriteString(buildBaseString)
	inStr = strings.Replace(inStr, " ", "", -1)
	var valueMap = make(map[int]string, 0)
	sqlEventsValuesPos := strings.Index(inStr, "--")
	inStr = inStr[sqlEventsValuesPos+2:]
	valueSlice := strings.Split(inStr, "\n")
	for _, v := range valueSlice {
		if len(v) == 0 {
			continue
		}
		valueIndexPos := strings.Index(v, ":")
		mapKey, err := strconv.Atoi(v[0:valueIndexPos])
		if err != nil {
			panic("atoi fail")
		}
		valueMap[mapKey] = v[valueIndexPos+1:]
	}
	println("schemaName:", schemaName)
	println("tablename:", tableName)
	information := getMetaTableInfoFromDatabase(schemaName, tableName)
	//delete from schemaname.tablename where column=x,column=y;
	for k, v := range information {
		if k == len(information)-1 {
			columnValue := valueMap[k]
			build.WriteString(v.ColumnName)
			build.WriteString("=")
			build.WriteString(columnValue)
			build.WriteString(";")
			continue
		}
		columnValue := valueMap[k]
		build.WriteString(v.ColumnName)
		build.WriteString("=")
		build.WriteString(columnValue)
		build.WriteString(",")
	}
	fmt.Println("build string:", build.String())
}

func getMetaTableInfoFromDatabase(schemaName, tableName string) []pkg.InformationSchema {
	var binlog2sql = pkg.Binlog2sqlStruct{
		"172.17.1.112", "test", "test",
		3306, "", "", "", 0, "information_schema", false}
	dbConnect := binlog2sql.MysqlConnect()
	//select * from COLUMNS where TABLE_SCHEMA='aa' and table_name='aaa'
	cmdString := fmt.Sprintf("select COLUMN_NAME from COLUMNS where TABLE_SCHEMA='%s' and table_name='%s'\n ", schemaName, tableName)
	information := make([]pkg.InformationSchema, 0)
	err := dbConnect.Select(&information, cmdString)
	if err != nil {
		println("err", err.Error())
		os.Exit(1)
	}
	return information
	//insert into schemaname.tablename(id,age,name  ) values()
	//for k,v := range information{
	//	println(len(inMap))
	//	if k == len(inMap)-1{
	//		build.WriteString(inMap[k])
	//		continue
	//	}
	//	build.WriteString(v.ColumnName)
	//	build.WriteString(",")
	//}
	//build.WriteString(")")
	//build.WriteString("values")
	//build.WriteString("(")
	//for k := range inMap{
	//	if k == len(inMap)-1{
	//		build.WriteString(inMap[k])
	//		continue
	//	}
	//	build.WriteString(inMap[k])
	//	build.WriteString(",")
	//}
	//build.WriteString(")")
	//fmt.Println("build string:",build.String())
}
