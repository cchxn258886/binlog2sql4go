package pkg
type Binlog2sqlStruct struct {
	Ip string;
	Username string;
	Password string;
	Port int;
	StartTime string;
	StopTime string;
	FileName string;
	PosPoint int ;
	DatabaseName string ;
}
type BinlogInfo5 struct {
	LogName string `db:"Log_name"`
	FileSize string `db:"File_size"`
}
type BinlogInfo8 struct {
	LogName string `db:"Log_name"`
	FileSize string `db:"File_size"`
	Encrypted string `db:"Encrypted"`
}
type MysqlVersion struct {
	Version string `db:"version()"`
}
type InformationSchema struct {
	ColumnName string `db:"COLUMN_NAME"`;
}

func NewBinlogVersion(LogName,FileSize,Encrypted string) interface{}{
	if Encrypted == ""{
		return &BinlogInfo5{LogName,FileSize}
	}else {
		return &BinlogInfo8{LogName,FileSize,Encrypted}
	}
}
