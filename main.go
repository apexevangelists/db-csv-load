package main

/* db-csv-load */

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/juju/loggo"
	//profile "github.com/pkg/profile"
	"github.com/howeyc/gopass"
	"github.com/spf13/viper"
	_ "gopkg.in/goracle.v2"
	"trimmer.io/go-csv"
)

// TConfig - parameters in config file
type TConfig struct {
	configFile       string
	debugMode        bool
	delimiter        string
	enclosedBy       string
	headers          bool
	inputFile        string
	table            string
	connectionConfig string
	connectionsDir   string
}

// TConnection - parameters passed by the user
type TConnection struct {
	dbConnectionString string
	username           string
	password           string
	hostname           string
	port               int
	service            string
}

// TGenericRecord - represents a generic CSV record
type GenericRecord struct {
	Record map[string]string `csv:",any"`
}

type GenericCSV []GenericRecord

var config = new(TConfig)
var connection TConnection

var logger = loggo.GetLogger("db-csv-load")

var insertedRowCount int = 0

/********************************************************************************/
func setDebug(debugMode bool) {
	if debugMode == true {
		loggo.ConfigureLoggers("db-csv-load=DEBUG")
		logger.Debugf("Debug log enabled")
	}
}

/********************************************************************************/
func parseFlags() {

	flag.StringVar(&config.configFile, "configFile", "config", "Configuration file for general parameters")
	flag.StringVar(&config.delimiter, "delimiter", ",", "Delimiter between fields")
	flag.StringVar(&config.enclosedBy, "enclosedBy", `"`, "Fields enclosed by")
	flag.StringVar(&config.inputFile, "input", "", "Input Filename")
	flag.StringVar(&config.inputFile, "i", "", "Input Filename")
	flag.StringVar(&config.table, "table", "", "Table to import into")
	flag.StringVar(&config.table, "t", "", "Table to import into")

	flag.BoolVar(&config.debugMode, "debug", false, "Debug mode (default=false)")
	flag.StringVar(&config.connectionConfig, "connection", "", "Configuration file for connection")

	flag.StringVar(&connection.dbConnectionString, "db", "", "Database Connection, e.g. user/password@host:port/sid")

	flag.Parse()

	// At a minimum we either need a dbConnection or a configFile
	if (config.configFile == "") && (connection.dbConnectionString == "") {
		flag.PrintDefaults()
		os.Exit(1)
	}

}

/********************************************************************************/
func getPassword() []byte {
	fmt.Printf("Password: ")
	pass, err := gopass.GetPasswd()
	if err != nil {
		// Handle gopass.ErrInterrupted or getch() read error
	}

	return pass
}

/********************************************************************************/
func getConnectionString(connection TConnection) string {

	if connection.dbConnectionString != "" {
		return connection.dbConnectionString
	}

	var str = fmt.Sprintf("%s/%s@%s:%d/%s", connection.username,
		connection.password,
		connection.hostname,
		connection.port,
		connection.service)

	return str
}

/********************************************************************************/
// To execute, at a minimum we need (connection && (object || sql))
func checkMinFlags() {
	// connection is required
	bHaveConnection := (getConnectionString(connection) != "")

	// check if we have either an object to export or a SQL statement
	bHaveTable := (config.table != "")

	if !bHaveTable {
		fmt.Printf("%s:\n", os.Args[0])
	}

	if !bHaveTable {
		fmt.Printf("  requires a table to be specified\n")
	}

	if !bHaveConnection {
		fmt.Printf("  requires a connection to be specified\n")
	}

	if !bHaveTable || !bHaveConnection {
		flag.PrintDefaults()
		os.Exit(1)
	}
}

/********************************************************************************/
func loadConfig(configFile string) {
	if config.configFile == "" {
		return
	}

	logger.Debugf("reading configFile: %s", configFile)
	viper.SetConfigType("yaml")
	viper.SetConfigName(configFile)
	viper.AddConfigPath(".")

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	// need to set debug mode if it's not already set
	setDebug(viper.GetBool("debugMode"))

	config.connectionsDir = viper.GetString("connectionsDir")
	config.connectionConfig = viper.GetString("connectionConfig")
	config.debugMode = viper.GetBool("debugMode")
	config.configFile = configFile
}

/********************************************************************************/
func loadConnection(connectionFile string) {
	v := viper.New()
	v.SetConfigType("yaml")
	v.SetConfigName(config.connectionConfig)
	v.AddConfigPath(config.connectionsDir)

	err := v.ReadInConfig() // Find and read the config file
	if err != nil {         // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	connection.dbConnectionString = v.GetString("dbConnectionString")
	connection.username = v.GetString("username")
	connection.password = v.GetString("password")
	connection.hostname = v.GetString("hostname")
	connection.port = v.GetInt("port")
	connection.service = v.GetString("service")

}

/********************************************************************************/
func debugConfig() {
	logger.Debugf("config.configFile: %s\n", config.configFile)
	logger.Debugf("config.debugMode: %s\n", strconv.FormatBool(config.debugMode))
	logger.Debugf("config.delimiter: %s\n", config.delimiter)
	logger.Debugf("config.enclosedBy: %s\n", config.enclosedBy)
	logger.Debugf("config.connectionConfig: %s\n", config.connectionConfig)
	logger.Debugf("connection.dbConnectionString: %s\n", connection.dbConnectionString)
}

/********************************************************************************/
func getCreateTableCMD(csv GenericCSV) string {
	lSQL := `create table %TABLE%(%COLUMNS%
		   )`

	headers := getMapKeys(csv[0])

	lCols := ""

	for i, header := range headers {

		// output a delimiter after each field EXCEPT for the last one
		if i < len(headers)-1 {
			lCols = fmt.Sprintf("%s\n%s VARCHAR2(4000),", lCols, header)
		} else {
			lCols = fmt.Sprintf("%s\n%s VARCHAR2(4000)", lCols, header)
		}
	}

	lSQL = strings.Replace(lSQL, "%TABLE%", config.table, -1)
	lSQL = strings.Replace(lSQL, "%COLUMNS%", lCols, -1)

	return lSQL

}

/********************************************************************************/
func getInsertStatement(r GenericRecord) string {
	lCMD := `insert into %TABLE% (%COLS%) VALUES (%VALS%)`
	lCols := ""
	lVals := ""
	h := getMapKeys(r)

	for i, header := range h {

		if i < len(h)-1 {
			lCols = fmt.Sprintf("%s\n%s,", lCols, header)
			lVals = fmt.Sprintf("%s'%s',", lVals, r.Record[header])
		} else {
			lCols = fmt.Sprintf("%s\n%s", lCols, header)
			lVals = fmt.Sprintf("%s'%s'", lVals, r.Record[header])
		}
	}

	lCMD = strings.Replace(lCMD, "%TABLE%", config.table, -1)
	lCMD = strings.Replace(lCMD, "%COLS%", lCols, -1)
	lCMD = strings.Replace(lCMD, "%VALS%", lVals, -1)
	return lCMD
}

/********************************************************************************/
func getMapKeys(m GenericRecord) []string {
	keys := make([]string, 0, len(m.Record))
	for k := range m.Record {
		keys = append(keys, k)
	}

	return keys
}

// ReadFileIntoMap - return a generic map from a file
/********************************************************************************/
func ReadFileIntoMap(path string) (GenericCSV, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var sep = []rune(config.delimiter)
	dec := csv.NewDecoder(f).Separator(sep[0])

	c := make(GenericCSV, 0)
	if err := dec.Decode(&c); err != nil {
		return nil, err
	}
	return c, nil
}

/********************************************************************************/
func process(db *sql.DB, csv GenericCSV) {

	createTable(db, csv)
	totalRowCount := len(csv)

	for _, row := range csv {

		insertStatement := getInsertStatement(row)
		insertRecord(db, insertStatement, totalRowCount)
	}
}

/********************************************************************************/
func createTable(db *sql.DB, csv GenericCSV) {
	createTableCMD := getCreateTableCMD(csv)

	_, err := db.Exec(createTableCMD)

	if err != nil {
		logger.Debugf("Table exists - inserting data")
		//fmt.Sprintf("error in createTable: %s\n", err)
	} else {
		logger.Debugf("Creating Table")
	}
}

/********************************************************************************/
func insertRecord(db *sql.DB, statement string, totalRowCount int) {
	insertedRowCount = insertedRowCount + 1
	logger.Debugf("Importing Record: (%d / %d)", insertedRowCount, totalRowCount)

	_, err := db.Exec(statement)
	if err != nil {
		logger.Debugf("error in insertRecord: %s\n", err)
		logger.Debugf("statement: %s\n", statement)
		fmt.Sprintf("error in insertRecord: %s\n", err)
	}
}

/********************************************************************************/
func main() {
	parseFlags()
	setDebug(config.debugMode)
	loadConfig(config.configFile)
	loadConnection(config.connectionConfig)

	debugConfig()
	checkMinFlags()

	if connection.password == "" {
		connection.password = string(getPassword())
	}

	csv, err := ReadFileIntoMap(config.inputFile)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}

	db, err := sql.Open("goracle", getConnectionString(connection))

	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	process(db, csv)

}
