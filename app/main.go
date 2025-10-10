package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

type DbHeader struct {
	headerString                 []byte
	dbSizeInBytes                uint16
	fileFormatWriteVer           byte
	fileFormatReadVer            byte
	reservedBytes                byte
	minPayloadFraction           byte
	maxPayloadFraction           byte
	leafPayloadFraction          byte
	fileChangeCounter            []byte
	dbSizeInPages                []byte
	pageNumberFirstFreeTrunkPage []byte
	totalFreeListPages           []byte
	schemaCookie                 []byte
	schemaFormatNumber           []byte
	defaultPageCacheSize         []byte
	largestRootBtreeVacum        []byte
	dbTextEncoding               []byte
	userVersion                  []byte
	incrementalVacuum            []byte
	appId                        []byte
	reservedExpansion            []byte
	versionValidForNumber        []byte
	sqlVersionNumber             []byte
}

func parseDatabaseHeader(data []byte) DbHeader {
	return DbHeader{
		headerString:                 data[0:16],
		dbSizeInBytes:                binary.BigEndian.Uint16(data[16:18]),
		fileFormatWriteVer:           data[18],
		fileFormatReadVer:            data[19],
		reservedBytes:                data[20],
		minPayloadFraction:           data[21],
		maxPayloadFraction:           data[22],
		leafPayloadFraction:          data[23],
		fileChangeCounter:            data[24:28],
		dbSizeInPages:                data[28:32],
		pageNumberFirstFreeTrunkPage: data[32:36],
		totalFreeListPages:           data[36:40],
		schemaCookie:                 data[40:44],
		schemaFormatNumber:           data[44:48],
		defaultPageCacheSize:         data[48:52],
		largestRootBtreeVacum:        data[52:56],
		dbTextEncoding:               data[56:60],
		userVersion:                  data[60:64],
		incrementalVacuum:            data[64:68],
		appId:                        data[68:72],
		reservedExpansion:            data[72:92],
		versionValidForNumber:        data[92:96],
		sqlVersionNumber:             data[96:100],
	}
}

type BtreeHeader struct {
	btreeType                    byte
	startOfFirstFreeblock        []byte
	numberOfCells                uint16
	startOfCellContentArea       uint16
	numberOfFragmenetedFreeBytes byte
	rightMostPointer             []byte
}

func parseBtreeHeader(data []byte) BtreeHeader {
	btreeType := data[0]
	isInterior := btreeType == 0x05 || btreeType == 0x02
	btreeHeader := BtreeHeader{
		btreeType:                    btreeType,
		startOfFirstFreeblock:        data[1:3],
		numberOfCells:                binary.BigEndian.Uint16(data[3:5]),
		startOfCellContentArea:       binary.BigEndian.Uint16(data[5:7]),
		numberOfFragmenetedFreeBytes: data[7],
	}

	if isInterior {
		btreeHeader.rightMostPointer = data[8:12]
	}

	return btreeHeader
}

type Cell struct {
	pageNumberLeftChild       []byte
	rowId                     uint64
	payload                   []byte
	pageNumberOfFirstoverflow []byte
}

func parseCell(data []byte, btreeType byte) (Cell, []byte) {
	var pageNumberLeftChild []byte
	if btreeType == 0x05 || btreeType == 0x02 {
		pageNumberLeftChild = data[:4]
		data = data[4:]
	}

	var numberOfBytesPayload uint64
	if btreeType != 0x05 {
		// b := data[0]
		numberOfBytesPayload, data = parseVarint(data)
	}

	var rowid uint64
	if btreeType == 0x0d || btreeType == 0x05 {
		rowid, data = parseVarint(data)
	}
	var payload []byte
	var pageNumberOfFirstoverflow []byte

	if btreeType != 0x05 {
		payload = data[:numberOfBytesPayload]
		data = data[numberOfBytesPayload:]

		if false {
			// need to find when there is overflow?
			pageNumberOfFirstoverflow = data[:4]
			data = data[4:]
		}
	}

	return Cell{
		pageNumberLeftChild,
		rowid,
		payload,
		pageNumberOfFirstoverflow,
	}, data
}

type DbSchema struct {
	schemaType string
	schemaName string
	tableName  string
	rootPage   uint64
	sqlText    string
}

func parseDataBaseSchemas(data []byte, numberOfCell int) []DbSchema {
	schemas := []DbSchema{}

	for i := 0; i < numberOfCell; i++ {
		dbSchema, newData := parseDataBaseSchema(data)
		schemas = append(schemas, dbSchema)
		data = newData
	}

	return schemas
}

func parseDataBaseSchema(data []byte) (DbSchema, []byte) {
	cell, data := parseCell(data, 0x0d)
	record := parseRecord(cell.payload)

	if len(record) != 5 {
		panic("record shuld contain 5 fields")
	}
	schemaType, ok := record[0].([]byte)
	if !ok {
		panic("schema type should be []byte")
	}

	schemaName, ok := record[1].([]byte)
	if !ok {
		panic("schema name should be []byte")
	}

	tableName, ok := record[2].([]byte)
	if !ok {
		panic("table name should be []byte")
	}
	var rootPage uint64
	switch v := record[3].(type) {
	case uint16:
		rootPage = uint64(v)
	case uint32:
		rootPage = uint64(v)
	case uint64:

	default:
		panic("root page not a number")
	}

	sqlText, ok := record[4].([]byte)
	if !ok {
		panic("sql text should be []byte")
	}

	return DbSchema{
		schemaType: string(schemaType),
		schemaName: string(schemaName),
		tableName:  string(tableName),
		rootPage:   rootPage,
		sqlText:    string(sqlText),
	}, data
}

func parseRecord(data []byte) []any {
	var headerSize uint64
	headerSize, data = parseVarint(data)

	columns := data[:headerSize-1]
	data = data[headerSize-1:]
	res := []uint64{}
	for len(columns) > 0 {
		var data uint64
		data, columns = parseVarint(columns)
		res = append(res, data)
	}

	resData := []any{}

	for _, column := range res {
		switch column {
		case 0:
			resData = append(resData, nil)
		case 1:
			var val uint16
			bigEndianConversion(&val, data[0:1])
			data = data[1:]
			resData = append(resData, val)
		case 2:
			var val uint16
			bigEndianConversion(&val, data[0:2])
			data = data[2:]
			resData = append(resData, val)
		case 3:
			var val uint32
			bigEndianConversion(&val, data[0:3])
			data = data[3:]
			resData = append(resData, val)
		case 4:
			var val uint32
			bigEndianConversion(&val, data[0:4])
			data = data[4:]
			resData = append(resData, val)
		case 5:
			var val uint64
			bigEndianConversion(&val, data[0:6])
			data = data[6:]
			resData = append(resData, val)
		case 6:
			var val uint64
			bigEndianConversion(&val, data[0:8])
			data = data[8:]
			resData = append(resData, val)
		case 7, 8, 9, 10, 11:
			panic("not implemented currently")
		default:
			if (column-13)%2 == 1 {
				panic("blob currently not implemenbted")
			}
			size := int((column - 13) / 2)
			dataaa := data[:size]
			data = data[size:]
			resData = append(resData, dataaa)
		}
	}

	return resData

}

func bigEndianConversion(val any, data []byte) {

	switch v := val.(type) {
	case *uint16:
		for _, b := range data {
			*v = (*v << 8) | uint16(b)
		}

	case *uint32:
		for _, b := range data {
			*v = (*v << 8) | uint32(b)
		}
	case *uint64:
		for _, b := range data {
			*v = (*v << 8) | uint64(b)
		}
	default:
		panic("unsporrted type")
	}
}

type Reader struct {
	databaseFilePath string
	pageSize         uint16
}

func NewReader(databaseFilePath string) Reader {
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	header := make([]byte, 18)

	_, err = databaseFile.Read(header)
	if err != nil {
		log.Fatal(err)
	}

	return Reader{
		pageSize:         binary.BigEndian.Uint16(header[16:18]),
		databaseFilePath: databaseFilePath,
	}

}

func (r Reader) read(pageNumber int) []byte {
	databaseFile, err := os.Open(r.databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	page := make([]byte, r.pageSize)

	_, err = databaseFile.Read(page)
	if err != nil {
		log.Fatal(err)
	}

	return page

}

func (r Reader) readHeader() []byte {
	databaseFile, err := os.Open(r.databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	header := make([]byte, 100)

	_, err = databaseFile.Read(header)
	if err != nil {
		log.Fatal(err)
	}

	return header

}

type SqliteServer struct {
	header DbHeader
	reader Reader
}

func (s SqliteServer) handleDbInfo() {
	page := s.reader.read(0)
	page = page[100:]

	btreeHeader := parseBtreeHeader(page[:12])

	fmt.Printf("database page size: %v\n", s.header.dbSizeInBytes)
	fmt.Printf("number of tables: %v\n", btreeHeader.numberOfCells)
}

func reverse[T any](s []T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func (s SqliteServer) handleTablesInfo() {
	page := s.reader.read(0)
	page = page[100:]

	btreeHeader := parseBtreeHeader(page[:12])

	startContentData := btreeHeader.startOfCellContentArea - 100

	contentData := page[startContentData:]

	schemas := parseDataBaseSchemas(contentData, int(btreeHeader.numberOfCells))

	reverse(schemas)

	for i, schema := range schemas {
		fmt.Printf(schema.tableName)
		if i < len(schemas)-1 {
			fmt.Printf(" ")
		}
	}
}

func (s SqliteServer) handle(command string) {
	switch command {
	case ".dbinfo":
		s.handleDbInfo()
	case ".tables":
		s.handleTablesInfo()
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

// Usage: ./your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	reader := NewReader(databaseFilePath)

	server := SqliteServer{
		header: parseDatabaseHeader(reader.readHeader()),
		reader: reader,
	}

	server.handle(command)

}
