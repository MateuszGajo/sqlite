package main

import (
	"encoding/binary"
	"fmt"
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

type BtreeHeader struct {
	btreeType                    byte
	startOfFirstFreeblock        []byte
	numberOfCells                uint16
	startOfCellContentArea       uint16
	numberOfFragmenetedFreeBytes byte
	rightMostPointer             []byte
}

type Cell struct {
	pageNumberLeftChild       []byte
	rowId                     uint64
	rawRecord                 []byte
	record                    []any
	pageNumberOfFirstoverflow []byte
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

func parseCell(data []byte, btreeType byte) (Cell, []byte) {
	// "\x81\x02\x01\a\x17\x19\x19\x01\x81_tablebananabanana\x02CREATE TABLE banana (id integer primary key, apple text,banana text,raspberry text,pear text,orange text)"
	// fmt.Println("data", data, len(data))

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

	// 00000010 10000001

	var rowid uint64
	if btreeType == 0x0d || btreeType == 0x05 {
		rowid, data = parseVarint(data)
	}
	var payload []byte
	var pageNumberOfFirstoverflow []byte
	var record []any

	if btreeType != 0x05 {
		payload = data[:numberOfBytesPayload]
		data = data[numberOfBytesPayload:]

		if false {
			// need to find when there is overflow?
			pageNumberOfFirstoverflow = data[:4]
			data = data[4:]
		}
		record = parseRecord(payload)
	}

	return Cell{
		pageNumberLeftChild:       pageNumberLeftChild,
		rowId:                     rowid,
		rawRecord:                 payload,
		record:                    record,
		pageNumberOfFirstoverflow: pageNumberOfFirstoverflow,
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
		case 7, 10, 11:
			panic(fmt.Sprintf("not implemented currently %v", column))
		case 8:
			val := false
			resData = append(resData, val)
		case 9:
			val := true
			resData = append(resData, val)
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

type Page struct {
	btreeHeader BtreeHeader
	cells       []Cell
}

func parsePage(page []byte, pageNumber int) Page {
	btreeHeaderData := page[:12]
	if pageNumber == 0 {
		btreeHeaderData = page[100:112]
	}

	btreeHeader := parseBtreeHeader(btreeHeaderData)

	contentData := page[btreeHeader.startOfCellContentArea:]

	cells := []Cell{}
	var cell Cell
	for i := 0; i < int(btreeHeader.numberOfCells); i++ {
		cell, contentData = parseCell(contentData, btreeHeader.btreeType)

		cells = append(cells, cell)
	}
	// reverse(cells)

	return Page{
		btreeHeader: btreeHeader,
		cells:       cells,
	}

}

type DbSchema struct {
	schemaType string
	schemaName string
	tableName  string
	rootPage   uint64
	sqlText    string
}

func parseDataBaseSchemas(page Page) []DbSchema {
	schemas := []DbSchema{}

	for i := 0; i < len(page.cells); i++ {
		dbSchema := parseDataBaseSchema(page.cells[i].record)
		schemas = append(schemas, dbSchema)
	}

	return schemas
}

func parseDataBaseSchema(record []any) DbSchema {

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
	}
}
