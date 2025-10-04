package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

type DbHeader struct {
	headerString                 []byte
	dbSizeInBytes                []byte
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
		dbSizeInBytes:                data[16:18],
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
	numberOfCells                []byte
	startOfCellContentArea       []byte
	numberOfFragmenetedFreeBytes byte
	rightMostPointer             []byte
}

func parseBtreeHeader(data []byte) BtreeHeader {
	btreeType := data[0]
	isInterior := btreeType == 0x05 || btreeType == 0x02
	btreeHeader := BtreeHeader{
		btreeType:                    btreeType,
		startOfFirstFreeblock:        data[1:3],
		numberOfCells:                data[3:5],
		startOfCellContentArea:       data[5:7],
		numberOfFragmenetedFreeBytes: data[7],
	}

	if isInterior {
		btreeHeader.rightMostPointer = data[8:12]
	}

	return btreeHeader
}

func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		header := make([]byte, 100)

		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}
		headerParsed := parseDatabaseHeader(header[:100])

		pageSize := binary.BigEndian.Uint16(headerParsed.dbSizeInBytes)

		page := make([]byte, pageSize-100)

		_, err = databaseFile.Read(page)
		if err != nil {
			log.Fatal(err)
		}
		btreeHeader := parseBtreeHeader(page[:12])

		startContentData := binary.BigEndian.Uint16(btreeHeader.startOfCellContentArea)

		contentData := page[startContentData:]

		fmt.Println("lets set content data? %+v", contentData)

		numberOfTables := binary.BigEndian.Uint16(btreeHeader.numberOfCells)
		fmt.Fprintln(os.Stderr, "Logs from your program will appear here!")

		fmt.Printf("database page size: %v\n", pageSize)
		fmt.Printf("number of tables: %v\n", numberOfTables)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
