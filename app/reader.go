package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

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

func (r Reader) seqRead(rootPage int) []Page {
	page := r.read(rootPage)
	pageParsed := parsePage(page, rootPage)
	return r.readRecusrive(pageParsed)
}

func (r Reader) readRecusrive(pageParsed Page) []Page {
	if pageParsed.btreeHeader.btreeType != 0x05 {
		return []Page{pageParsed}
	}

	pages := []Page{}
	for _, cell := range pageParsed.cells {
		pageNumber := binary.BigEndian.Uint32(cell.pageNumberLeftChild)
		page := r.read(int(pageNumber))
		cellPageParsed := parsePage(page, int(pageNumber))

		pages = append(pages, r.readRecusrive(cellPageParsed)...)
	}

	return pages
}

func (r Reader) read(pageNumber int) []byte {
	databaseFile, err := os.Open(r.databaseFilePath)
	databaseFile.Seek(int64(pageNumber-1)*int64(r.pageSize), 0)
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

func (r Reader) getSchemas() []DbSchema {
	pageData := r.read(0)
	page := parsePage(pageData, 0)

	schemas := parseDataBaseSchemas(page)

	reverse(schemas)

	return schemas
}

func (r Reader) getSchemaByTablename(tableName string) (DbSchema, error) {
	schemas := r.getSchemas()

	for _, item := range schemas {
		if item.tableName == tableName {
			return item, nil
		}
	}

	return DbSchema{}, fmt.Errorf("couldn't find schema for table :%v", tableName)

}
