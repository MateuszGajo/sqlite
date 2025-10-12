package main

import (
	"encoding/binary"
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
