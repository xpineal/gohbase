package gohbase

import (
	"context"
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/tsuna/gohbase/hrpc"
	"time"
)

const (
	Family = "F"
)

var (
	//Not Exist
	ErrNotExist = errors.New("item.not.exist")
	//Data corruption
	ErrDataCorrupt = errors.New("data.corruption")
)

type ColV struct {
	ColName string
	Data    []byte
}

//put key - value
func PutRowCell(cli Client, table []byte, key []byte, colName string, data []byte, timeout time.Duration) error {
	//generate row
	var row = map[string]map[string][]byte{
		Family: {
			colName: data,
		},
	}
	return putRow(cli, table, key, row, timeout)
}

//put row cell list
func PutRowCellList(cli Client, table []byte, key []byte, timeout time.Duration, cvs ...ColV) error {
	//generate row
	var row = genRowInfo(cvs...)
	return putRow(cli, table, key, row, timeout)
}

//delete row
func DeleteRow(cli Client, table []byte, key []byte, timeout time.Duration) error {
	var rowFamily = map[string]map[string][]byte{
		Family: nil,
	}
	var ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var req, err = hrpc.NewDel(ctx, table, key, rowFamily)
	if err != nil {
		return err
	}
	_, err = cli.Put(req)
	return err
}

//delete row cell
func DeleteRowCell(cli Client, table []byte, key []byte, colName string, timeout time.Duration) error {
	var rowFamily = map[string]map[string][]byte{
		Family: {
			colName: nil,
		},
	}
	var ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var req, err = hrpc.NewDel(ctx, table, key, rowFamily)
	if err != nil {
		return err
	}
	_, err = cli.Put(req)
	return err
}

//get row
func GetRow(cli Client, table []byte, key []byte, timeout time.Duration) ([]*hrpc.Cell, error) {
	var hRet, err = getRow(cli, table, key, timeout)
	if err != nil {
		return nil, err
	}
	return hRet.Cells, nil
}

//get row cell by first character
func GetRowCellByFirstChar(cli Client,
	table []byte, key []byte, colName byte, timeout time.Duration) ([]byte, error) {

	var hRet, err = getRow(cli, table, key, timeout)
	if err != nil {
		return nil, err
	}

	for _, cell := range hRet.Cells {
		if cell.Qualifier[0] == colName {
			return cell.Value, nil
		}
	}
	return nil, ErrDataCorrupt
}

//get row cell by name
func GetRowCellByName(cli Client, table []byte, key []byte, colName string, timeout time.Duration) ([]byte, error) {
	var hRet, err = getRow(cli, table, key, timeout)
	if err != nil {
		return nil, err
	}

	for _, cell := range hRet.Cells {
		if string(cell.Qualifier) == colName {
			return cell.Value, nil
		}
	}
	return nil, ErrDataCorrupt
}

//scan row
//func ScanRows(cli Client, table[]byte, start, end []byte, reverse bool, )

//gen put
func GenPut(ctx context.Context, table []byte, key []byte, cvs ...ColV) (*hrpc.Mutate, error) {
	var row = genRowInfo(cvs...)
	return hrpc.NewPut(ctx, table, key, row)
}

//get row
func getRow(cli Client, table []byte, key []byte, timeout time.Duration) (*hrpc.Result, error) {
	var ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var req, err = hrpc.NewGet(ctx, table, key)
	if err != nil {
		return nil, err
	}
	var hRet *hrpc.Result
	hRet, err = cli.Get(req)
	if err != nil {
		return nil, err
	}
	if hRet == nil {
		logrus.Error("error----> hRpc.result.should.not.be.nil")
		return nil, ErrNotExist
	}
	if len(hRet.Cells) == 0 {
		return nil, ErrNotExist
	}
	return hRet, nil
}

//put row
func putRow(cli Client, table []byte, key []byte, row map[string]map[string][]byte, timeout time.Duration) error {
	var ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()

	//submit record
	var req, err = hrpc.NewPut(ctx, table, key, row)
	if err != nil {
		return err
	}
	_, err = cli.Put(req)
	return err
}

//generate row info
func genRowInfo(cvs ...ColV) map[string]map[string][]byte {
	var row = make(map[string]map[string][]byte)
	var cellHash = make(map[string][]byte)
	for i := range cvs {
		cellHash[cvs[i].ColName] = cvs[i].Data
	}
	row[Family] = cellHash
	return row
}

func init() {
	logrus.SetLevel(logrus.ErrorLevel)
}