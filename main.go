package main

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/reflect/filq"
)

var (
	BoltDBFile string
)

func init() {
	flag.StringVar(&BoltDBFile, "f", "", "")
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func main() {
	flag.Parse()

	db, err := bolt.Open(BoltDBFile, 0400, &bolt.Options{Timeout: 1 * time.Second, ReadOnly: true})
	if err != nil {
		panic(err)
	}

	err = db.View(func(tx *bolt.Tx) error {
		ctx := filq.NewContext()
		ctx.DefineConverter(reflect.TypeOf(&bolt.Tx{}), &TxConverter{})
		ctx.DefineConverter(reflect.TypeOf(&bolt.Bucket{}), &BucketConverter{})

		outs, err := filq.Run(ctx, flag.Arg(0), tx)
		if err != nil {
			return err
		}

		for _, out := range outs {
			fmt.Printf("%+v\n", out)
		}

		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
