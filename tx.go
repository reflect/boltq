package main

import (
	"fmt"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/reflect/filq/context"
	"github.com/reflect/filq/types"
)

type txSelector struct {
	tx   *bolt.Tx
	in   []context.Valuer
	tree [][]byte
}

func (s *txSelector) Value(ctx *context.Context) (interface{}, error) {
	b := s.tx.Bucket(s.tree[0])
	if b == nil {
		return nil, nil
	}

	if len(s.tree) == 1 {
		return ctx.Convert(b), nil
	}

	inner := &bucketSelector{
		bucket: b,
		tree:   s.tree[1:],
	}
	return inner.Value(ctx)
}

type Tx bolt.Tx

func (tx *Tx) Format(f fmt.State, c rune) {
	if c != 'v' {
		types.FormatDefault(f, c, (*bolt.Tx)(tx))
	}

	var buckets []string
	(*bolt.Tx)(tx).ForEach(func(name []byte, bucket *bolt.Bucket) error {
		buckets = append(buckets, fmt.Sprintf("%+v{}", types.Bytes(name)))
		return nil
	})

	fmt.Fprintf(f, "%s", strings.Join(buckets, ", "))
}

func (tx *Tx) Select(ctx *context.Context, in []context.Valuer) (context.Valuer, error) {
	tree, err := bytesTree(ctx, in)
	if err != nil {
		return nil, err
	}

	return &txSelector{tx: (*bolt.Tx)(tx), in: in, tree: tree}, nil
}

func (tx *Tx) Expand(ctx *context.Context) ([]context.Valuer, error) {
	var entries []context.Valuer
	err := (*bolt.Tx)(tx).ForEach(func(name []byte, bucket *bolt.Bucket) error {
		entries = append(entries, types.NewEntryValuer(name, bucket))
		return nil
	})
	if err != nil {
		return nil, err
	}

	return entries, nil
}

type TxConverter struct{}

func (tco *TxConverter) Convert(in interface{}) interface{} {
	return (*Tx)(in.(*bolt.Tx))
}
