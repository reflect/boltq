package main

import (
	"fmt"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/reflect/filq/context"
	"github.com/reflect/filq/types"
)

type bucketSelector struct {
	bucket *bolt.Bucket
	in     []context.Valuer
	tree   [][]byte
}

func (s *bucketSelector) Value(ctx *context.Context) (interface{}, error) {
	b := s.bucket

	for _, c := range s.tree[:len(s.tree)-1] {
		if b == nil {
			return nil, nil
		}

		b = b.Bucket(c)
	}

	if b == nil {
		return nil, nil
	}

	key := s.tree[len(s.tree)-1]
	br := b.Bucket(key)
	if br != nil {
		return ctx.Convert(br), nil
	}

	vr := b.Get(key)
	if vr != nil {
		return ctx.Convert(vr), nil
	}

	return nil, nil
}

type Bucket bolt.Bucket

func (b *Bucket) Format(f fmt.State, c rune) {
	if c != 'v' {
		types.FormatDefault(f, c, (*bolt.Bucket)(b))
	}

	var kv []string
	(*bolt.Bucket)(b).ForEach(func(k, v []byte) error {
		if v == nil {
			if cb := (*bolt.Bucket)(b).Bucket(k); cb != nil {
				kv = append(kv, fmt.Sprintf("%+v{}", types.Bytes(k)))
				return nil
			}
		}

		kv = append(kv, fmt.Sprintf("%+v = %+v", types.Bytes(k), types.Bytes(v)))
		return nil
	})

	fmt.Fprintf(f, "%s", strings.Join(kv, ", "))
}

func (b *Bucket) Select(ctx *context.Context, in []context.Valuer) (context.Valuer, error) {
	tree, err := bytesTree(ctx, in)
	if err != nil {
		return nil, err
	}

	return &bucketSelector{bucket: (*bolt.Bucket)(b), in: in, tree: tree}, nil
}

func (b *Bucket) Expand(ctx *context.Context) ([]context.Valuer, error) {
	var entries []context.Valuer
	err := (*bolt.Bucket)(b).ForEach(func(k, v []byte) error {
		var value interface{}
		if v == nil {
			bucket := (*bolt.Bucket)(b).Bucket(k)
			if bucket != nil {
				value = bucket
			}
		} else {
			value = v
		}

		entries = append(entries, types.NewEntryValuer(k, value))
		return nil
	})
	if err != nil {
		return nil, err
	}

	return entries, nil
}

type BucketConverter struct{}

func (bco *BucketConverter) Convert(in interface{}) interface{} {
	return (*Bucket)(in.(*bolt.Bucket))
}
