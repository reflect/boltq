package main

import (
	"reflect"

	"github.com/pkg/errors"
	"github.com/reflect/filq/context"
	"github.com/reflect/filq/types"
)

func bytesTree(ctx *context.Context, in []context.Valuer) ([][]byte, error) {
	tree := make([][]byte, len(in))
	for i, va := range in {
		v, err := va.Value(ctx)
		if err != nil {
			return nil, err
		}

		switch vt := v.(type) {
		case types.Bytes:
			tree[i] = []byte(vt)
		case types.Str:
			tree[i] = []byte(vt)
		default:
			return nil, errors.WithStack(&context.UnexpectedTypeError{
				Wanted: []reflect.Type{
					reflect.TypeOf(types.Str("")),
					reflect.TypeOf(types.Bytes([]byte{})),
				},
				Got: reflect.TypeOf(v),
			})
		}
	}

	return tree, nil
}
