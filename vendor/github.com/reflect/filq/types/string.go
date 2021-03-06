package types

import (
	"bytes"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"github.com/reflect/filq/context"
)

type Str string

func (s Str) Equal(ctx *context.Context, other context.Valuer) (bool, error) {
	ov, err := other.Value(ctx)
	if err != nil {
		return false, err
	}

	if os, ok := ov.(Str); ok {
		return string(s) == string(os), nil
	} else if ob, ok := ov.(Bytes); ok {
		return bytes.Equal([]byte(s), ob), nil
	}

	return false, nil
}

func (s Str) Index(ctx *context.Context, key context.Valuer) (context.Valuer, error) {
	k, err := key.Value(ctx)
	if err != nil {
		return nil, err
	}

	var r int

	switch kt := k.(type) {
	case Str:
		r = strings.Index(string(s), string(kt))
	case Bytes:
		r = strings.Index(string(s), string(kt))
	case Int:
		r = strings.Index(string(s), string(kt))
	case byte:
		r = strings.IndexByte(string(s), kt)
	case rune:
		r = strings.IndexRune(string(s), kt)
	default:
		return nil, errors.WithStack(&context.UnexpectedTypeError{
			Wanted: []reflect.Type{
				reflect.TypeOf(Str("")),
				reflect.TypeOf(Bytes([]byte{})),
				reflect.TypeOf(Int(0)),
				reflect.TypeOf(byte(0)),
				reflect.TypeOf(rune(0)),
			},
			Got: reflect.TypeOf(k),
		})
	}

	if r < 0 {
		return context.NewConstValuer(nil), nil
	}

	return context.NewConstValuer(r), nil
}

type StrConverter struct{}

func (sco *StrConverter) Convert(in interface{}) interface{} {
	return Str(in.(string))
}
