package log

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
)

const ErrorKey = "error"

type Fld map[string]any

// FieldsError is an error carrying fields for the log record that reports it.
type FieldsError struct {
	err    error
	fields Fld
}

func (e *FieldsError) Error() string {
	return e.err.Error()
}

func (e *FieldsError) Unwrap() error {
	return e.err
}

// Fields returns a copy of the fields carried by the error.
func (e *FieldsError) Fields() Fld {
	return maps.Clone(e.fields)
}

// LogValue renders the error as a group of its message and its fields.
func (e *FieldsError) LogValue() slog.Value {
	return groupValue(e.Error(), e.fields)
}

// Wrap prefixes err with msg and attaches fields. Fields of a FieldsError
// already in the chain are kept; on a key collision the new value wins.
func Wrap(msg string, err error, fields Fld) error {
	merged := Fld{}

	if inner, ok := errors.AsType[*FieldsError](err); ok {
		maps.Copy(merged, inner.fields)
	}

	maps.Copy(merged, fields)

	return &FieldsError{
		err:    fmt.Errorf("%s: %w", msg, err),
		fields: merged,
	}
}

// Err returns an attribute for err under ErrorKey. When the chain holds a
// FieldsError, the attribute is a group of the full message and its fields;
// otherwise it is the message alone. A nil err yields an empty attribute,
// which handlers skip.
func Err(err error) slog.Attr {
	if err == nil {
		return slog.Attr{}
	}

	if fieldsErr, ok := errors.AsType[*FieldsError](err); ok {
		return slog.Attr{Key: ErrorKey, Value: groupValue(err.Error(), fieldsErr.fields)}
	}

	return slog.String(ErrorKey, err.Error())
}

func groupValue(msg string, fields Fld) slog.Value {
	attrs := make([]slog.Attr, 0, len(fields)+1)
	attrs = append(attrs, slog.String("msg", msg))

	for _, key := range slices.Sorted(maps.Keys(fields)) {
		attrs = append(attrs, slog.Any(key, fields[key]))
	}

	return slog.GroupValue(attrs...)
}
