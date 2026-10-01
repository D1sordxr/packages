// Package tx provides a transaction manager on top of executor; the transaction lives in context.
package tx

//go:generate ifacemaker -f *.go -s ManagerImpl -i Manager -p tx -o manager_interface.go

//go:generate gowrap gen -g -p . -i Manager -t ../../gowrap/errwrap.tmpl -o manager_with_errwrap.go -v "OpPrefix=postgres.tx.Manager"

type ManagerImpl struct {
	executor
}

func NewManager(executor executor) *ManagerImpl {
	return &ManagerImpl{executor: executor}
}
