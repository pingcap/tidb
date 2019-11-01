package main

import (
	"context"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

type Storage struct {
}

func NewStorage() *Storage {
	return nil
}

func (s *Storage) Begin() (kv.Transaction, error) {
	return nil, nil
	//return NewTransaction(), nil
}

func (s *Storage) BeginWithStartTS(startTS uint64) (kv.Transaction, error) {
	return nil, nil
	/*
		if trans, err := t.Begin(); err == nil {
			return trans.WithStartTs(startTs), nil
		} else {
			return nil, err
		}
	*/
}

func (s *Storage) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	return nil, nil
}

func (s *Storage) GetClient() kv.Client {
	return nil
}

func (s *Storage) Close() error {
	return nil
}

func (s *Storage) UUID() string {
	return "uuid"
}

func (s *Storage) CurrentVersion() (kv.Version, error) {
	return kv.Version{233}, nil
}

func (s *Storage) GetOracle() oracle.Oracle {
	return nil
}

func (s *Storage) SupportDeleteRange() (supported bool) {
	return false
}

func (s *Storage) Name() string {
	return "wasm-storage-name"
}

func (s *Storage) Describe() string {
	return ""
}

func (s *Storage) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	return nil, nil
}

/*
type Transaction struct {
	startTs uint64
}

func NewTransaction() *Transaction {
	return nil
}

func (t *Transaction) WithStartTs(startTs uint64) *Transaction {
	t.startTs = startTs
	return t
}*/
