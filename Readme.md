# Write-Ahead Logging (WAL) Implementation in Go

## Overview
This project is a minimal, production-aware implementation of a Write-Ahead Log (WAL) in Go. It ensures durable, ordered, and recoverable write operations for systems like databases or message queues.


## Features
- Data durability via periodic flushing and fsync

- Log segmentation with automatic rotation and deletion of old segments

- Crash-safe recovery through log replay with checksum validation

- Buffered I/O for performance, with configurable buffer size

- Thread-safe using sync.Mutex

- Customizable flush intervals with background sync loop

- CRC32 checksum to ensure data integrity

- Easy integration into larger systems via Replay(apply func([]byte))

## Usage

### 1. Plain Text WAL with Raw Byte Slices

```go
package main

import (
	"fmt"
	"time"

	WAL "github.com/mhmeteren/wal"
	"go.uber.org/zap"
)


func main() {

	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	option := WAL.WALOptions{
		LogDir:    "data",
		LogPrefix: "wal.log",

		FlushInterval: 5 * time.Second,

		Log: log,

		MaxBufSize:  1024,
		MaxLogSize:  1 * 1024,
		MaxSegments: 7,
	}

	wal, err := WAL.NewWAL(option)

	if err != nil {
		panic(err)
	}

	defer wal.Close()

	for i := 0; i <= 10; i++ {
		if err := wal.Write([]byte(fmt.Sprintf("data-%d", i))); err != nil {
			panic(err)
		}
	}

	err = wal.Sync()
	if err != nil {
		fmt.Println("Error in sync", err)
	}

	store := []string{}

	wal.Replay(func(data []byte) error {
		store = append(store, string(data))
		return nil
	})

	fmt.Println("Store >>")
	for _, v := range store {
		fmt.Printf("%s\n", v)
	}
	fmt.Println("Store <<")
}
```

### 2. Structured WAL with JSON-Encoded Operations

```go

package main

import (
	"encoding/json"
	"fmt"
	"time"

	WAL "github.com/mhmeteren/wal"
	"go.uber.org/zap"
)

type OperationType string

const (
	OpCreate OperationType = "CREATE"
	OpUpdate OperationType = "UPDATE"
	OpDelete OperationType = "DELETE"
)

type Operation struct {
	Type  OperationType `json:"type"`
	Key   string        `json:"key"`
	Value string        `json:"value,omitempty"`
}

func main() {

	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	option := WAL.WALOptions{
		LogDir:    "data",
		LogPrefix: "wal.log",

		FlushInterval: 1 * time.Second,

		Log: log,

		MaxBufSize:  1024,
		MaxLogSize:  1 * 1024,
		MaxSegments: 2,
	}

	wal, err := WAL.NewWAL(option)

	if err != nil {
		panic(err)
	}

	defer wal.Close()

	store := make(map[string]string)

	wal.Replay(func(data []byte) error {
		var op Operation
		if err := json.Unmarshal(data, &op); err != nil {
			return err
		}
		applyToMemory(store, op)
		return nil
	})

	ops := []Operation{
		{Type: OpCreate, Key: "user", Value: "alice"},
		{Type: OpUpdate, Key: "user", Value: "bob"},
		{Type: OpCreate, Key: "email", Value: "bob@example.com"},
		{Type: OpDelete, Key: "email"},
		{Type: OpCreate, Key: "k1", Value: "v1"},
		{Type: OpUpdate, Key: "k1", Value: "v2"},
		{Type: OpCreate, Key: "k3", Value: "v3"},
		{Type: OpDelete, Key: "k1"},
	}

	for _, op := range ops {
		data, _ := json.Marshal(op)
		if err := wal.Write(data); err != nil {
			panic(err)
		}
		applyToMemory(store, op)
	}

	fmt.Println("Store >>")
	for k, v := range store {
		fmt.Printf("%s : %s\n", k, v)
	}
	fmt.Println("Store <<")

}

func applyToMemory(store map[string]string, op Operation) {
	switch op.Type {
	case OpCreate, OpUpdate:
		store[op.Key] = op.Value
	case OpDelete:
		delete(store, op.Key)
	}
}
```

