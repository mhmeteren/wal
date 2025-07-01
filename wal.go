package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	lengthBufferSize   = 4
	checkSumBufferSize = 4
)

type walFile struct {
	path        string
	segmentID   int64
	startOffset int64
}

type file interface {
	io.Closer
	io.ReadWriteSeeker
	Sync() error
}

type WAL struct {
	logDir    string
	logPrefix string

	currentSegmentID int64
	segmentCount     int
	maxSegments      int

	logSize      int64
	maxLogSize   int64
	globalOffset int64

	log *zap.Logger

	file          file
	bufWrite      *bufio.Writer
	maxBufferSize int64
	bufferSize    int64
	mu            sync.Mutex
	syncTicker    *time.Ticker
	done          chan bool
	sizeBuf       [lengthBufferSize]byte
	sizeCheckSum  [checkSumBufferSize]byte
}

type WALOptions struct {
	LogDir    string
	LogPrefix string

	FlushInterval time.Duration

	Log *zap.Logger

	MaxBufSize  int64
	MaxLogSize  int64
	MaxSegments int
}

func NewWAL(option WALOptions) (*WAL, error) {

	w := &WAL{
		logDir:           option.LogDir,
		logPrefix:        option.LogPrefix,
		currentSegmentID: 1,
		segmentCount:     1,
		globalOffset:     0,
		log:              option.Log,
		maxLogSize:       option.MaxLogSize,
		maxSegments:      option.MaxSegments,
		maxBufferSize:    option.MaxBufSize,
		syncTicker:       time.NewTicker(option.FlushInterval),
		done:             make(chan bool, 1),
	}

	if err := w.initFromExistingFiles(); err != nil {
		return nil, err
	}

	go w.startSyncLoop()
	return w, nil
}

func (wal *WAL) initFromExistingFiles() error {
	if err := os.MkdirAll(wal.logDir, 0755); err != nil {
		return err
	}

	files, err := getSortedWalFiles(wal.logDir, wal.logPrefix)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		// create a new segment
		wal.currentSegmentID = 1
		wal.segmentCount = 1
		wal.globalOffset = 0

		fileName := fmt.Sprintf("%s/%s.%d.%d", wal.logDir, wal.logPrefix, 1, 0)
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		if err != nil {
			return err
		}

		wal.file = file
		wal.bufWrite = bufio.NewWriter(file)
		wal.logSize = 0
		return nil
	}

	// find and open last file
	last := files[len(files)-1]
	file, err := os.OpenFile(last.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	wal.file = file
	wal.bufWrite = bufio.NewWriter(file)
	wal.currentSegmentID = last.segmentID
	wal.segmentCount = len(files)
	wal.globalOffset = last.startOffset

	fi, err := file.Stat()
	if err != nil {
		return err
	}
	wal.logSize = fi.Size()

	return nil
}

func (wal *WAL) startSyncLoop() {
	for {
		select {
		case <-wal.syncTicker.C:
			wal.mu.Lock()
			err := wal.Sync()
			if err == nil {
				wal.log.Info("Automatically written to disk", zap.String("Tag", "Ticker"))
			} else {
				wal.log.Error("Flush error occurred", zap.String("Tag", "Ticker"), zap.Error(err))
			}
			wal.mu.Unlock()
		case <-wal.done:
			wal.log.Info("Stopped", zap.String("Tag", "WAL"))
			return
		}
	}
}

func (wal *WAL) Sync() error {
	if err := wal.bufWrite.Flush(); err != nil {
		return err
	}
	if err := wal.file.Sync(); err != nil {
		return err
	}

	wal.bufferSize = 0
	wal.log.Info("Written to disk *persistently*")
	return nil
}

func (wal *WAL) Close() error {
	wal.syncTicker.Stop()
	wal.done <- true

	wal.mu.Lock()
	defer wal.mu.Unlock()

	if err := wal.Sync(); err != nil {
		return err
	}

	return wal.file.Close()
}

func (wal *WAL) Write(data []byte) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	entrySize := lengthBufferSize + checkSumBufferSize + len(data)

	// 1. Buffer control
	if wal.bufferSize+int64(entrySize) >= wal.maxBufferSize {
		if err := wal.Sync(); err != nil {
			return err
		}
	}

	// 2. file size control
	if wal.logSize+int64(entrySize) > wal.maxLogSize {
		if err := wal.Sync(); err != nil {
			return err
		}
		if err := wal.rotateLog(); err != nil {
			return err
		}
	}

	// 1. Data len
	binary.LittleEndian.PutUint32(wal.sizeBuf[:], uint32(len(data)))
	if _, err := wal.bufWrite.Write(wal.sizeBuf[:]); err != nil {
		return err
	}

	// 2. Checksum
	checksum := crc32.ChecksumIEEE(data)
	binary.LittleEndian.PutUint32(wal.sizeCheckSum[:], checksum)
	if _, err := wal.bufWrite.Write(wal.sizeCheckSum[:]); err != nil {
		return err
	}

	// 3. Data
	if _, err := wal.bufWrite.Write(data); err != nil {
		return err
	}

	wal.bufferSize += int64(entrySize)
	wal.logSize += int64(entrySize)
	wal.globalOffset++

	wal.log.Info("Write-Ahead Log", zap.Int64("Segment", wal.currentSegmentID), zap.Int64("Offset", wal.globalOffset), zap.Int("byte", entrySize), zap.String("Tag", "Write"))

	return nil
}

func (wal *WAL) rotateLog() error {
	if err := wal.file.Close(); err != nil {
		return err
	}

	if wal.segmentCount >= wal.maxSegments {
		if err := wal.deleteOldestSegment(); err != nil {
			return err
		}
	}

	wal.currentSegmentID++
	wal.segmentCount++
	wal.logSize = 0

	newFileName := fmt.Sprintf("%s/%s.%d.%d", wal.logDir, wal.logPrefix, wal.currentSegmentID, wal.globalOffset)
	file, err := os.OpenFile(newFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	wal.file = file
	wal.bufWrite = bufio.NewWriter(file)

	wal.log.Info("New segment", zap.String("File", newFileName))
	return nil
}

func (wal *WAL) deleteOldestSegment() error {
	files, err := getSortedWalFiles(wal.logDir, wal.logPrefix)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return nil
	}

	oldest := files[0]
	wal.log.Info("Deleting oldest segment", zap.String("path", oldest.path), zap.String("Tag", "Rotate"))

	err = os.Remove(oldest.path)
	if err != nil {
		return err
	}

	wal.segmentCount--
	return nil
}

func (wal *WAL) Replay(apply func([]byte) error) error {
	files, err := getSortedWalFiles(wal.logDir, wal.logPrefix)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		wal.log.Info("Segment file not found.", zap.String("Tag", "Replay"))
		return nil
	}

	for _, walFile := range files {
		wal.log.Info("Opening file", zap.String("path", walFile.path), zap.String("Tag", "Replay"))
		file, err := os.Open(walFile.path)
		if err != nil {
			return err
		}

		reader := bufio.NewReader(file)

		for {
			lenBuf := make([]byte, lengthBufferSize)
			_, err := io.ReadFull(reader, lenBuf)
			if err == io.EOF {
				break // end of file
			}
			if err != nil {
				return err
			}

			length := binary.LittleEndian.Uint32(lenBuf)

			// checksum
			checkBuf := make([]byte, checkSumBufferSize)
			_, err = io.ReadFull(reader, checkBuf)
			if err != nil {
				return err
			}
			expectedChecksum := binary.LittleEndian.Uint32(checkBuf)

			// data
			data := make([]byte, length)
			_, err = io.ReadFull(reader, data)
			if err != nil {
				return err
			}

			// checksum validation
			actualChecksum := crc32.ChecksumIEEE(data)
			if actualChecksum != expectedChecksum {
				return fmt.Errorf("[Replay] Checksum mismatch, file may be corrupted: %s", walFile.path)
			}

			if err := apply(data); err != nil {
				return err
			}
		}

		file.Close()
	}

	wal.log.Info("All segments loaded successfully", zap.String("Tag", "Replay"))
	return nil
}

func getSortedWalFiles(dir, prefix string) ([]walFile, error) {
	paths, err := filepath.Glob(fmt.Sprintf("%s/%s.*.*", dir, prefix))
	if err != nil {
		return nil, err
	}

	var files []walFile
	for _, path := range paths {
		base := filepath.Base(path)
		parts := strings.Split(base, ".")

		if len(parts) != 4 {
			continue //format: wal.log.segment.offset
		}

		segID, err1 := strconv.Atoi(parts[2])
		offset, err2 := strconv.Atoi(parts[3])
		if err1 != nil || err2 != nil {
			continue
		}

		files = append(files, walFile{
			path:        path,
			segmentID:   int64(segID),
			startOffset: int64(offset),
		})
	}

	// sort by segmentID, then offset
	sort.Slice(files, func(i, j int) bool {
		if files[i].segmentID == files[j].segmentID {
			return files[i].startOffset < files[j].startOffset
		}
		return files[i].segmentID < files[j].segmentID
	})

	return files, nil
}
