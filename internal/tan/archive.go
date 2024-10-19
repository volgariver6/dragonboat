// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tan

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/vfs"
)

const (
	gcInterval          = time.Hour * 24
	gcFailRetryInterval = time.Minute
	haInterval          = time.Hour * 3
	gcThreshold         = time.Hour * 24 * 30
)

func (d *db) startArchiver() {
	if d.archiver == nil {
		return
	}
	gcTicker := time.NewTicker(gcInterval)
	defer gcTicker.Stop()
	haTicker := time.NewTicker(haInterval)
	defer haTicker.Stop()
	gcFailRetryTicker := time.NewTicker(gcFailRetryInterval)
	defer gcFailRetryTicker.Stop()

	// If there is a ARCHIVE.tmp file, remove it first.
	tmpFile := makeFilename(d.archiver.recorder.fs, d.dirname, fileTypeArchiveTemp, 0)
	if fileExists(d.archiver.recorder.fs, tmpFile) {
		_ = d.archiver.recorder.fs.Remove(tmpFile)
	}

	for {
		select {
		case <-d.stopper.ShouldStop():
			return

		case item := <-d.archiver.appendC:
			if err := d.archiver.recorder.append(item); err != nil {
				plog.Errorf("failed to append record item to archive file, %+v", item)
			}

		case <-gcTicker.C:
			d.archiver.gc(time.Now().Add(-gcThreshold), false)

		case <-haTicker.C:
			d.archiver.backup()

		case <-gcFailRetryTicker.C:
			// start a async job to delete files.
			go d.archiver.gcFailedRetry()
		}
	}
}

type archiver struct {
	config.ArchiveIO

	ctx      context.Context
	subDir   string
	appendC  chan config.RecordItem
	recorder *recorder
	mu       struct {
		sync.Mutex
		// gcFailedQueue contains the gc failed files, which will be GCed
		// for next time.
		gcFailedQueue []string
	}
}

func newArchiver(
	ctx context.Context,
	archiveIO config.ArchiveIO,
	dirname string,
	fs vfs.FS,
) *archiver {
	if archiveIO == nil {
		return nil
	}
	return &archiver{
		ctx:       ctx,
		subDir:    fs.PathBase(dirname),
		ArchiveIO: archiveIO,
		appendC:   make(chan config.RecordItem, 30),
		recorder:  newArchiveRecorder(dirname, fs),
	}
}

func (a *archiver) isActive() bool {
	return a != nil && a.ArchiveIO != nil
}

func (a *archiver) addItem(item config.RecordItem) {
	if a == nil {
		return
	}
	select {
	case a.appendC <- item:
	default:
	}
}

func (a *archiver) gcFailedRetry() {
	if a.ArchiveIO == nil {
		return
	}
	a.mu.Lock()
	files := a.mu.gcFailedQueue[:]
	a.mu.gcFailedQueue = a.mu.gcFailedQueue[:0]
	a.mu.Unlock()
	for _, f := range files {
		if err := a.Delete(a.ctx, a.subDir, f); err != nil {
			plog.Errorf("failed to delete log/index file from remote in retry, file: %s, err: %v",
				f, err)
			a.mu.Lock()
			a.mu.gcFailedQueue = append(a.mu.gcFailedQueue, f)
			a.mu.Unlock()
		}
	}
}

func (a *archiver) gc(gcTS time.Time, sync bool) {
	if err := a.recorder.withFile(typeRead, func() error {
		// count is the number of items we should remove from the ARCHIVE file
		// from the beginning.
		var count int64
		var fileNums []uint64
		iterateItems(a.recorder.mu.file, func(item config.RecordItem) bool {
			if item.TS.Before(gcTS) {
				count++
				fileNums = append(fileNums, item.FileNum)
				return true
			}
			return false
		})
		if count > 0 {
			pos := count * singleSize()
			_, err := a.recorder.mu.file.Seek(pos, 0)
			if err != nil {
				plog.Errorf("failed to seek ARCHIVE file: %v", err)
				return err
			}
			tmpFilePath := makeFilename(a.recorder.fs, a.recorder.dirname, fileTypeArchiveTemp, 0)
			if fileExists(a.recorder.fs, tmpFilePath) {
				if err := a.recorder.fs.Remove(tmpFilePath); err != nil {
					plog.Errorf("failed to remove tmp ARCHIVE file: %v", err)
					return err
				}
			}
			tf, err := a.recorder.fs.Create(tmpFilePath)
			if err != nil {
				plog.Errorf("failed to create tmp ARCHIVE file: %v", err)
				return err
			}
			_, err = io.Copy(tf, a.recorder.mu.file)
			if err != nil {
				plog.Errorf("failed to copy tmp ARCHIVE file: %v", err)
				return err
			}
			if err := a.recorder.fs.Rename(tmpFilePath, a.recorder.filePath); err != nil {
				plog.Errorf("failed to rename tmp ARCHIVE file: %v", err)
				return err
			}

			files := make([]string, 0, len(fileNums))
			for _, num := range fileNums {
				files = append(files,
					makeFilename(a.recorder.fs, a.recorder.dirname, fileTypeLog, fileNum(num)),
					makeFilename(a.recorder.fs, a.recorder.dirname, fileTypeIndex, fileNum(num)),
				)
			}

			if a.ArchiveIO != nil {
				if sync {
					if err := a.Delete(a.ctx, a.subDir, files...); err != nil {
						plog.Errorf("failed to delete log/index file from remote: %v", err)
						a.mu.gcFailedQueue = append(a.mu.gcFailedQueue, files...)
					}
				} else {
					go func() {
						if err := a.Delete(a.ctx, a.subDir, files...); err != nil {
							plog.Errorf("failed to delete log/index file from remote: %v", err)
							a.mu.Lock()
							a.mu.gcFailedQueue = append(a.mu.gcFailedQueue, files...)
							a.mu.Unlock()
						}
					}()
				}
			}
		}
		return nil
	}); err != nil {
		plog.Errorf("failed to do GC operation: %v", err)
		return
	}
}

func (a *archiver) backup() {
	if a.ArchiveIO != nil {
		if err := a.Write(a.ctx, "", a.recorder.filePath); err != nil {
			plog.Errorf("failed to backup ARCHIVE file to remote: %v", err)
		}
	}
}

type fileOpenType int8

var (
	typeRead  fileOpenType = 0
	typeWrite fileOpenType = 1
)

type recorder struct {
	dirname string
	fs      vfs.FS
	// filePath is the file path of ARCHIVE file itself.
	filePath string
	mu       struct {
		sync.Mutex
		// file is the file descriptor of the ARCHIVE file.
		file vfs.File
	}
}

func newArchiveRecorder(dirname string, fs vfs.FS) *recorder {
	r := &recorder{
		dirname:  dirname,
		fs:       fs,
		filePath: makeFilename(fs, dirname, fileTypeArchive, 0),
	}
	r.mu.file = nil
	r.ensureDir()
	return r
}

func (r *recorder) ensureDir() {
	_, err := r.fs.Stat(r.dirname)
	if err != nil {
		var pathError *os.PathError
		if errors.As(err, &pathError) {
			if err := r.fs.MkdirAll(r.dirname, 0755); err != nil {
				panic(fmt.Sprintf("failed to mkdir %s", r.dirname))
			}
		} else {
			panic(fmt.Sprintf("stat dir failed: %v", err))
		}
	}
}

func (r *recorder) withFile(typ fileOpenType, fn func() error) error {
	closeFn, err := r._ensureFile(typ)
	if err != nil {
		return err
	}
	defer closeFn()
	return fn()
}

// _ensureFile ensures the ARCHIVE file is opened correctly. Do not call it
// directly, instead, call it in withFile() method.
func (r *recorder) _ensureFile(typ fileOpenType) (func(), error) {
	r.mu.Lock()
	var err error
	if typ == typeRead {
		r.mu.file, err = r.fs.Open(r.filePath)
	} else {
		r.mu.file, err = r.fs.OpenForAppend(r.filePath)
	}
	if err != nil {
		var pathError *os.PathError
		if errors.As(err, &pathError) {
			if err := r.fs.MkdirAll(r.dirname, 0666); err != nil {
				return nil, err
			}
			r.mu.file, err = r.fs.Create(r.filePath)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return func() {
		_ = r.mu.file.Close()
		r.mu.file = nil
		r.mu.Unlock()
	}, nil
}

func (r *recorder) size() (int64, error) {
	stat, err := r.fs.Stat(r.filePath)
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// append appends the item to the ARCHIVE file. The item is not committed for
// now. It will be committed when commit() is called after the log file is
// written to remote storage.
func (r *recorder) append(item config.RecordItem) error {
	return r.withFile(typeWrite, func() error {
		data, err := item.Marshal()
		if err != nil {
			return err
		}
		_, err = r.mu.file.Write(data)
		if err != nil {
			return err
		}
		if err := r.mu.file.Sync(); err != nil {
			return err
		}
		return nil
	})
}

// searchByTS searches the ARCHIVE file to find the item whose TS is less than
// the specified timestamp.
// Return values:
//  1. lsn for the found item
//  2. if the first pos is committed
//  3. error
func (r *recorder) searchByTS(ts time.Time) (uint64, bool, error) {
	var lsn uint64
	var committed bool
	if err := r.withFile(typeRead, func() error {
		rightIndex, err := r.lastIndex()
		if err != nil {
			return err
		}
		pos := r.findLocked(0, int64(rightIndex), func(item config.RecordItem) int {
			t1 := item.TS.UnixNano()
			t2 := ts.UnixNano()
			return int(t1 - t2)
		})
		lsn, committed, err = r.committedFileLsnLocked(pos)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return 0, false, err
	}
	return lsn, committed, nil
}

// searchByTS searches the ARCHIVE file to find the item whose lsn is less than
// the specified lsn.
// Return values:
//  1. lsn for the found item
//  2. if the first pos is committed
//  3. error
func (r *recorder) searchByLsn(lsn uint64) (uint64, bool, error) {
	var foundLsn uint64
	var committed bool
	if err := r.withFile(typeRead, func() error {
		rightIndex, err := r.lastIndex()
		if err != nil {
			return err
		}
		pos := r.findLocked(0, int64(rightIndex), func(item config.RecordItem) int {
			return int(item.FirstLsn - lsn)
		})
		foundLsn, committed, err = r.committedFileLsnLocked(pos)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return 0, false, err
	}
	return foundLsn, committed, nil
}

func fileExists(fs vfs.FS, path string) bool {
	_, err := fs.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false
		}
		return true
	}
	return true
}

func iterateItems(file vfs.File, f func(item config.RecordItem) bool) {
	sz := singleSize()
	buf := make([]byte, sz)
	stat, err := file.Stat()
	if err != nil || stat.Size() == 0 {
		return
	}
	count := stat.Size() / singleSize()
	var i int64
	for i = 0; i < count; i++ {
		n, err := file.ReadAt(buf, i*sz)
		if err != nil || int64(n) != sz {
			return
		}
		var item config.RecordItem
		if err := item.Unmarshal(buf); err != nil {
			return
		}
		if !f(item) {
			return
		}
	}
}

// offset reads the opened file to get the num position, and return the
// position in the file.
func (r *recorder) offset(compare func(config.RecordItem) int) int64 {
	var pos int64
	if err := r.withFile(typeRead, func() error {
		stat, err := r.mu.file.Stat()
		if err != nil || stat.Size() == 0 {
			pos = -1
		} else {
			pos = r.findLocked(0, stat.Size()/singleSize()-1, compare)
		}
		return nil
	}); err != nil {
		return -1
	}
	return pos
}

func (r *recorder) readItemByFileNum(num uint64) (config.RecordItem, error) {
	pos := r.offset(func(item config.RecordItem) int {
		return int(item.FileNum - num)
	})
	var item config.RecordItem
	var err error
	if err = r.withFile(typeRead, func() error {
		var readErr error
		if pos < 0 {
			return fmt.Errorf("cannot get item with file num %d", num)
		}
		item, readErr = r.readItemLocked(pos)
		if readErr != nil {
			return readErr
		}
		return nil
	}); err != nil {
		return config.RecordItem{}, err
	}
	return item, nil
}

func (r *recorder) readItemLocked(pos int64) (config.RecordItem, error) {
	var item config.RecordItem
	buf := make([]byte, item.Size())
	n, err := r.mu.file.ReadAt(buf, pos)
	if err != nil || int64(n) != item.Size() {
		return config.RecordItem{}, err
	}
	if err := item.Unmarshal(buf); err != nil {
		return config.RecordItem{}, err
	}
	return item, nil
}

func position(index int64) int64 {
	var item config.RecordItem
	return index * item.Size()
}

func singleSize() int64 {
	var item config.RecordItem
	return item.Size()
}

func (r *recorder) lastIndex() (int, error) {
	st, err := r.mu.file.Stat()
	if err != nil {
		return 0, err
	}
	return int(st.Size()/singleSize()) - 1, nil
}

// search the record item backward from the specified position, util
// a commit one is found.
// Return values:
//  1. lsn for the found item
//  2. if the first pos is committed
//  3. error
func (r *recorder) committedFileLsnLocked(pos int64) (uint64, bool, error) {
	// remote indicates that the pos on the remote storage has been committed.
	committed := true
	// find the latest committed record.
	sz := singleSize()
	for {
		if pos < 0 {
			return 0, committed, raftio.ErrArchiveItemNotFound
		}
		item, err := r.readItemLocked(pos)
		if err != nil {
			return 0, committed, err
		}
		// If the file does not exist, means it has been committed.
		// If it has not been committed, go backward to find the committed item.
		if !fileExists(r.fs, makeFilename(r.fs, r.dirname, fileTypeLog, fileNum(item.FileNum))) {
			return item.FirstLsn, committed, nil
		}
		committed = false
		pos -= sz
	}
}

// we consider the items in the file as an array which is sorted. leftIndex
// and rightIndex are the index in the array. return the position of an item
// in the file whose log num is the same as the specified one.
func (r *recorder) findLocked(
	leftIndex, rightIndex int64, compare func(config.RecordItem) int,
) int64 {
	if leftIndex > rightIndex {
		return -1
	}
	if leftIndex == rightIndex {
		pos := position(leftIndex)
		item, err := r.readItemLocked(pos)
		if err != nil {
			return -1
		}
		if compare(item) <= 0 {
			return pos
		}
		return -1
	}

	if leftIndex+1 == rightIndex {
		lPos := position(leftIndex)
		lItem, err := r.readItemLocked(lPos)
		if err != nil {
			return -1
		}
		rPos := position(rightIndex)
		rItem, err := r.readItemLocked(rPos)
		if err != nil {
			return -1
		}
		// it is smaller than the smallest.
		if compare(lItem) > 0 {
			return -1
		}
		// it is bigger than or equal to the biggest.
		if compare(rItem) <= 0 {
			return rPos
		}
		if compare(lItem) <= 0 {
			return lPos
		}
		return -1
	}

	midIndex := (leftIndex + rightIndex) / 2
	midPos := position(midIndex)
	item, err := r.readItemLocked(midPos)
	if err != nil {
		return -1
	}
	if compare(item) == 0 {
		return midPos
	}
	if compare(item) < 0 {
		return r.findLocked(midIndex, rightIndex, compare)
	} else {
		return r.findLocked(leftIndex, midIndex, compare)
	}
}
