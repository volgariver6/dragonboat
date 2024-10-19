// Copyright 2024 Matrix Origin
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
	"io"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
)

type mockArchiveIO struct {
	fs      vfs.FS
	dirname string
}

func newMockArchiveIO(fs vfs.FS, dirname string) *mockArchiveIO {
	return &mockArchiveIO{
		fs:      fs,
		dirname: dirname,
	}
}

func (i *mockArchiveIO) init() error {
	if !fileExists(i.fs, i.dirname) {
		if err := i.fs.MkdirAll(i.dirname, 0755); err != nil {
			return err
		}
	}
	return nil
}

func (i *mockArchiveIO) clean() error {
	return i.fs.RemoveAll(i.dirname)
}

func (i *mockArchiveIO) filesCount() int {
	list, _ := i.fs.List(i.dirname)
	return len(list)
}

func (i *mockArchiveIO) Write(_ context.Context, subDir, filePath string) error {
	src, err := i.fs.Open(filePath)
	if err != nil {
		return err
	}
	defer src.Close()
	if err := i.fs.MkdirAll(i.dirname, 0755); err != nil {
		return err
	}
	dst, err := i.fs.Create(i.fs.PathJoin(i.dirname, i.fs.PathBase(filePath)))
	if err != nil {
		return err
	}
	defer dst.Close()
	_, err = io.Copy(dst, src)
	if err != nil {
		return err
	}
	return nil
}

func (i *mockArchiveIO) List(_ context.Context, surDir string) ([]string, error) {
	return i.fs.List(i.dirname)
}

func (i *mockArchiveIO) Open(_ context.Context, subDir, filename string) (io.ReadCloser, error) {
	src, err := i.fs.Open(i.fs.PathJoin(i.dirname, i.fs.PathBase(filename)))
	if err != nil {
		return nil, err
	}
	return src, nil
}

func (i *mockArchiveIO) Download(_ context.Context, subDir, filename string, dstFile string) error {
	src, err := i.fs.Open(i.fs.PathJoin(i.dirname, i.fs.PathBase(filename)))
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := i.fs.Create(dstFile)
	if err != nil {
		return err
	}
	defer dst.Close()
	_, err = io.Copy(dst, src)
	if err != nil {
		return err
	}
	return nil
}

func (i *mockArchiveIO) Delete(_ context.Context, subDir string, files ...string) error {
	for _, f := range files {
		if err := i.fs.Remove(i.fs.PathJoin(i.dirname, i.fs.PathBase(f))); err != nil {
			return err
		}
	}
	return nil
}

func TestRecorderAppend(t *testing.T) {
	tempDir := t.TempDir()
	r := newArchiveRecorder(tempDir, vfs.NewStrictMem())
	item := config.RecordItem{
		FileNum:  12,
		TS:       time.Now(),
		FirstLsn: 19,
	}
	count := 100
	for i := 0; i < count; i++ {
		assert.NoError(t, r.append(item))
	}
	singleSz := item.Size()
	sz, err := r.size()
	assert.NoError(t, err)
	assert.Equal(t, singleSz*int64(count), sz)
}

func TestArchiveFiles(t *testing.T) {
	logDir := t.TempDir()
	fs := vfs.NewStrictMem()
	r := newArchiveRecorder(logDir, fs)
	count := 20
	logNums := make([]uint64, count)
	filesMetadata := make([]*fileMetadata, 0, count)
	for i := 0; i < count; i++ {
		f, err := fs.Create(makeFilename(fs, logDir, fileTypeLog, fileNum(i+1)))
		assert.NoError(t, err)
		assert.NoError(t, f.Close())

		f, err = fs.Create(makeFilename(fs, logDir, fileTypeIndex, fileNum(i+1)))
		assert.NoError(t, err)
		assert.NoError(t, f.Close())

		item := config.RecordItem{
			FileNum:  uint64(i + 1),
			TS:       time.Now(),
			FirstLsn: uint64(i * 50),
		}
		assert.NoError(t, r.append(item))
		logNums[i] = uint64(i + 1)
		filesMetadata = append(filesMetadata, &fileMetadata{fileNum: fileNum(i + 1)})
	}
	archiveDir := t.TempDir()
	aio := newMockArchiveIO(fs, archiveDir)
	assert.NoError(t, aio.init())
	defer func() {
		assert.NoError(t, aio.clean())
	}()
	a := newArchiver(context.Background(), aio, logDir, fs)
	d := &db{
		opts:     &Options{FS: fs, archiveIO: a},
		dirname:  logDir,
		archiver: a,
	}
	assert.NoError(t, d.archiveFiles(filesMetadata))
	assert.Equal(t, count*2, aio.filesCount())
	sz, err := r.size()
	assert.NoError(t, err)
	assert.Equal(t, int64(count)*singleSize(), sz)
}

func TestOffset(t *testing.T) {
	tempDir := t.TempDir()
	r := newArchiveRecorder(tempDir, vfs.NewStrictMem())
	count := 200
	for i := 0; i < count; i++ {
		if i%2 == 0 {
			continue
		}
		item := config.RecordItem{
			FileNum:  uint64(i),
			TS:       time.Now(),
			FirstLsn: uint64(i * 50),
		}
		assert.NoError(t, r.append(item))
	}

	var item config.RecordItem
	for i := 1; i < 92; i++ {
		if i%2 != 0 {
			assert.Equal(t, int64(i/2)*item.Size(), r.offset(func(item config.RecordItem) int {
				return int(item.FileNum) - i
			}))
		} else {
			assert.Equal(t, int64(i/2-1)*item.Size(), r.offset(func(item config.RecordItem) int {
				return int(item.FileNum) - i
			}))
		}
	}
}

func TestSearchByTS(t *testing.T) {
	tempDir := t.TempDir()
	r := newArchiveRecorder(tempDir, vfs.NewStrictMem())
	count := 20
	now := time.Now()
	for i := 0; i < count; i++ {
		item := config.RecordItem{
			FileNum:  uint64(i + 1),
			TS:       now.Add(time.Second * time.Duration(i)),
			FirstLsn: uint64(i * 50),
		}
		assert.NoError(t, r.append(item))
	}
	for i := 0; i < count; i++ {
		ts := now.Add(time.Second*time.Duration(i) + time.Millisecond*20)
		pos, committed, err := r.searchByTS(ts)
		assert.NoError(t, err)
		assert.Equal(t, i*50, int(pos))
		assert.Equal(t, true, committed)
	}
	ts := now.Add(time.Second * (-100))
	_, _, err := r.searchByTS(ts)
	assert.Error(t, err)
}

func TestSearchByLsn(t *testing.T) {
	tempDir := t.TempDir()
	r := newArchiveRecorder(tempDir, vfs.NewStrictMem())
	count := 20
	now := time.Now()
	for i := 0; i < count; i++ {
		item := config.RecordItem{
			FileNum:  uint64(i + 1),
			TS:       now.Add(time.Second * time.Duration(i)),
			FirstLsn: uint64(i*50) + 10,
		}
		assert.NoError(t, r.append(item))
	}
	for i := 0; i < count; i++ {
		lsn := i*50 + 20
		pos, committed, err := r.searchByLsn(uint64(lsn))
		assert.NoError(t, err)
		assert.Equal(t, i*50+10, int(pos))
		assert.Equal(t, true, committed)
	}
	_, _, err := r.searchByLsn(0)
	assert.Error(t, err)
}

func TestGC(t *testing.T) {
	archiveDir := t.TempDir()
	fs := vfs.NewMem()
	r := newArchiveRecorder(archiveDir, fs)
	count := 20
	now := time.Now()
	for i := 0; i < count; i++ {
		item := config.RecordItem{
			FileNum:  uint64(i + 1),
			TS:       now.Add(time.Second * time.Duration(i) * 5),
			FirstLsn: uint64(i*50) + 10,
		}
		assert.NoError(t, r.append(item))
	}
	aio := newMockArchiveIO(fs, archiveDir)
	assert.NoError(t, aio.init())
	defer func() {
		assert.NoError(t, aio.clean())
	}()
	a := newArchiver(context.Background(), aio, archiveDir, fs)
	a.gc(now.Add(time.Second*13), true)

	sz, err := r.size()
	assert.NoError(t, err)
	assert.Equal(t, int64(408), sz)
}

func TestGCFailedRetry(t *testing.T) {
	fs := vfs.NewMem()
	archiveDir := t.TempDir()
	aio := newMockArchiveIO(fs, archiveDir)
	assert.NoError(t, aio.init())
	defer func() {
		assert.NoError(t, aio.clean())
	}()
	logDir := t.TempDir()
	a := newArchiver(context.Background(), aio, logDir, fs)
	a.mu.gcFailedQueue = append(a.mu.gcFailedQueue, "a.log")
	a.gcFailedRetry()
	assert.Equal(t, 1, len(a.mu.gcFailedQueue))
	f, err := fs.Create(fs.PathJoin(logDir, "a.log"))
	assert.NoError(t, err)
	defer f.Close()
	_, err = f.Write([]byte("test data"))
	assert.NoError(t, err)
	assert.NoError(t, a.Write(context.TODO(), "", fs.PathJoin(logDir, "a.log")))
	a.gcFailedRetry()
	assert.Equal(t, 0, len(a.mu.gcFailedQueue))
}

func TestBackup(t *testing.T) {
	fs := vfs.NewStrictMem()
	logDir := t.TempDir()
	r := newArchiveRecorder(logDir, fs)
	r.ensureDir()
	localFile := makeFilename(fs, logDir, fileTypeArchive, 0)
	f, err := fs.Create(localFile)
	assert.NoError(t, err)
	assert.NotNil(t, f)

	archiveDir := t.TempDir()
	aio := newMockArchiveIO(fs, archiveDir)
	assert.NoError(t, aio.init())
	defer func() {
		assert.NoError(t, aio.clean())
	}()
	a := newArchiver(context.Background(), aio, logDir, fs)
	a.backup()
	assert.Equal(t, 1, aio.filesCount())
}

func TestReadItemByFileNum(t *testing.T) {
	archiveDir := t.TempDir()
	fs := vfs.NewMem()
	r := newArchiveRecorder(archiveDir, fs)
	count := 20
	now := time.Now()
	for i := 0; i < count; i++ {
		item := config.RecordItem{
			FileNum:  uint64(i + 1),
			TS:       now.Add(time.Second * time.Duration(i) * 5),
			FirstLsn: uint64(i*50) + 10,
		}
		assert.NoError(t, r.append(item))
	}
	item, err := r.readItemByFileNum(10)
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), item.FileNum)
}
