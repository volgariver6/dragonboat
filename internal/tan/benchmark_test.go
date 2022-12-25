// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tan

import (
	"testing"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/logger"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/stretchr/testify/require"
)

var benchmarkTestDirname = "tan_benchmark_dir"

func benchmarkWrite1(b *testing.B, sz int) {
	logger.GetLogger("tan").SetLevel(logger.WARNING)
	b.ReportAllocs()
	fs := vfs.GetTestFS()
	if err := fs.MkdirAll(benchmarkTestDirname, 0766); err != nil {
		b.Fatalf("failed to create dir %v", err)
	}
	defer func() {
		if err := fs.RemoveAll(benchmarkTestDirname); err != nil {
			b.Fatalf("failed to remove dir %v", err)
		}
	}()
	cfg := config.NodeHostConfig{
		Expert: config.ExpertConfig{
			FS:    fs,
			LogDB: config.GetLargeMemLogDBConfig(),
		},
	}
	dirs := []string{benchmarkTestDirname}
	ldb, err := CreateTan(cfg, nil, dirs, []string{})
	require.NoError(b, err)
	u := pb.Update{
		ShardID: 100,
		EntriesToSave: []pb.Entry{
			pb.Entry{Cmd: make([]byte, sz)},
		},
	}
	defer b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ldb.SaveRaftState([]pb.Update{u}, 1); err != nil {
			b.Fatalf("failed to save raft state %v", err)
		}
		b.SetBytes(int64(sz))
	}
}

func benchmarkWrite(b *testing.B, sz int) {
	logger.GetLogger("tan").SetLevel(logger.WARNING)
	b.ReportAllocs()
	fs := vfs.GetTestFS()
	opts := &Options{
		FS: fs,
	}
	if err := fs.MkdirAll(benchmarkTestDirname, 0766); err != nil {
		b.Fatalf("failed to create dir %v", err)
	}
	defer func() {
		if err := fs.RemoveAll(benchmarkTestDirname); err != nil {
			b.Fatalf("failed to remove dir %v", err)
		}
	}()
	db, err := open("test-db", benchmarkTestDirname, opts)
	if err != nil {
		b.Fatalf("failed to open db %v", err)
	}
	defer db.close()

	u := pb.Update{
		ShardID: 100,
		EntriesToSave: []pb.Entry{
			{Cmd: make([]byte, sz)},
		},
	}
	buf := make([]byte, sz*2)
	defer b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sync, err := db.write(u, buf)
		if err != nil {
			b.Fatalf("failed to write %v", err)
		}
		if sync {
			if err := db.sync(); err != nil {
				b.Fatalf("failed to sync %v", err)
			}
		}
		b.SetBytes(int64(sz))
	}
}

func BenchmarkWrite16K(b *testing.B) {
	benchmarkWrite(b, 16*1024)
}

func BenchmarkWrite128K(b *testing.B) {
	benchmarkWrite(b, 128*1024)
}

func BenchmarkWrite512K(b *testing.B) {
	benchmarkWrite(b, 512*1024)
}

func BenchmarkWrite1024K(b *testing.B) {
	benchmarkWrite(b, 1024*1024)
}

func BenchmarkWrite1024KNew(b *testing.B) {
	benchmarkWrite1(b, 1024*1024)
}
