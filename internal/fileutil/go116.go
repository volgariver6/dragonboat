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

// +build go1.16

package fileutil

import (
	"io"
	"os"
)

// Discard ...
var Discard = io.Discard

// CreateTemp ...
func CreateTemp(dir string, pattern string) (*os.File, error) {
	return os.CreateTemp(dir, pattern)
}

// ReadAll ...
func ReadAll(r io.Reader) ([]byte, error) {
	return io.ReadAll(r)
}

// MkdirTemp ...
func MkdirTemp(dir string, pattern string) (string, error) {
	return os.MkdirTemp(dir, pattern)
}

// ReadFile ...
func ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}