/*
 * Copyright 2021 the Astrolabe contributors
 * SPDX-License-Identifier: Apache-2.0
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kopiarepo

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/kopia/kopia/fs"
	"github.com/pkg/errors"
	"github.com/vmware-tanzu/astrolabe/pkg/astrolabe"
	"io"
	"io/ioutil"
	"os"
	"time"
)

type ProtectedEntityKopiaEntry struct {

}


type ProtectedEntityKopiaFile struct {
	pe astrolabe.ProtectedEntity
	entries []fs.Entry
}

type DummyReadSeekerCloser struct {
	readCloser io.ReadCloser
}

func (recv DummyReadSeekerCloser) Read(buf []byte) (n int, err error) {
	return recv.readCloser.Read(buf)
}

func (recv DummyReadSeekerCloser) Close() error {
	return recv.readCloser.Close()
}

func (recv DummyReadSeekerCloser) Seek(offset int64, whence int) (int64, error) {
	panic("implement me")
}

func NewProtectedEntityKopiaFile(ctx context.Context, pe *ProtectedEntity) (ProtectedEntityKopiaFile, error) {
	dataReader, err := pe.GetDataReader(ctx)
	if err != nil {
		return ProtectedEntityKopiaFile{}, errors.WithMessage(err, "Could not retrieve data reader")
	}
	metadataReader, err := pe.GetMetadataReader(ctx)
	if err != nil {
		return ProtectedEntityKopiaFile{}, errors.WithMessage(err, "Could not retrieve metadata reader")
	}
	return NewProtectedEntityKopiaFileWithReaders(ctx, pe, dataReader, metadataReader)
}

func NewProtectedEntityKopiaFileWithReaders(ctx context.Context, pe * ProtectedEntity, dataReader io.ReadCloser,
	metadataReader io.ReadCloser) (ProtectedEntityKopiaFile, error) {
	var entries []fs.Entry

	peInfo, err := pe.GetInfo(ctx)
	if err != nil {
		return ProtectedEntityKopiaFile{}, errors.WithMessage(err, "Could not retrieve peinfo")
	}
	peInfoBuf, err := json.Marshal(peInfo)
	if err != nil {
		return ProtectedEntityKopiaFile{}, errors.Wrap(err, "Could not marshal to JSON")
	}
	if len(peInfoBuf) > maxPEInfoSize {
		return ProtectedEntityKopiaFile{}, errors.New("JSON for pe info > 16K")
	}
	entries = append(entries, NewStreamEntry(pe.rpetm.peinfoName(pe.GetID()), int64(len(peInfoBuf)), time.Now(),
		ioutil.NopCloser(bytes.NewReader(peInfoBuf)), pe.rpetm.peinfoName(pe.GetID())))
	if dataReader != nil {
		entries = append(entries, NewStreamEntry(pe.rpetm.dataName(pe.GetID()), pe.peinfo.GetSize(), time.Now(),
			dataReader, pe.rpetm.dataName(pe.GetID())))
	}
	if metadataReader != nil {
		entries = append(entries, NewStreamEntry(pe.rpetm.metadataName(pe.GetID()), pe.peinfo.GetSize(), time.Now(),
			metadataReader, pe.rpetm.metadataName(pe.GetID())))
	}
	return ProtectedEntityKopiaFile{
		pe:pe,
		entries: entries,
	}, nil

}

func (recv ProtectedEntityKopiaFile) Name() string {
	return recv.pe.GetID().GetID()
}

func (recv ProtectedEntityKopiaFile) Size() int64 {
	return 1;	// TODO - calculate directory size if necessary
}

func (recv ProtectedEntityKopiaFile) Mode() os.FileMode {
	return os.ModeDir
}

func (recv ProtectedEntityKopiaFile) ModTime() time.Time {
	return time.Now()	// Should return snapshot time
}

func (recv ProtectedEntityKopiaFile) IsDir() bool {
	return true
}

func (recv ProtectedEntityKopiaFile) Sys() interface{} {
	return nil
}

func (recv ProtectedEntityKopiaFile) Owner() fs.OwnerInfo {
	return fs.OwnerInfo{
		UserID:  0,
		GroupID: 0,
	}
}

func (recv ProtectedEntityKopiaFile) Device() fs.DeviceInfo {
	return fs.DeviceInfo{}
}

func (recv ProtectedEntityKopiaFile) LocalFilesystemPath() string {
	return "/" + recv.Name()
}

func (recv ProtectedEntityKopiaFile) Child(ctx context.Context, name string) (fs.Entry, error) {
	for _, checkEntry := range recv.entries {
		if checkEntry.Name() == name {
			return checkEntry, nil
		}
	}
	return nil, errors.Errorf("Could not find %s", name)
}

func (recv ProtectedEntityKopiaFile) Readdir(ctx context.Context) (fs.Entries, error) {
	return recv.entries, nil
}

type StreamEntry struct {
	name string
	size int64
	modTime time.Time
	reader io.ReadCloser
	path string
}

func NewStreamEntry(name string, size int64, modTime time.Time, reader io.ReadCloser, path string) (StreamEntry) {
	readSeeker, ok := reader.(io.ReadSeeker)
	if !ok {
		readSeeker = DummyReadSeekerCloser{readCloser: reader}
	}
	return StreamEntry{
		name:    name,
		size:    size,
		modTime: modTime,
		reader:  readSeeker.(io.ReadCloser),
		path:    path,
	}
}

func (recv StreamEntry) Name() string {
	return recv.name
}

func (recv StreamEntry) Size() int64 {
	return recv.size
}

func (recv StreamEntry) Mode() os.FileMode {
	return os.ModePerm
}

func (recv StreamEntry) ModTime() time.Time {
	return recv.modTime
}

func (recv StreamEntry) IsDir() bool {
	return false
}

func (recv StreamEntry) Sys() interface{} {
	return nil
}

func (recv StreamEntry) Owner() fs.OwnerInfo {
	return fs.OwnerInfo{
		UserID:  0,
		GroupID: 0,
	}
}

func (recv StreamEntry) Device() fs.DeviceInfo {
	return fs.DeviceInfo{
		Dev:  0,
		Rdev: 0,
	}
}

func (recv StreamEntry) LocalFilesystemPath() string {
	return recv.path
}

func (recv StreamEntry) Open(ctx context.Context) (fs.Reader, error) {
	return NewStreamReader(recv.reader, recv)
}

type StreamReader struct {
	entry fs.Entry
	reader io.ReadCloser
	seeker io.ReadSeeker
}

func NewStreamReader(reader io.ReadCloser, entry fs.Entry) (StreamReader, error) {
	rs, ok := reader.(io.ReadSeeker)
	if ! ok {
		return StreamReader{}, errors.New("reader must also be an io.ReadSeeker")
	}
	return StreamReader{
		entry:  entry,
		reader: reader,
		seeker: rs,
	}, nil
}
func (recv StreamReader) Read(p []byte) (n int, err error) {
	return recv.reader.Read(p)
}

func (recv StreamReader) Close() error {
	return recv.reader.Close()
}

func (recv StreamReader) Seek(offset int64, whence int) (int64, error) {
	return recv.seeker.Seek(offset, whence)
}

func (recv StreamReader) Entry() (fs.Entry, error) {
	return recv.entry, nil
}
