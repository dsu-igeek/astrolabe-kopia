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
	"context"
	"encoding/json"
	"fmt"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/pkg/errors"
	"github.com/vmware-tanzu/astrolabe/pkg/astrolabe"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type ProtectedEntity struct{
	peinfo astrolabe.ProtectedEntityInfo
	repository repo.Repository
	source snapshot.SourceInfo
	rpetm *ProtectedEntityTypeManager
}

func NewProtectedEntityFromJSONBuf(rpetm *ProtectedEntityTypeManager, respository repo.Repository,
	source snapshot.SourceInfo, buf []byte) (pe ProtectedEntity, err error) {
	peii := astrolabe.ProtectedEntityInfoImpl{}
	err = json.Unmarshal(buf, &peii)
	if err != nil {
		return
	}
	pe.peinfo = peii
	pe.rpetm = rpetm
	pe.repository = respository
	pe.source = source
	return
}

func NewProtectedEntityFromJSONReader(rpetm *ProtectedEntityTypeManager, respository repo.Repository,
	source snapshot.SourceInfo, reader io.Reader) (pe ProtectedEntity, err error) {
	decoder := json.NewDecoder(reader)
	peInfo := astrolabe.ProtectedEntityInfoImpl{}
	err = decoder.Decode(&peInfo)
	if err == nil {
		pe.peinfo = peInfo
		pe.rpetm = rpetm
		pe.repository = respository
		pe.source = source
	}
	return
}

func (recv ProtectedEntity) GetInfo(ctx context.Context) (astrolabe.ProtectedEntityInfo, error) {
	return recv.peinfo, nil
}

func (recv ProtectedEntity) GetCombinedInfo(ctx context.Context) ([]astrolabe.ProtectedEntityInfo, error) {
	panic("implement me")
}

func (recv ProtectedEntity) Snapshot(ctx context.Context, params map[string]map[string]interface{}) (astrolabe.ProtectedEntitySnapshotID, error) {
	panic("implement me")
}

func (recv ProtectedEntity) ListSnapshots(ctx context.Context) ([]astrolabe.ProtectedEntitySnapshotID, error) {
	panic("implement me")
}

func (recv ProtectedEntity) DeleteSnapshot(ctx context.Context, snapshotToDelete astrolabe.ProtectedEntitySnapshotID, params map[string]map[string]interface{}) (bool, error) {
	return true, nil  //TODO - implement me
}

func (recv ProtectedEntity) GetInfoForSnapshot(ctx context.Context, snapshotID astrolabe.ProtectedEntitySnapshotID) (*astrolabe.ProtectedEntityInfo, error) {
	panic("implement me")
}

func (recv ProtectedEntity) GetComponents(ctx context.Context) ([]astrolabe.ProtectedEntity, error) {
	panic("implement me")
}

func (recv ProtectedEntity) GetID() astrolabe.ProtectedEntityID {
	return recv.peinfo.GetID()
}

func (recv ProtectedEntity) GetDataReader(ctx context.Context) (io.ReadCloser, error) {
	panic("implement me")
}

func (recv ProtectedEntity) GetMetadataReader(ctx context.Context) (io.ReadCloser, error) {
	panic("implement me")
}

func (recv ProtectedEntity) Overwrite(ctx context.Context, sourcePE astrolabe.ProtectedEntity, params map[string]map[string]interface{}, overwriteComponents bool) error {
	panic("implement me")
}

const maxPEInfoSize int = 16 * 1024

func (recv *ProtectedEntity) copy(ctx context.Context,dataReader io.ReadCloser,
	metadataReader io.ReadCloser) error {
	snapshotCreateDescription := recv.GetID().String() + " upload"
	peEntry, err := NewProtectedEntityKopiaFileWithReaders(ctx, recv, dataReader, metadataReader)
	repoWriter, err := recv.rpetm.repository.NewWriter(ctx, snapshotCreateDescription)
	uploader := setupUploader(repoWriter)

	sourceInfo := snapshot.SourceInfo{
		Host:     "localhost",	// TODO - put something reasonable in here
		UserName: "astrolabe",
		Path:     recv.GetID().GetID(),
	}

	previous, err := findPreviousSnapshotManifest(ctx, repoWriter, sourceInfo, nil)

	policyTree, err := policy.TreeForSource(ctx, repoWriter, sourceInfo)
	if err != nil {
		return errors.Wrap(err, "unable to get policy tree")
	}

	manifest, err := uploader.Upload(ctx, peEntry, policyTree, sourceInfo, previous...)
	if err != nil {
		return errors.Wrap(err, "upload error")
	}

	manifest.Description = snapshotCreateDescription

	snapID, err := snapshot.SaveSnapshot(ctx, repoWriter, manifest)
	if err != nil {
		return errors.Wrap(err, "cannot save manifest")
	}

	fmt.Printf("snapID = %s\n", snapID)
	if _, err = policy.ApplyRetentionPolicy(ctx, repoWriter, sourceInfo, true); err != nil {
		return errors.Wrap(err, "unable to apply retention policy")
	}

	if err = repoWriter.Flush(ctx); err != nil {
		return errors.Wrap(err, "flush error")
	}

	//defer recv.cleanupOnAbortedUpload(&ctx)
	//peInfo := recv.peinfo
	//peinfoName := recv.rpetm.peinfoName(peInfo.GetID())


	/*
	peInfoBuf, err := json.Marshal(peInfo)
	if err != nil {
		return err
	}
	if len(peInfoBuf) > maxPEInfoSize {
		return errors.New("JSON for pe info > 16K")
	}

	// TODO: defer the clean up of disk handle of source PE's data reader
	if dataReader != nil {
		dataName := recv.rpetm.dataName(peInfo.GetID())
		snapshot.
		err = recv.uploadStream(ctx, dataName, maxSegmentSize, dataReader)
		if err != nil {
			return err
		}
	}

	if metadataReader != nil {
		mdName := recv.rpetm.metadataName(peInfo.GetID())
		err = recv.uploadStream(ctx, mdName, maxSegmentSize, metadataReader)
		if err != nil {
			return err
		}
	}
	jsonBytes := bytes.NewReader(peInfoBuf)

	jsonParams := &s3.PutObjectInput{
		Bucket:        aws.String(recv.rpetm.bucket),
		Key:           aws.String(peinfoName),
		Body:          jsonBytes,
		ContentLength: aws.Int64(int64(len(peInfoBuf))),
		ContentType:   aws.String(peInfoFileType),
	}
	_, err = recv.rpetm.s3.PutObjectWithContext(ctx, jsonParams)
	if err != nil {
		return errors.Wrapf(err, "copy S3 PutObject for PE info failed for PE %s bucket %s key %s",
			peInfo.GetID(), recv.rpetm.bucket, peinfoName)
	}

	 */
	return err
}

func setupUploader(rep repo.RepositoryWriter) *snapshotfs.Uploader {
	u := snapshotfs.NewUploader(rep)
	u.MaxUploadBytes = 0	// No limit


	u.ForceHashPercentage = 0
	u.ParallelUploads = 0

	u.Progress = &peProgress{}

	return u
}

type peProgress struct {
	snapshotfs.NullUploadProgress

	// all int64 must precede all int32 due to alignment requirements on ARM
	uploadedBytes          int64
	cachedBytes            int64
	hashedBytes            int64
	nextOutputTimeUnixNano int64

	cachedFiles       int32
	inProgressHashing int32
	hashedFiles       int32
	uploadedFiles     int32
	errorCount        int32

	uploading      int32
	uploadFinished int32

	lastLineLength  int
	spinPhase       int
	uploadStartTime time.Time

	estimatedFileCount  int
	estimatedTotalBytes int64

	// indicates shared instance that does not reset counters at the beginning of upload.
	shared bool

	outputMutex sync.Mutex
}

func (recv *peProgress) HashingFile(fname string) {
	atomic.AddInt32(&recv.inProgressHashing, 1)
}

func (recv *peProgress) FinishedHashingFile(fname string, totalSize int64) {
	atomic.AddInt32(&recv.hashedFiles, 1)
	atomic.AddInt32(&recv.inProgressHashing, -1)
}

func (recv *peProgress) UploadedBytes(numBytes int64) {
	atomic.AddInt64(&recv.uploadedBytes, numBytes)
	atomic.AddInt32(&recv.uploadedFiles, 1)

}

func (recv *peProgress) HashedBytes(numBytes int64) {
	atomic.AddInt64(&recv.hashedBytes, numBytes)
}

func (recv *peProgress) IgnoredError(path string, err error) {
	atomic.AddInt32(&recv.errorCount, 1)
}

func (recv *peProgress) CachedFile(fname string, numBytes int64) {
	atomic.AddInt64(&recv.cachedBytes, numBytes)
	atomic.AddInt32(&recv.cachedFiles, 1)
}

// findPreviousSnapshotManifest returns the list of previous snapshots for a given source, including
// last complete snapshot and possibly some number of incomplete snapshots following it.
func findPreviousSnapshotManifest(ctx context.Context, rep repo.Repository, sourceInfo snapshot.SourceInfo, noLaterThan *time.Time) ([]*snapshot.Manifest, error) {
	man, err := snapshot.ListSnapshots(ctx, rep, sourceInfo)
	if err != nil {
		return nil, errors.Wrap(err, "error listing previous snapshots")
	}

	// phase 1 - find latest complete snapshot.
	var previousComplete *snapshot.Manifest

	var previousCompleteStartTime time.Time

	var result []*snapshot.Manifest

	for _, p := range man {
		if noLaterThan != nil && p.StartTime.After(*noLaterThan) {
			continue
		}

		if p.IncompleteReason == "" && (previousComplete == nil || p.StartTime.After(previousComplete.StartTime)) {
			previousComplete = p
			previousCompleteStartTime = p.StartTime
		}
	}

	if previousComplete != nil {
		result = append(result, previousComplete)
	}

	// add all incomplete snapshots after that
	for _, p := range man {
		if noLaterThan != nil && p.StartTime.After(*noLaterThan) {
			continue
		}

		if p.IncompleteReason != "" && p.StartTime.After(previousCompleteStartTime) {
			result = append(result, p)
		}
	}

	return result, nil
}