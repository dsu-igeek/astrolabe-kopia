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
	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	logger   logrus.FieldLogger

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
	pe.logger = rpetm.logger
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
		pe.logger = rpetm.logger
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
	sourceInfo := getSourceInfo(recv.GetID())
	snapshotManifests, err := snapshot.ListSnapshots(ctx, recv.repository, sourceInfo)
	if err != nil {
		return nil, errors.WithMessagef(err, "error listing kopia snapshots for %s", sourceInfo.Path)
	}
	var snapshotIDs []astrolabe.ProtectedEntitySnapshotID
	for _, snapshotManifest := range snapshotManifests {
		curID, err := astrolabe.NewProtectedEntityIDFromString(snapshotManifest.Description)
		if err != nil {
			recv.logger.Errorf("Could not parse description '%s' from snapshot id %s as Protected Entity ID", snapshotManifest.Description,
				snapshotManifest.ID)
		} else {
			if curID.HasSnapshot() {
				snapshotIDs = append(snapshotIDs, curID.GetSnapshotID())
			} else {
				recv.logger.Errorf("Protected Entity ID '%s' from snapshot id %s doest not have a snapshot ID", curID.String(),
					snapshotManifest.ID)
			}
		}
	}
	return snapshotIDs, nil
}

func (recv ProtectedEntity) DeleteSnapshot(ctx context.Context, snapshotToDelete astrolabe.ProtectedEntitySnapshotID, params map[string]map[string]interface{}) (bool, error) {
	return true, nil  //TODO - implement me
}

func (recv ProtectedEntity) GetInfoForSnapshot(ctx context.Context, snapshotID astrolabe.ProtectedEntitySnapshotID) (*astrolabe.ProtectedEntityInfo, error) {
	panic("implement me")
}

func (recv ProtectedEntity) GetComponents(ctx context.Context) ([]astrolabe.ProtectedEntity, error) {
	componentIDs := recv.peinfo.GetComponentIDs()
	components := make([]astrolabe.ProtectedEntity, len(componentIDs))
	/*
		for curComponentIDNum, curComponentID := range componentIDs {
			components[curComponentID] = recv.rpetm.
		}
	*/
	return components, nil
}

func (recv ProtectedEntity) GetID() astrolabe.ProtectedEntityID {
	return recv.peinfo.GetID()
}

func (recv ProtectedEntity) GetDataReader(ctx context.Context) (io.ReadCloser, error) {
	id := recv.GetID()
	if ! id.HasSnapshot() {
		return nil, errors.New("Must be a snapshot")
	}
	snapshotID := id.GetSnapshotID()
	manifest, err := recv.getManifestForSnapshot(ctx, snapshotID)
	if err != nil {
		return nil, err
	}
	root, err := snapshotfs.EntryFromDirEntry(recv.repository, manifest.RootEntry)
	dataEntry, err := root.(fs.Directory).Child(ctx, recv.rpetm.dataName(recv.GetID()))
	if err != nil {
		return nil, errors.WithMessage(err,"Error retrieveing data entry")
	}
	dataFile, ok := dataEntry.(fs.File)
	if !ok {
		return nil, errors.Errorf("Entry %s is not a file", dataEntry.Name())
	}
	dataReader, err := dataFile.Open(ctx)
	if err != nil {
		return nil, errors.WithMessagef(err, "Could not get data reader")
	}
	return dataReader, nil
}

func (recv ProtectedEntity) GetMetadataReader(ctx context.Context) (io.ReadCloser, error) {
	id := recv.GetID()
	if ! id.HasSnapshot() {
		return nil, errors.New("Must be a snapshot")
	}
	snapshotID := id.GetSnapshotID()
	manifest, err := recv.getManifestForSnapshot(ctx, snapshotID)
	if err != nil {
		return nil, err
	}
	root, err := snapshotfs.EntryFromDirEntry(recv.repository, manifest.RootEntry)
	metadataEntry, err := root.(fs.Directory).Child(ctx, recv.rpetm.metadataName(recv.GetID()))
	if err != nil {
		return nil, errors.WithMessage(err,"Error retrieveing data entry")
	}
	metadataFile, ok := metadataEntry.(fs.File)
	if !ok {
		return nil, errors.Errorf("Entry %s is not a file", metadataEntry.Name())
	}
	metadataReader, err := metadataFile.Open(ctx)
	if err != nil {
		return nil, errors.WithMessagef(err, "Could not get metadata reader")
	}
	return metadataReader, nil}

func (recv ProtectedEntity) Overwrite(ctx context.Context, sourcePE astrolabe.ProtectedEntity, params map[string]map[string]interface{}, overwriteComponents bool) error {
	panic("implement me")
}

const maxPEInfoSize int = 16 * 1024

func getSourceInfo(id astrolabe.ProtectedEntityID) snapshot.SourceInfo {
	return snapshot.SourceInfo{
		Host:     "localhost",	// TODO - put something reasonable in here
		UserName: "astrolabe",
		Path:     id.GetBaseID().String(),
	}
}
func (recv ProtectedEntity) getManifestForSnapshot(ctx context.Context, snapshotID astrolabe.ProtectedEntitySnapshotID) (*snapshot.Manifest, error) {
	sourceInfo := getSourceInfo(recv.GetID())
	snapshotManifests, err := snapshot.ListSnapshots(ctx, recv.repository, sourceInfo)
	if err != nil {
		return nil, errors.WithMessagef(err, "error listing kopia snapshots for %s", sourceInfo.Path)
	}
	for _, snapshotManifest := range snapshotManifests {
		curID, err := astrolabe.NewProtectedEntityIDFromString(snapshotManifest.Description)
		if err != nil {
			recv.logger.Errorf("Could not parse description '%s' from snapshot id %s as Protected Entity ID", snapshotManifest.Description,
				snapshotManifest.ID)
		} else {
			if curID.HasSnapshot() {
				if curID == recv.GetID().IDWithSnapshot(snapshotID) {
					return snapshotManifest, nil
				}
			} else {
				recv.logger.Errorf("Protected Entity ID '%s' from snapshot id %s doest not have a snapshot ID", curID.String(),
					snapshotManifest.ID)
			}
		}
	}
	return nil, errors.Errorf("Snapshot ID %s not found", snapshotID.String())
}
func (recv *ProtectedEntity) copy(ctx context.Context,dataReader io.ReadCloser,
	metadataReader io.ReadCloser) error {
	snapshotCreateDescription := recv.GetID().String()

	peEntry, err := NewProtectedEntityKopiaFileWithReaders(ctx, recv, dataReader, metadataReader)
	repoWriter, err := recv.rpetm.repository.NewWriter(ctx, snapshotCreateDescription)
	uploader := setupUploader(repoWriter)

	sourceInfo := getSourceInfo(recv.GetID())

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

	if err = repoWriter.Close(ctx); err != nil {
		return errors.Wrap(err, "flush error")
	}
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
	snapshotManifests, err := snapshot.ListSnapshots(ctx, rep, sourceInfo)
	if err != nil {
		return nil, errors.Wrap(err, "error listing previous snapshots")
	}

	// phase 1 - find latest complete snapshot.
	var previousComplete *snapshot.Manifest

	var previousCompleteStartTime time.Time

	var result []*snapshot.Manifest

	for _, snapshotManifest := range snapshotManifests {
		if noLaterThan != nil && snapshotManifest.StartTime.After(*noLaterThan) {
			continue
		}

		if snapshotManifest.IncompleteReason == "" && (previousComplete == nil || snapshotManifest.StartTime.After(previousComplete.StartTime)) {
			previousComplete = snapshotManifest
			previousCompleteStartTime = snapshotManifest.StartTime
		}
	}

	if previousComplete != nil {
		result = append(result, previousComplete)
	}

	// add all incomplete snapshots after that
	for _, p := range snapshotManifests {
		if noLaterThan != nil && p.StartTime.After(*noLaterThan) {
			continue
		}

		if p.IncompleteReason != "" && p.StartTime.After(previousCompleteStartTime) {
			result = append(result, p)
		}
	}

	return result, nil
}