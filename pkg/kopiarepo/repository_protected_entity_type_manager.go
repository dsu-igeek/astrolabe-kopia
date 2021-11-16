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
	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/astrolabe/pkg/astrolabe"
	"io"
)

type ProtectedEntityTypeManager struct {
	typeName string
	repository repo.Repository
	objectPrefix, peinfoPrefix, mdPrefix, dataPrefix string
	logger                                           logrus.FieldLogger
}

func NewProtectedEntityTypeManager(ctx context.Context, typeName string, repository repo.Repository,
	logger logrus.FieldLogger) (ProtectedEntityTypeManager, error) {
	return ProtectedEntityTypeManager{
		typeName: typeName,
		repository: repository,
		objectPrefix: "pe-",
		peinfoPrefix: "peinfo-",
		mdPrefix: "md-",
		dataPrefix: "data-",
		logger: logger,
	}, nil
}
func (recv ProtectedEntityTypeManager) GetTypeName() string {
	return recv.typeName
}

func (recv ProtectedEntityTypeManager) GetProtectedEntity(ctx context.Context, id astrolabe.ProtectedEntityID) (astrolabe.ProtectedEntity, error) {
	source := getSourceInfo(id)

	snapshots, err := snapshot.ListSnapshots(ctx, recv.repository, source)
	if err != nil {
		return nil, errors.WithMessage(err, "Cannot list snapshots")
	}
	for _, snapshot := range snapshots {
		descriptionID, err := astrolabe.NewProtectedEntityIDFromString(snapshot.Description)
		if err != nil {
			continue;	// Malformed description
		}
		var checkPEID astrolabe.ProtectedEntityID
		// In the Kopia repo, every entry has a snapshot.  If there is no snapshot, we'll just take the first PEInfo we
		// find with the same base ID
		if id.HasSnapshot() {
			checkPEID = descriptionID
		} else {
			checkPEID = descriptionID.GetBaseID()
		}
		if id == checkPEID {
			root, err := snapshotfs.SnapshotRoot(recv.repository, snapshot)
			if err != nil {
				return nil, errors.WithMessage(err, "Cannot get root")
			}

			rootDir, ok := root.(fs.Directory)
			if !ok {
				return nil, errors.WithMessagef(err, "Cannot get rootdir for snapshot %s", id.GetSnapshotID().String())
			}
			peinfoName := recv.peinfoName(descriptionID)
			peInfoEntry, err := rootDir.Child(ctx, peinfoName)
			if err != nil {
				return nil, errors.WithMessagef(err, "Cannot get peinfo %s", peinfoName)
			}
			peInfoFile, ok := peInfoEntry.(fs.File)
			if !ok {
				return nil, errors.WithMessagef(err, "peinfo %s is not a file", peinfoName)
			}
			peInfoReader, err := peInfoFile.Open(ctx)
			if err != nil {
				return nil, errors.WithMessagef(err, "Cannot open peinfo %s", peinfoName)
			}
			returnPE, err := NewProtectedEntityFromJSONReader(&recv, recv.repository, source, peInfoReader)
			if err != nil {
				return nil, errors.Wrapf(err, "NewProtectedEntityFromJSONReader failed for %s", id.String())
			}
			// TODO - if there's no snapshot specified we should clear the snapshot ID from the returned PE
			return returnPE, nil
		}
	}

	return nil, errors.Errorf("Could not find PE %s", id.String())
}

func (recv ProtectedEntityTypeManager) GetProtectedEntities(ctx context.Context) ([]astrolabe.ProtectedEntityID, error) {
	sources, err := snapshot.ListSources(ctx, recv.repository)
	if err != nil {
		return nil, errors.WithMessage(err, "Cannot list PEs in repo")
	}
	returnIDs := []astrolabe.ProtectedEntityID{}
	for _, source := range sources {
		curPEID, err := astrolabe.NewProtectedEntityIDFromString(source.Path)
		if err != nil {

		} else {
			returnIDs = append(returnIDs, curPEID)
		}
	}

	return returnIDs, nil
}

func (recv ProtectedEntityTypeManager) Copy(ctx context.Context, sourcePE astrolabe.ProtectedEntity, params map[string]map[string]interface{},
	options astrolabe.CopyCreateOptions) (astrolabe.ProtectedEntity, error) {
	sourcePEInfo, err := sourcePE.GetInfo(ctx)
	if err != nil {
		return nil, err
	}
	dataReader, err := sourcePE.GetDataReader(ctx)
	if dataReader != nil {
		defer func() {
			if err := dataReader.Close(); err != nil {
				recv.logger.Errorf("The deferred data reader is closed with error, %v", err)
			}
		}()
	}

	if err != nil {
		return nil, err
	}

	metadataReader, err := sourcePE.GetMetadataReader(ctx)
	if err != nil {
		return nil, err
	}
	return recv.copyInt(ctx, sourcePEInfo, options, dataReader, metadataReader)
}

func (recv ProtectedEntityTypeManager) CopyFromInfo(ctx context.Context, info astrolabe.ProtectedEntityInfo, params map[string]map[string]interface{}, options astrolabe.CopyCreateOptions) (astrolabe.ProtectedEntity, error) {
	panic("implement me")
}

func (this ProtectedEntityTypeManager) copyInt(ctx context.Context, sourcePEInfo astrolabe.ProtectedEntityInfo,
	options astrolabe.CopyCreateOptions, dataReader io.ReadCloser, metadataReader io.ReadCloser) (astrolabe.ProtectedEntity, error) {
	id := sourcePEInfo.GetID()
	if id.GetPeType() != this.typeName {
		return nil, errors.New(id.GetPeType() + " is not of type " + this.typeName)
	}
	if options == astrolabe.AllocateObjectWithID {
		return nil, errors.New("AllocateObjectWithID not supported")
	}

	if options == astrolabe.UpdateExistingObject {
		return nil, errors.New("UpdateExistingObject not supported")
	}

	_, err := this.GetProtectedEntity(ctx, id)
	if err == nil {
		return nil, errors.New("id " + id.String() + " already exists")
	}

	var dataTransports []astrolabe.DataTransport
	if len(sourcePEInfo.GetDataTransports()) > 0 {
		dataTransports, err = this.dataTransportsForID(id)
		if err != nil {
			return nil, err
		}
	} else {
		dataTransports = []astrolabe.DataTransport{}
	}

	var metadataTransports []astrolabe.DataTransport
	if len(sourcePEInfo.GetMetadataTransports()) > 0 {
		metadataTransports, err = this.metadataTransportsForID(id)
		if err != nil {
			return nil, err
		}
	} else {
		metadataTransports = []astrolabe.DataTransport{}
	}

	combinedTransports := []astrolabe.DataTransport{}

	rPEInfo := astrolabe.NewProtectedEntityInfo(sourcePEInfo.GetID(), sourcePEInfo.GetName(),
		sourcePEInfo.GetSize(),	dataTransports, metadataTransports, combinedTransports, sourcePEInfo.GetComponentIDs())

	rpe := ProtectedEntity{
		rpetm:  &this,
		peinfo: rPEInfo,
	}

	_, err = rpe.DeleteSnapshot(ctx, id.GetSnapshotID(), make(map[string]map[string]interface{}))

	err = rpe.copy(ctx, dataReader, metadataReader)
	if err != nil {
		return nil, err
	}
	return rpe, nil
}

func (recv ProtectedEntityTypeManager) Delete(ctx context.Context, id astrolabe.ProtectedEntityID) error {
	panic("implement me")
}

const MD_SUFFIX = ".md"
const DATA_SUFFIX = ".data"

func (recv ProtectedEntityTypeManager) peinfoName(id astrolabe.ProtectedEntityID) string {
	if !id.HasSnapshot() {
		panic("Cannot store objects that do not have snapshots")
	}
	return recv.peinfoPrefix + id.String()
}

func (recv ProtectedEntityTypeManager) metadataName(id astrolabe.ProtectedEntityID) string {
	if !id.HasSnapshot() {
		panic("Cannot store objects that do not have snapshots")
	}
	return recv.mdPrefix + id.String() + MD_SUFFIX
}

func (recv ProtectedEntityTypeManager) dataName(id astrolabe.ProtectedEntityID) string {
	if !id.HasSnapshot() {
		panic("Cannot store objects that do not have snapshots")
	}
	return recv.mdPrefix + id.String() + DATA_SUFFIX
}

func (this *ProtectedEntityTypeManager) dataTransportsForID(id astrolabe.ProtectedEntityID) ([]astrolabe.DataTransport, error) {
	return []astrolabe.DataTransport{}, nil
}

func (this *ProtectedEntityTypeManager) metadataTransportsForID(id astrolabe.ProtectedEntityID) ([]astrolabe.DataTransport, error) {
	return []astrolabe.DataTransport{}, nil
}

func (this ProtectedEntityTypeManager) GetCapabilities() map[string]string {
	return map[string]string{}	// We don't have any capabilities for the Kopia repos
}
