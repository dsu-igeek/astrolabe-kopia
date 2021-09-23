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
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/blob/s3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/astrolabe/pkg/astrolabe"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

type fsStorageFactory struct {
	pesDir string
}

func (recv fsStorageFactory) getStorage(peTypeName string, create bool) (fsStorage blob.Storage, err error) {
	storagePath := recv.pesDir + string(os.PathSeparator) + peTypeName
	if create {
		_, err = os.Stat(storagePath)
		if os.IsNotExist(err) {
			err = os.Mkdir(storagePath, 0755)
			if err != nil {
				err = errors.WithMessagef(err, "Failed making storage dir for type %s", peTypeName);
				return
			}
		}
	}
	fsStorage, err = filesystem.New(context.TODO(), &filesystem.Options{
		Path: storagePath,
	})

	return
}


type s3StorageFactory struct {
	s3.Options
}

func (recv s3StorageFactory) getStorage(peTypeName string, create bool) (s3Storage blob.Storage, err error) {
	peOptions := recv.Options
	peOptions.Prefix = peOptions.Prefix + "/" + peTypeName + "/"
	return s3.New(context.Background(), &peOptions)
}


type storageFactory interface {
	getStorage(peTypeName string, create bool) (storage blob.Storage, err error)
}

type RepositoryProtectedEntityManager struct {
	petms map[string]astrolabe.ProtectedEntityTypeManager
	repoDir string
	configFilesDir string
	storageFactory storageFactory
	logger logrus.FieldLogger
	mutex   sync.Mutex
}

const CONFIG_FILE_EXT = ".kopia"
const PE_FS_STORAGE_DIR = "pes"
const CONFIG_DIR = "configs"
const PE_S3CONFIG_FILE = "s3_config.json"

func NewKopiaRepositoryProtectedEntityManager(repoDir string, logger logrus.FieldLogger) (astrolabe.ProtectedEntityManager, error) {
	repoDirInfo, err := os.Stat(repoDir)
	if err != nil {
		return nil, err
	}
	if !repoDirInfo.IsDir() {
		return nil, errors.Errorf("repoDir %s is not a directory", repoDir)
	}

	s3ConfigFile := repoDir + string(os.PathSeparator) + PE_S3CONFIG_FILE
	s3ConfigFileInfo, err := os.Stat(s3ConfigFile)

	var repoStorageFactory storageFactory
	if err == nil {
		if s3ConfigFileInfo.IsDir() {
			return nil, errors.Errorf("%s is a directory", s3ConfigFile)
		}
		s3ConfigFile, err := os.Open(s3ConfigFile)
		if err != nil {
			return nil, err
		}
		defer s3ConfigFile.Close()
		jsonBytes, err := ioutil.ReadAll(s3ConfigFile)
		if err != nil {
			return nil, err
		}
		s3Options := s3.Options{}
		err = json.Unmarshal(jsonBytes, &s3Options)
		if err != nil {
			return nil, err
		}
		repoStorageFactory = s3StorageFactory{
			Options: s3Options,
		}
	} else {
		pesDir := repoDir + string(os.PathSeparator) + PE_FS_STORAGE_DIR
		pesDirInfo, err := os.Stat(pesDir)

		if err != nil {
			if os.IsNotExist(err) {
				err = os.Mkdir(pesDir, 0755)
				if err != nil {
					return nil, err
				}
			}
		} else {
			if !pesDirInfo.IsDir() {
				return nil, errors.Errorf("pesDir %s is not a directory", pesDir)
			}
		}

		// TODO - initialize storage info
		repoStorageFactory = fsStorageFactory{
			pesDir: pesDir,
		}
	}
	configFilesDir := repoDir + string(os.PathSeparator) + CONFIG_DIR
	configFilesDirInfo, err := os.Stat(configFilesDir)

	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(configFilesDir, 0755)
			if err != nil {
				return nil, err
			}
		}
	} else {
		if !configFilesDirInfo.IsDir() {
			return nil, errors.Errorf("configFilesDir %s is not a directory", configFilesDir)
		}
	}

	petms := make(map[string]astrolabe.ProtectedEntityTypeManager, 0)
	configFilesDirFile, err := os.Open(configFilesDir)
	configFiles, err := configFilesDirFile.Readdirnames(0)
	for _, curConfigFile := range configFiles {
		if strings.HasSuffix(curConfigFile, CONFIG_FILE_EXT) {
			typeName := curConfigFile[0:len(curConfigFile) - len(CONFIG_FILE_EXT)]
			storage, err := repoStorageFactory.getStorage(typeName, false)
			if err != nil {
				// TODO - log the error
			}
			typePETM, err := setupPETM(context.TODO(), typeName, curConfigFile, storage, false, logger)
			if err != nil {
				// TODO - log the error
				continue;
			}
			petms[typeName] = typePETM
		}
	}

	returnPEM := RepositoryProtectedEntityManager{
		petms:          petms,
		repoDir:        repoDir,
		configFilesDir: configFilesDir,
		storageFactory:    repoStorageFactory,
		logger: logger,
	}

	return returnPEM, nil
}

func setupPETM(context context.Context, typeName string, configFile string,
	storage blob.Storage, create bool, logger logrus.FieldLogger) (astrolabe.ProtectedEntityTypeManager, error) {
	masterPassword := "astrolabe"

	err := repo.Connect(context, configFile, storage, masterPassword, nil)
	if err != nil {
		if create {
			if err := repo.Initialize(context, storage, nil, masterPassword); err != nil {
				return nil, errors.Wrap(err, "cannot initialize repository")
			}

		}
		err = repo.Connect(context, configFile, storage, masterPassword, nil)
		if err != nil {
			return nil, err
		}
	}
	openOpt := &repo.Options{}

	petmRepo, err := repo.Open(context, configFile, masterPassword, openOpt)

	if err != nil {
		return nil, err
	}
	return NewProtectedEntityTypeManager(context, typeName, petmRepo, logger)
}

func (recv RepositoryProtectedEntityManager) GetProtectedEntity(ctx context.Context, id astrolabe.ProtectedEntityID) (astrolabe.ProtectedEntity, error) {
	petm := recv.getProtectedEntityTypeManagerInt(id.GetPeType(), false)
	if petm == nil {
		return nil, errors.Errorf("No PETM for type %s, cannot retrieve ID %s", id.GetPeType(), id.String())
	}
	return petm.GetProtectedEntity(ctx, id)
}

func (recv RepositoryProtectedEntityManager) GetProtectedEntityTypeManager(peType string) astrolabe.ProtectedEntityTypeManager {
	return recv.getProtectedEntityTypeManagerInt(peType, true)
}

func (recv RepositoryProtectedEntityManager) getProtectedEntityTypeManagerInt(peType string, create bool) astrolabe.ProtectedEntityTypeManager {
	recv.mutex.Lock()
	defer recv.mutex.Unlock()
	returnPETM := recv.petms[peType]
	if returnPETM == nil {
		var err error
		configFile := recv.configFilesDir + string(os.PathSeparator) + peType + CONFIG_FILE_EXT
		storage, err := recv.storageFactory.getStorage(peType, create)
		if err == nil {
			returnPETM, err = setupPETM(context.TODO(), peType, configFile, storage, create, recv.logger)
			if err != nil {
				recv.logger.Errorf("Could not create Kopia RepositoryProtectedEntityTypeManager for type %s, err = %v", peType, err)
			}
		} else {
			recv.logger.Errorf("Could not retrieve storage for Kopia RepositoryProtectedEntityTypeManager for type %s, err = %v", peType, err)
		}
	}
	return returnPETM
}

func (recv RepositoryProtectedEntityManager) ListEntityTypeManagers() []astrolabe.ProtectedEntityTypeManager {
	recv.mutex.Lock()
	defer recv.mutex.Unlock()
	returnPETMs := make([]astrolabe.ProtectedEntityTypeManager, len(recv.petms))
	curPETMNum := 0
	for _, curPETM := range recv.petms {
		returnPETMs[curPETMNum] = curPETM
		curPETMNum++
	}
	return returnPETMs
}
