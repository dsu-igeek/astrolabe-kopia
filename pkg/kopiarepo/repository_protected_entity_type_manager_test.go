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
	"github.com/google/uuid"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/blob/s3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/astrolabe/pkg/astrolabe"
	astrolabefs "github.com/vmware-tanzu/astrolabe/pkg/fs"
	"io"
	"net"
	"os"
	"testing"
)

func TestS3(t *testing.T) {
	ctx := context.Background()

	repoDir, err := os.MkdirTemp("", "kopia-test-s3-repo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	options := s3.Options{
		BucketName:                     "dsu-kopia",
		Prefix:                         "test",
		AccessKeyID:                    "",
		SecretAccessKey:                "",
		Region:                         "us-east-1",
		Endpoint:                       "s3.us-east-1.amazonaws.com",
	}
	repoConfFilePath := repoDir + string(os.PathSeparator) + PE_S3CONFIG_FILE

	repoConfFile, err := os.Create(repoConfFilePath)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	optionsBytes, err := json.Marshal(options)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	bytesWritten, err := repoConfFile.Write(optionsBytes)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if bytesWritten != len(optionsBytes) {
		t.Fatalf("OptionsBytes len = %d, wrote %d", len(optionsBytes), bytesWritten)
	}
	repoConfFile.Close()
	rpem, err := NewKopiaRepositoryProtectedEntityManager(repoDir, logrus.StandardLogger())
	logger := logrus.StandardLogger()
	rpetm := rpem.GetProtectedEntityTypeManager("fs")

	fsParams := make(map[string]interface{})
	fsParams["root"] = "/home/dsmithuchida/astrolabe_fs_root"

	fsPETM, err := astrolabefs.NewFSProtectedEntityTypeManagerFromConfig(fsParams, astrolabe.S3Config{
		Port:      9000,
		Host:      net.IPv4(127,0,0,1),
		AccessKey: "accessKey",
		Secret:    "secret",
		Prefix:    "",
		URLBase:   "",
		Region:    "local",
		UseHttp:   false,
	}, logrus.New())
	if err != nil {
		t.Fatal(err)
	}

	fsPEs, err := fsPETM.GetProtectedEntities(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, fsPEID := range fsPEs {
		// FS doesn't have snapshots, but repository likes them, so fake one
		snapID, err := uuid.NewRandom()

		if err != nil {
			t.Fatal(err)
		}
		snapPEID := astrolabe.NewProtectedEntityIDWithSnapshotID(fsPEID.GetPeType(), fsPEID.GetID(),
			astrolabe.NewProtectedEntitySnapshotID(snapID.String()))
		fsPE, err := fsPETM.GetProtectedEntity(ctx, snapPEID)
		if err != nil {
			t.Fatal(err)
		}
		s3PE, err := rpetm.Copy(ctx, fsPE, make(map[string]map[string]interface{}), astrolabe.AllocateNewObject)
		if err != nil {
			t.Fatal(err)
		}
		logger.Printf("Created new kopia PE %s\n", s3PE.GetID().String())
/*
		rep.Close(ctx)

		newRep, err := repo.Open(ctx, configFile, masterPassword, openOpt)
		if err != nil {
			t.Fatalf("can't open: %v", err)
		}
		rpetm, err := NewProtectedEntityTypeManager(context.TODO(), "fs", newRep, logger)
		if err != nil {
			t.Fatal(err)
		}

		checkPE, err := rpetm.GetProtectedEntity(ctx, fsPE.GetID())

		if err != nil {
			t.Fatal(err)
		}

		fsDataReader, err := fsPE.GetDataReader(ctx)
		checkDataReader, err := checkPE.GetDataReader(ctx)
		err = compareStreams(fsDataReader, checkDataReader)
		if err != nil {
			t.Fatal(err)
		}

 */
		/*
			newFSPE, err := fsPETM.Copy(ctx, s3PE, make(map[string]map[string]interface{}), astrolabe.AllocateNewObject)
			if err != nil {
				t.Fatal(err)
			}
			logger.Printf("Restored new FSPE %s\n", newFSPE.GetID().String())
		*/
	}
}
func Test(t *testing.T) {
	ctx := context.Background()
	st, err := filesystem.New(ctx, &filesystem.Options{
		Path: "/tmp/my-repository",
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	configFile := "/tmp/config"
	masterPassword := "4given"
	repo.Connect(ctx, configFile, st, masterPassword, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	openOpt := &repo.Options{}

	rep, err := repo.Open(ctx, configFile, masterPassword, openOpt)
	defer rep.Close(ctx)
	if err != nil {
		t.Fatalf("can't open: %v", err)
	}
/*
	actualSources, err := snapshot.ListSources(ctx, rep)
	if err != nil {
		t.Errorf("error listing sources: %v", err)
	}

	for _, source := range actualSources {
		fmt.Printf("%s\n", source.String())
	}
	snapshots, err := snapshot.ListSnapshots(ctx, rep, actualSources[0])
	if err != nil {
		t.Errorf("error listing sources: %v", err)
	}
	for _, snapshot := range snapshots {
		fmt.Printf("%v\n", snapshot)
	}
 */
	logger := logrus.StandardLogger()
	rpetm, err := NewProtectedEntityTypeManager(context.TODO(), "fs", rep, logger)

	fsParams := make(map[string]interface{})
	fsParams["root"] = "/home/dsmithuchida/astrolabe_fs_root"

	fsPETM, err := astrolabefs.NewFSProtectedEntityTypeManagerFromConfig(fsParams, astrolabe.S3Config{}, logrus.New())
	if err != nil {
		t.Fatal(err)
	}

	fsPEs, err := fsPETM.GetProtectedEntities(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, fsPEID := range fsPEs {
		// FS doesn't have snapshots, but repository likes them, so fake one
		snapID, err := uuid.NewRandom()

		if err != nil {
			t.Fatal(err)
		}
		snapPEID := astrolabe.NewProtectedEntityIDWithSnapshotID(fsPEID.GetPeType(), fsPEID.GetID(),
			astrolabe.NewProtectedEntitySnapshotID(snapID.String()))
		fsPE, err := fsPETM.GetProtectedEntity(ctx, snapPEID)
		if err != nil {
			t.Fatal(err)
		}
		s3PE, err := rpetm.Copy(ctx, fsPE, make(map[string]map[string]interface{}), astrolabe.AllocateNewObject)
		if err != nil {
			t.Fatal(err)
		}
		logger.Printf("Created new kopia PE %s\n", s3PE.GetID().String())

		rep.Close(ctx)

		newRep, err := repo.Open(ctx, configFile, masterPassword, openOpt)
		if err != nil {
			t.Fatalf("can't open: %v", err)
		}
		rpetm, err := NewProtectedEntityTypeManager(context.TODO(), "fs", newRep, logger)
		if err != nil {
			t.Fatal(err)
		}

		checkPE, err := rpetm.GetProtectedEntity(ctx, fsPE.GetID())

		if err != nil {
			t.Fatal(err)
		}

		fsDataReader, err := fsPE.GetDataReader(ctx)
		checkDataReader, err := checkPE.GetDataReader(ctx)
		err = compareStreams(fsDataReader, checkDataReader)
		if err != nil {
			t.Fatal(err)
		}
		/*
		newFSPE, err := fsPETM.Copy(ctx, s3PE, make(map[string]map[string]interface{}), astrolabe.AllocateNewObject)
		if err != nil {
			t.Fatal(err)
		}
		logger.Printf("Restored new FSPE %s\n", newFSPE.GetID().String())
		 */
	}
}

func compareStreams(stream1, stream2 io.ReadCloser) error {
	for {
		buf1 := make([]byte, 1024*1024)
		buf2 := make([]byte, 1024*1024)
		s1BytesRead, err1 := io.ReadFull(stream1, buf1)

		s2BytesRead, err2 := io.ReadFull(stream2, buf2)
		if err1 != nil || err2 != nil {
			if err1 == io.EOF && err2 == io.EOF {
				return nil
			} else if err1 == io.EOF || err2 == io.EOF {
				return errors.New("Mismatched stream length")
			} else {
				if ! (s1BytesRead > 0 && s1BytesRead == s2BytesRead) {
					// If we didn't get an EOF and the number of bytes read is non-zero, assume we got an unexpected EOF
					// and a short read and we'll loop around
					if err1 != nil {
						return err1
					}
					return err2
				}
			}
		}
		if s1BytesRead != s2BytesRead {
			return errors.New("Mismatched read lengths")
		}
		if !bytes.Equal(buf1, buf2) {
			return errors.New("buffers do not match")
		}
	}
	return nil
}
