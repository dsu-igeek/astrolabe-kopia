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
	"fmt"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/astrolabe/pkg/astrolabe"
	"testing"
	astrolabefs "github.com/vmware-tanzu/astrolabe/pkg/fs"
)

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
	if err != nil {
		t.Fatalf("can't open: %v", err)
	}

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
		snapPEID := astrolabe.NewProtectedEntityIDWithSnapshotID(fsPEID.GetPeType(), fsPEID.GetID(),
			astrolabe.NewProtectedEntitySnapshotID("dummy-snap-id"))
		fsPE, err := fsPETM.GetProtectedEntity(ctx, snapPEID)
		if err != nil {
			t.Fatal(err)
		}
		s3PE, err := rpetm.Copy(ctx, fsPE, make(map[string]map[string]interface{}), astrolabe.AllocateNewObject)
		if err != nil {
			t.Fatal(err)
		}
		logger.Printf("Creted new s3PE %s\n", s3PE.GetID().String())
		/*
		newFSPE, err := fsPETM.Copy(ctx, s3PE, make(map[string]map[string]interface{}), astrolabe.AllocateNewObject)
		if err != nil {
			t.Fatal(err)
		}
		logger.Printf("Restored new FSPE %s\n", newFSPE.GetID().String())
		 */
	}
}