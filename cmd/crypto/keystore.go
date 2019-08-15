// MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crypto

import (
	"encoding/base64"
	"fmt"
)

type KeyStore interface {
	Get(path string) (ObjectKey, error)

	Store(path string, key ObjectKey) error

	Delete(path string) error
}

func (v *vaultService) Get(path string) (ObjectKey, error) {
	secret, err := v.client.Logical().Read(fmt.Sprintf("/%s/%s", v.keyStore, path))
	if err != nil {
		return ObjectKey{}, err
	}
	base64Key := secret.Data["object-key"].(string)
	decodedKey, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return ObjectKey{}, err
	}
	if len(decodedKey) != 32 {
		// TODO: error
	}
	var objectKey ObjectKey
	copy(objectKey[:], decodedKey)
	return objectKey, nil
}

func (v *vaultService) Store(path string, key ObjectKey) error {
	payload := map[string]interface{}{
		"object-key": base64.StdEncoding.EncodeToString(key[:]),
	}
	_, err := v.client.Logical().Write(fmt.Sprintf("/%s/%s", v.keyStore, path), payload)
	return err
}

func (v *vaultService) Delete(path string) error {
	_, err := v.client.Logical().Delete(fmt.Sprintf("/%s/%s", v.keyStore, path))
	return err
}
