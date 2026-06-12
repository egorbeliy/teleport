// Copyright 2026 Gravitational, Inc
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

package types

import "github.com/gravitational/trace"

// EnrollPairingFilter encodes server-side filter parameters for EnrollPairing
// watchers. The public Device Trust RPC subscribes scoped to the specific
// token it's blocking on, so a TTL-expired-and-recreated pairing for the
// same user doesn't leak through and resolve the wait on the wrong pairing.
type EnrollPairingFilter struct {
	Token string
}

const enrollPairingFilterKeyToken = "token"

// IntoMap copies the filter values into a string map suitable for
// types.WatchKind.Filter.
func (f *EnrollPairingFilter) IntoMap() map[string]string {
	m := make(map[string]string)
	if f.Token != "" {
		m[enrollPairingFilterKeyToken] = f.Token
	}
	return m
}

// FromMap copies values from a string map into f.
func (f *EnrollPairingFilter) FromMap(m map[string]string) error {
	for key, val := range m {
		switch key {
		case enrollPairingFilterKeyToken:
			f.Token = val
		default:
			return trace.BadParameter("unknown filter key %s", key)
		}
	}
	return nil
}

// Match returns true if the given pairing token passes the filter. An empty
// filter matches everything.
func (f *EnrollPairingFilter) Match(token string) bool {
	if f.Token != "" && token != f.Token {
		return false
	}
	return true
}
