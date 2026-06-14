/*
Copyright 2021 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package prompt

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/gravitational/trace"
	"github.com/stretchr/testify/require"
)

func TestInput(t *testing.T) {
	t.Parallel()

	out, in := io.Pipe()
	t.Cleanup(func() { out.Close() })
	write := func(t *testing.T, s string) {
		_, err := in.Write([]byte(s))
		require.NoError(t, err)
	}

	r := NewContextReader(out)
	ctx := context.Background()

	t.Run("no whitespace", func(t *testing.T) {
		go write(t, "hi")
		got, err := Input(ctx, io.Discard, r, "")
		require.NoError(t, err)
		require.Equal(t, "hi", got)
	})

	t.Run("with whitespace", func(t *testing.T) {
		go write(t, "hey\n")
		got, err := Input(ctx, io.Discard, r, "")
		require.NoError(t, err)
		require.Equal(t, "hey", got)
	})

	t.Run("closed input", func(t *testing.T) {
		require.NoError(t, in.Close())
		got, err := Input(ctx, io.Discard, r, "")
		require.ErrorIs(t, err, io.EOF)
		require.Empty(t, got)
	})
}

func TestPickOneByNumber(t *testing.T) {
	t.Parallel()

	type option struct {
		name        string
		description string
	}

	options := []option{
		{name: "proxy", description: "public entrypoint"},
		{name: "auth", description: "certificate authority"},
		{name: "node", description: "ssh endpoint"},
	}
	renderRow := func(item option) string {
		return item.name + " (" + item.description + ")"
	}
	menu := "Pick a service:\n" +
		"1. proxy (public entrypoint)\n" +
		"2. auth (certificate authority)\n" +
		"3. node (ssh endpoint)\n" +
		"Choose service: "

	tests := []struct {
		name         string
		input        string
		want         option
		assertErr    func(*testing.T, error)
		assertOutput func(*testing.T, string)
	}{
		{
			name:  "selects option by number",
			input: "2\n",
			want:  options[1],
			assertOutput: func(t *testing.T, output string) {
				require.Equal(t, menu, output)
			},
		},
		{
			name:  "trims whitespace around answer",
			input: "\t3 \n",
			want:  options[2],
			assertOutput: func(t *testing.T, output string) {
				require.Equal(t, menu, output)
			},
		},
		{
			name:  "retries invalid choices",
			input: "bogus\n7\n1\n",
			want:  options[0],
			assertOutput: func(t *testing.T, output string) {
				require.Equal(t, menu+"error: invalid choice: bogus\n\n"+menu+"error: invalid choice: 7\n\n"+menu, output)
			},
		},
		{
			name:  "fails after too many invalid choices",
			input: strings.Repeat("0\n", nAttempts),
			assertErr: func(t *testing.T, err error) {
				require.True(t, trace.IsLimitExceeded(err), "expected limit exceeded error, got %v", err)
			},
			assertOutput: func(t *testing.T, output string) {
				require.Equal(t, nAttempts, strings.Count(output, menu))
				require.Equal(t, nAttempts, strings.Count(output, "error: invalid choice: 0\n\n"))
			},
		},
		{
			name:  "fails on read error",
			input: "",
			assertErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "Failed reading prompt response.")
			},
			assertOutput: func(t *testing.T, output string) {
				require.Equal(t, menu, output)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := &bytes.Buffer{}
			got, err := PickOneByNumber(strings.NewReader(tt.input), out, "Pick a service", "Choose service", options, renderRow)
			if tt.assertErr != nil {
				require.Error(t, err)
				tt.assertErr(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
			if tt.assertOutput != nil {
				tt.assertOutput(t, out.String())
			}
		})
	}
}
