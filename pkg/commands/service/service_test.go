// Copyright © 2019 The Knative Authors
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

package service

import (
	"bytes"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"

	"knative.dev/client/pkg/commands"
	knflags "knative.dev/client/pkg/flags"
	clientservingv1 "knative.dev/client/pkg/serving/v1"
)

// Helper methods
var blankConfig clientcmd.ClientConfig

func init() {
	var err error
	blankConfig, err = clientcmd.NewClientConfigFromBytes([]byte(`kind: Config
version: v1
users:
- name: u
clusters:
- name: c
  cluster:
    server: example.com
contexts:
- name: x
  context:
    user: u
    cluster: c
current-context: x
`))
	if err != nil {
		panic(err)
	}
}

func executeServiceCommand(client clientservingv1.KnServingClient, args ...string) (string, error) {
	knParams := &commands.KnParams{}
	knParams.ClientConfig = blankConfig

	// we need to temporary reset os.Args, becase it is being used for evaluation
	// of order of envs set by --env and --env-value-from
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = args

	output := new(bytes.Buffer)
	knParams.Output = output
	knParams.NewServingClient = func(namespace string) (clientservingv1.KnServingClient, error) {
		return client, nil
	}
	cmd := NewServiceCommand(knParams)
	cmd.SetArgs(args)
	cmd.SetOutput(output)

	cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		return knflags.ReconcileBoolFlags(cmd.Flags())
	}
	err := cmd.Execute()
	return output.String(), err
}

// executeServiceCommandGitops runs a service command against a real local
// GitOps client rooted at targetDir (the --target flag is passed by the caller
// inside args).
func executeServiceCommandGitops(args ...string) (string, error) {
	knParams := &commands.KnParams{}
	knParams.ClientConfig = blankConfig

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = args

	output := new(bytes.Buffer)
	knParams.Output = output
	knParams.NewGitopsServingClient = func(namespace string, dir string) (clientservingv1.KnServingClient, error) {
		return clientservingv1.NewKnServingGitOpsClient(namespace, dir), nil
	}
	cmd := NewServiceCommand(knParams)
	cmd.SetArgs(args)
	cmd.SetOutput(output)

	cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		return knflags.ReconcileBoolFlags(cmd.Flags())
	}
	err := cmd.Execute()
	return output.String(), err
}
