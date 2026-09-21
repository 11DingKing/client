// Copyright © 2020 The Knative Authors
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
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	servingv1 "knative.dev/serving/pkg/apis/serving/v1"

	"knative.dev/client/pkg/commands"
	clientservingv1 "knative.dev/client/pkg/serving/v1"
)

var applyExample = `
# Create an initial service with using 'kn service apply', if the service has not
# been already created
kn service apply s0 --image knativesamples/helloworld

# Apply the service again which is a no-operation if none of the options changed
kn service apply s0 --image knativesamples/helloworld

# Add an environment variable to your service. Note, that you have to always fully
# specify all parameters (in contrast to 'kn service update')
kn service apply s0 --image knativesamples/helloworld --env foo=bar

# Read the service declaration from a file
kn service apply s0 --filename my-svc.yml

# Apply the service declaration in offline mode to local files instead of a
# kubernetes cluster (experimental). The merged declaration is published
# atomically, no Ready condition is waited for
kn service apply s0 --image knativesamples/helloworld --target=/user/knfiles
kn service apply s0 --image knativesamples/helloworld --target=/user/knfiles/test.yaml
kn service apply s0 --image knativesamples/helloworld --target=/user/knfiles/test.json
`

func NewServiceApplyCommand(p *commands.KnParams) *cobra.Command {
	var applyFlags ConfigurationEditFlags
	var waitFlags commands.WaitFlags

	serviceApplyCommand := &cobra.Command{
		Use:     "apply NAME",
		Short:   "Apply a service declaration",
		Example: applyExample,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if len(args) != 1 && applyFlags.Filename == "" {
				return errors.New("'service apply' requires the service name given as single argument")
			}
			name := ""
			if len(args) == 1 {
				name = args[0]
			}

			namespace, err := p.GetNamespace(cmd)
			if err != nil {
				return err
			}

			var service *servingv1.Service
			applyFlags.RevisionName = ""
			if applyFlags.Filename == "" {
				service, err = constructService(cmd, applyFlags, name, namespace)
			} else {
				service, err = constructServiceFromFile(cmd, applyFlags, name, namespace)
			}
			if err != nil {
				return err
			}

			targetFlag := cmd.Flag("target").Value.String()
			client, err := newServingClient(p, namespace, targetFlag)
			if err != nil {
				return err
			}

			waitDoing, waitVerb, err := examineServiceForApply(cmd, client, service.Name)
			if err != nil {
				return err
			}

			hasChanged, err := client.ApplyService(cmd.Context(), service)
			if err != nil {
				return err
			}

			out := cmd.OutOrStdout()

			// GitOps path: completion is the atomic publication of the target
			// file. There is no cluster and hence no Ready condition to wait for.
			if targetFlag != "" {
				if !hasChanged {
					fmt.Fprintf(out, "No changes to apply to service '%s'.\n", service.Name)
					return nil
				}
				fmt.Fprintf(out, "Service '%s' %s in namespace '%s'.\n", service.Name, waitVerb, client.Namespace())
				return nil
			}

			if !hasChanged {
				fmt.Fprintf(out, "No changes to apply to service '%s'.\n", service.Name)
				if !waitFlags.Wait {
					return showUrl(cmd.Context(), client, service.Name, "unchanged", "", out)
				}
				// Waiting was requested, so even an unchanged declaration must
				// satisfy the Ready condition before completion is reported.
				// This makes a retry after a previous wait-timeout reliable:
				// an already ready service returns immediately, a not-ready one
				// is watched until ready or the timeout is hit.
				wconfig := clientservingv1.WaitConfig{
					Timeout:     time.Duration(waitFlags.TimeoutInSeconds) * time.Second,
					ErrorWindow: time.Duration(waitFlags.ErrorWindowInSeconds) * time.Second,
				}
				return waitForServiceToGetReady(cmd.Context(), client, service.Name, wconfig, "", out)
			}
			return waitIfRequested(cmd.Context(), client, waitFlags, service.Name, waitDoing, waitVerb, "", out)
		},
	}
	commands.AddNamespaceFlags(serviceApplyCommand.Flags(), false)
	commands.AddGitOpsFlags(serviceApplyCommand.Flags())
	applyFlags.AddCreateFlags(serviceApplyCommand)
	waitFlags.AddConditionWaitFlags(serviceApplyCommand, commands.WaitDefaultTimeout, "apply", "service", "ready")
	return serviceApplyCommand
}

func examineServiceForApply(cmd *cobra.Command, client clientservingv1.KnServingClient, serviceName string) (string, string, error) {
	currentService, err := client.GetService(cmd.Context(), serviceName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "Creating", "created", nil
		}
		return "", "", err
	}

	annotationMap := currentService.Annotations
	if annotationMap != nil {
		if _, ok := annotationMap[corev1.LastAppliedConfigAnnotation]; !ok {
			fmt.Fprintf(cmd.OutOrStdout(), "Warning: 'kn service apply' should be used only for services created by 'kn service apply'\n")
		}
	}
	return "Applying", "applied", nil
}
