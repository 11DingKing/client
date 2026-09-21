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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	servingv1 "knative.dev/serving/pkg/apis/serving/v1"

	knclient "knative.dev/client/pkg/serving/v1"
	"knative.dev/client/pkg/util/mock"
	"knative.dev/client/pkg/wait"

	"knative.dev/client/pkg/util"
)

func TestServiceApplyCreateMock(t *testing.T) {
	// New mock client
	client := knclient.NewMockKnServiceClient(t)

	r := setupServiceApplyRecorder(client, "foo", nil, apierrors.NewNotFound(servingv1.Resource("service"), "foo"), true)

	// Testing:
	output, err := executeServiceCommand(client, "apply", "foo", "--image", "gcr.io/foo/bar:baz")
	assert.NilError(t, err)
	assert.Assert(t, util.ContainsAll(output, "created", "foo", "http://foo.example.com", "Ready"))

	// Validate that all recorded API methods have been called
	r.Validate()
}

func TestServiceApplyCreateFromFileMock(t *testing.T) {
	testWithServiceFiles(t, func(t *testing.T, file string) {
		for _, testArgs := range [][]string{
			{"apply", "foo", "--filename", file},
			{"apply", "--filename", file},
		} {
			client := knclient.NewMockKnServiceClient(t)

			r := setupServiceApplyRecorder(client, "foo", nil, apierrors.NewNotFound(servingv1.Resource("service"), "foo"), true)

			// Testing:
			output, err := executeServiceCommand(client, testArgs...)
			assert.NilError(t, err)
			assert.Assert(t, util.ContainsAll(output, "created", "foo", "http://foo.example.com", "Ready"))

			// Validate that all recorded API methods have been called
			r.Validate()
		}
	})
}

func TestServiceApplyCreateFromFileMockWithoutName(t *testing.T) {
	testWithServiceFiles(t, func(t *testing.T, file string) {
		client := knclient.NewMockKnServiceClient(t)

		r := setupServiceApplyRecorder(client, "foo", nil, apierrors.NewNotFound(servingv1.Resource("service"), "foo"), true)

		// Testing:
		output, err := executeServiceCommand(client, "apply", "foo", "--filename", file)
		assert.NilError(t, err)
		assert.Assert(t, util.ContainsAll(output, "created", "foo", "http://foo.example.com", "Ready"))

		// Validate that all recorded API methods have been called
		r.Validate()
	})
}

func TestServiceApplyUpdateMock(t *testing.T) {
	// New mock client
	client := knclient.NewMockKnServiceClient(t)

	service := createServiceWithImage("foo", "gcr.io/foo/bar:baz")

	r := setupServiceApplyRecorder(client, "foo", service, nil, true)

	// Testing:
	output, err := executeServiceCommand(client, "apply", "foo", "--image", "gcr.io/foo/bar:baz")
	assert.NilError(t, err)
	assert.Assert(t, util.ContainsAll(output, "applied", "foo", "http://foo.example.com", "Ready"))

	// Validate that all recorded API methods have been called
	r.Validate()
}

func TestServiceApplyUpdateUnchanged(t *testing.T) {
	// New mock client
	client := knclient.NewMockKnServiceClient(t)

	service := createServiceWithImage("foo", "gcr.io/foo/bar:baz")

	r := setupServiceApplyRecorder(client, "foo", service, nil, false)

	// Testing:
	output, err := executeServiceCommand(client, "apply", "foo", "--image", "gcr.io/foo/bar:baz")
	assert.NilError(t, err)
	assert.Assert(t, util.ContainsAll(output, "No changes", "apply", "foo", "http://foo.example.com"))

	// Validate that all recorded API methods have been called
	r.Validate()
}

func TestServiceApplyWithGetError(t *testing.T) {
	// New mock client
	client := knclient.NewMockKnServiceClient(t)

	errThrown := errors.New("boom!")
	r := setupServiceApplyRecorder(client, "foo", nil, errThrown, true)

	_, err := executeServiceCommand(client, "apply", "foo", "--image", "gcr.io/foo/bar:baz")
	assert.Equal(t, err, errThrown)

	// Validate that all recorded API methods have been called
	r.Validate()
}

func TestServiceApplyUnchangedNoWait(t *testing.T) {
	// New mock client
	client := knclient.NewMockKnServiceClient(t)

	service := createServiceWithImage("foo", "gcr.io/foo/bar:baz")

	// Recording: with --no-wait no Ready condition is checked for an unchanged service
	r := client.Recorder()
	r.GetService("foo", service, nil)
	r.ApplyService(func(t *testing.T, a interface{}) {
		svc := a.(*servingv1.Service)
		assert.Equal(t, svc.Name, "foo")
		setUrl(svc, "http://foo.example.com")
	}, false, nil)
	r.GetService("foo", getServiceWithUrl("foo", "http://foo.example.com"), nil)

	// Testing:
	output, err := executeServiceCommand(client, "apply", "foo", "--image", "gcr.io/foo/bar:baz", "--no-wait")
	assert.NilError(t, err)
	assert.Assert(t, util.ContainsAll(output, "No changes", "foo", "http://foo.example.com"))
	assert.Assert(t, !strings.Contains(output, "Ready"))

	// Validate that all recorded API methods have been called
	r.Validate()
}

func TestServiceApplyGitopsDirectory(t *testing.T) {
	targetDir := t.TempDir()

	// Create
	output, err := executeServiceCommandGitops("apply", "foo", "--image", "gcr.io/foo/bar:baz", "--target", targetDir)
	assert.NilError(t, err)
	assert.Assert(t, util.ContainsAll(output, "created", "foo", "default"), output)
	// GitOps completion does not wait for a Ready condition
	assert.Assert(t, !strings.Contains(output, "Ready"), output)

	filePath := filepath.Join(targetDir, "default", "ksvc", "foo.yaml")
	content, err := os.ReadFile(filePath)
	assert.NilError(t, err)
	assert.Assert(t, strings.Contains(string(content), "gcr.io/foo/bar:baz"))
	assert.Assert(t, strings.Contains(string(content), "last-applied-configuration"))

	// Applying the same declaration is unchanged
	output, err = executeServiceCommandGitops("apply", "foo", "--image", "gcr.io/foo/bar:baz", "--target", targetDir)
	assert.NilError(t, err)
	assert.Assert(t, util.ContainsAll(output, "No changes", "foo"), output)

	// Applying an updated declaration is applied, no Ready wait involved
	output, err = executeServiceCommandGitops("apply", "foo", "--image", "gcr.io/foo/bar:baz", "--env", "a=mouse", "--target", targetDir)
	assert.NilError(t, err)
	assert.Assert(t, util.ContainsAll(output, "applied", "foo"), output)
	assert.Assert(t, !strings.Contains(output, "Ready"), output)

	content, err = os.ReadFile(filePath)
	assert.NilError(t, err)
	assert.Assert(t, strings.Contains(string(content), "name: a"))
	assert.Assert(t, strings.Contains(string(content), "mouse"))

	// The published declaration converges, applying it again is a no-op
	output, err = executeServiceCommandGitops("apply", "foo", "--image", "gcr.io/foo/bar:baz", "--env", "a=mouse", "--target", targetDir)
	assert.NilError(t, err)
	assert.Assert(t, util.ContainsAll(output, "No changes", "foo"), output)
}

func TestServiceApplyGitopsSingleJSONFile(t *testing.T) {
	targetFile := filepath.Join(t.TempDir(), "foo.json")

	output, err := executeServiceCommandGitops("apply", "foo", "--image", "gcr.io/foo/bar:baz", "--target", targetFile)
	assert.NilError(t, err)
	assert.Assert(t, util.ContainsAll(output, "created", "foo"), output)

	content, err := os.ReadFile(targetFile)
	assert.NilError(t, err)
	assert.Assert(t, strings.HasPrefix(strings.TrimSpace(string(content)), "{"), "target must be JSON")
	assert.Assert(t, strings.Contains(string(content), "gcr.io/foo/bar:baz"))
}

func setupServiceApplyRecorder(client *knclient.MockKnServingClient, name string, service *servingv1.Service, err error, hasChanged bool) *knclient.ServingRecorder {
	// Recording:
	r := client.Recorder()
	// Check for existing service --> no
	r.GetService(name, service, err)
	// Error test
	if err != nil && !apierrors.IsNotFound(err) {
		return r
	}

	// Create service (don't validate given service --> "Any()" arg is allowed)
	r.ApplyService(func(t *testing.T, a interface{}) {
		svc := a.(*servingv1.Service)
		assert.Equal(t, svc.Name, name)
		setUrl(svc, fmt.Sprintf("http://%s.example.com", name))
	}, hasChanged, nil)

	// Both changed and unchanged declarations wait for (or immediately verify)
	// the Ready condition before completion is reported
	r.WaitForService(name, mock.Any(), wait.NoopMessageCallback(), nil, time.Second)

	// Fetch service for URL
	r.GetService(name, getServiceWithUrl(name, fmt.Sprintf("http://%s.example.com", name)), nil)

	return r
}

func createServiceWithImage(name string, image string) *servingv1.Service {
	return &servingv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: servingv1.ServiceSpec{
			ConfigurationSpec: servingv1.ConfigurationSpec{
				Template: servingv1.RevisionTemplateSpec{
					Spec: servingv1.RevisionSpec{
						PodSpec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Image: image,
								},
							},
						},
					},
				},
			},
		},
	}
}
