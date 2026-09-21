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

package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"gotest.tools/v3/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
	servingv1 "knative.dev/serving/pkg/apis/serving/v1"
	"sigs.k8s.io/yaml"

	"knative.dev/client/pkg/util"
)

func TestApplyServiceWithNoImage(t *testing.T) {
	_, client := setup()
	serviceFaulty := newService("faulty-service")
	_, err := client.ApplyService(context.Background(), serviceFaulty)
	assert.Assert(t, err != nil)
	assert.Assert(t, util.ContainsAll(err.Error(), "image name"))
}

func TestApplyServiceCreate(t *testing.T) {
	serving, client := setup()

	serviceNew := newServiceWithImage("new-service", "test/image")
	// Server-side state of "persisted" services, used by both get and create reactors
	stored := map[string]*servingv1.Service{}

	serving.AddReactor("get", "services",
		func(a clienttesting.Action) (bool, runtime.Object, error) {
			name := a.(clienttesting.GetAction).GetName()
			if name == "new-service-fail" {
				return true, nil, errors.NewInternalError(fmt.Errorf("mock internal error"))
			}
			if svc, ok := stored[name]; ok {
				// Create read-back: the object has been persisted by the server
				return true, svc, nil
			}
			return true, nil, errors.NewNotFound(servingv1.Resource("service"), name)
		})

	serving.AddReactor("create", "services",
		func(a clienttesting.Action) (bool, runtime.Object, error) {
			assert.Equal(t, testNamespace, a.GetNamespace())
			svc := a.(clienttesting.CreateAction).GetObject().(*servingv1.Service)
			stored[svc.Name] = svc
			return true, svc, nil
		})

	hasChanged, err := client.ApplyService(context.Background(), serviceNew)
	assert.NilError(t, err)
	assert.Assert(t, hasChanged, "service has changed")
	// The create path must read the object back before reporting completion
	_, err = client.GetService(context.Background(), "new-service")
	assert.NilError(t, err)

	serviceFail := newServiceWithImage("new-service-fail", "test/image")
	hasChanged, err = client.ApplyService(context.Background(), serviceFail)
	assert.ErrorType(t, err, errors.IsInternalError)
	assert.Assert(t, !hasChanged)
}

func TestApplyServiceUpdate(t *testing.T) {
	serving, client := setup()

	serviceOld := newServiceWithImage("my-service", "test/image")
	serviceNew := newServiceWithImage("my-service", "test/new-image")
	serviceConflict := newServiceWithImage("conflict-service", "test/image")
	// Mutable server-side state; reactors simulate a real server that persists patches
	current := map[string]*servingv1.Service{
		"my-service":       serviceOld,
		"conflict-service": serviceConflict,
	}

	serving.AddReactor("get", "services",
		func(a clienttesting.Action) (bool, runtime.Object, error) {
			name := a.(clienttesting.GetAction).GetName()
			if name == "err-service" {
				return true, newServiceWithImage("err-service", "test/image"), errors.NewInternalError(fmt.Errorf("internal error"))
			}
			svc, ok := current[name]
			if !ok {
				t.FailNow()
			}
			return true, svc, nil
		})

	serving.AddReactor("patch", "services",
		func(a clienttesting.Action) (bool, runtime.Object, error) {
			name := a.(clienttesting.GetAction).GetName()
			if name == "conflict-service" {
				return true, current[name], errors.NewConflict(servingv1.Resource("service"), name, fmt.Errorf("error patching service"))
			}
			// Simulate the server applying the merge patch and persisting the result
			patch := a.(clienttesting.PatchAction).GetPatch()
			currentJSON, err := json.Marshal(current[name])
			assert.NilError(t, err)
			mergedJSON, err := jsonMergePatch(currentJSON, patch)
			assert.NilError(t, err)
			merged := &servingv1.Service{}
			assert.NilError(t, json.Unmarshal(mergedJSON, merged))
			merged.Generation = 2
			merged.Status.ObservedGeneration = 1
			current[name] = merged
			return true, merged, nil
		})

	// Update: server applies the patch, read-back matches the target declaration
	hasChanged, err := client.ApplyService(context.Background(), serviceNew)
	assert.NilError(t, err)
	assert.Assert(t, hasChanged, "service has changed")

	// Applying the same declaration again is a no-op computed from the actual server object
	hasChanged, err = client.ApplyService(context.Background(), serviceNew)
	assert.NilError(t, err)
	assert.Assert(t, !hasChanged, "service has not changed")

	// A corrupted last-applied annotation fails the merge instead of reporting success
	current["my-service"] = current["my-service"].DeepCopy()
	current["my-service"].SetAnnotations(map[string]string{corev1.LastAppliedConfigAnnotation: "never"})
	hasChanged, err = client.ApplyService(context.Background(), serviceNew)
	assert.ErrorContains(t, err, "Invalid JSON")
	assert.Assert(t, !hasChanged, "service has not changed")

	// Persistent conflicts are retried and finally reported as conflict
	hasChanged, err = client.ApplyService(context.Background(), serviceConflict)
	assert.ErrorType(t, err, errors.IsConflict)
	assert.Assert(t, !hasChanged, "service has not changed")

	// An internal error on the initial read is surfaced directly
	serviceErr := newServiceWithImage("err-service", "test/image")
	hasChanged, err = client.ApplyService(context.Background(), serviceErr)
	assert.ErrorType(t, err, errors.IsInternalError)
	assert.Assert(t, !hasChanged, "service has not changed")
}

// TestApplyServiceReadbackDiverges verifies that a PATCH whose server-side
// read-back does not match the target is never reported as success: it is
// retried against the actual object and, while it keeps diverging, an error is
// returned after the attempts are exhausted.
func TestApplyServiceReadbackDiverges(t *testing.T) {
	serving, client := setup()

	serviceOld := newServiceWithImage("stale-service", "test/image")
	serviceNew := newServiceWithImage("stale-service", "test/new-image")

	serving.AddReactor("get", "services",
		func(a clienttesting.Action) (bool, runtime.Object, error) {
			// Read-back always returns the stale object, as if the patch never landed
			return true, serviceOld, nil
		})
	serving.AddReactor("patch", "services",
		func(a clienttesting.Action) (bool, runtime.Object, error) {
			// The PATCH call succeeds but the server keeps serving the old state
			saved := serviceNew.DeepCopy()
			saved.Generation = 2
			saved.Status.ObservedGeneration = 1
			return true, saved, nil
		})

	hasChanged, err := client.ApplyService(context.Background(), serviceNew)
	assert.Assert(t, err != nil)
	assert.Assert(t, util.ContainsAll(err.Error(), "does not match the target declaration"))
	assert.Assert(t, !hasChanged, "a diverged apply must not be reported as changed")
}

// TestApplyServiceConvergesAfterRetry verifies that when the server-side
// read-back only matches the target declaration after a couple of retries, the
// apply is ultimately reported as changed and successful.
func TestApplyServiceConvergesAfterRetry(t *testing.T) {
	serving, client := setup()

	serviceOld := newServiceWithImage("slow-service", "test/image")
	serviceNew := newServiceWithImage("slow-service", "test/new-image")
	current := serviceOld
	applyCount := 0

	serving.AddReactor("get", "services",
		func(a clienttesting.Action) (bool, runtime.Object, error) {
			return true, current, nil
		})
	serving.AddReactor("patch", "services",
		func(a clienttesting.Action) (bool, runtime.Object, error) {
			applyCount++
			patch := a.(clienttesting.PatchAction).GetPatch()
			if applyCount >= 3 {
				// Only the third patch is actually persisted by the server
				currentJSON, err := json.Marshal(current)
				assert.NilError(t, err)
				mergedJSON, err := jsonMergePatch(currentJSON, patch)
				assert.NilError(t, err)
				merged := &servingv1.Service{}
				assert.NilError(t, json.Unmarshal(mergedJSON, merged))
				merged.Generation = 2
				merged.Status.ObservedGeneration = 1
				current = merged
				return true, merged, nil
			}
			// The PATCH call is accepted but its effect is not visible to reads yet
			saved := current.DeepCopy()
			saved.Generation = 2
			saved.Status.ObservedGeneration = 1
			return true, saved, nil
		})

	hasChanged, err := client.ApplyService(context.Background(), serviceNew)
	assert.NilError(t, err)
	assert.Assert(t, hasChanged, "service has changed once the server converged")
	assert.Equal(t, 3, applyCount)
}

func newServiceWithImage(name string, image string) *servingv1.Service {
	svc := newService(name)
	svc.Spec = servingv1.ServiceSpec{
		ConfigurationSpec: servingv1.ConfigurationSpec{
			Template: servingv1.RevisionTemplateSpec{
				Spec: servingv1.RevisionSpec{
					PodSpec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Image: image,
							},
						},
					},
				},
			},
		},
	}
	return svc
}

func TestExtractUserContainer(t *testing.T) {
	tests := []struct {
		name    string
		service string
		want    string
	}{
		{"Simple Service",
			`
spec:
  template:
    spec:
      containers:
        - image: gcr.io/foo/bar:baz
`,
			`
image: gcr.io/foo/bar:baz
`,
		},
		{
			"No template",
			`
spec:
`,
			"",
		}, {
			"No template spec",
			`
spec:
  template:
`,
			"",
		},
		{
			"No template spec containers",
			`
spec:
  template:
    spec:
`,
			"",
		},
		{
			"Empty template spec containers",
			`
spec:
  template:
    spec:
      containers: []
`,
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var serviceMap map[string]interface{}
			yaml.Unmarshal([]byte(tt.service), &serviceMap)

			got := extractUserContainer(serviceMap)

			if tt.want == "" {
				assert.Assert(t, got == nil)
			} else {
				var expectedMap map[string]interface{}
				yaml.Unmarshal([]byte(tt.want), &expectedMap)
				if !reflect.DeepEqual(got, expectedMap) {
					t.Errorf("extractUserContainer() = %v, want %v", got, expectedMap)
				}
			}
		})
	}
}

func TestCleanupServiceUnstructured(t *testing.T) {
	tests := []struct {
		name    string
		service string
		want    string
	}{
		{"Simple Service with fields to remove",
			`
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: foo
  creationTimestamp: "2020-10-22T08:16:37Z"
spec:
  template:
    metadata:
      name: "bar"
      creationTimestamp: null
    spec:
      containers:
      - image: gcr.io/foo/bar:baz
        name: "bla"
        resources: {}
status:
  observedGeneration: 1
`,
			`
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: foo
spec:
  template:
    metadata:
      name: "bar"
    spec:
      containers:
      - image: gcr.io/foo/bar:baz
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ud := &unstructured.Unstructured{}
			assert.NilError(t, yaml.Unmarshal([]byte(tt.service), ud))
			cleanupServiceUnstructured(ud)

			expectedMap := &unstructured.Unstructured{}
			yaml.Unmarshal([]byte(tt.want), &expectedMap)
			if !reflect.DeepEqual(ud, expectedMap) {
				t.Errorf("cleanupServiceUnstructured(): %s", cmp.Diff(ud, expectedMap))
			}
		})
	}
}

func TestJSONMergePatch(t *testing.T) {
	tests := []struct {
		name     string
		original string
		patch    string
		want     string
	}{
		{"nested objects are merged", `{"a":{"b":1,"c":2}}`, `{"a":{"c":3,"d":4}}`, `{"a":{"b":1,"c":3,"d":4}}`},
		{"null removes members", `{"a":1,"b":2}`, `{"b":null}`, `{"a":1}`},
		{"null removes nested members", `{"a":{"b":1,"c":2}}`, `{"a":{"c":null}}`, `{"a":{"b":1}}`},
		{"arrays are replaced wholesale", `{"a":[1,2,3]}`, `{"a":[9]}`, `{"a":[9]}`},
		{"object is replaced by scalar", `{"a":{"b":1}}`, `{"a":1}`, `{"a":1}`},
		{"scalar is replaced by object", `{"a":1}`, `{"a":{"b":1}}`, `{"a":{"b":1}}`},
		{"empty original is created", ``, `{"a":1}`, `{"a":1}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := jsonMergePatch([]byte(tt.original), []byte(tt.patch))
			assert.NilError(t, err)
			var gotMap, wantMap interface{}
			assert.NilError(t, json.Unmarshal(got, &gotMap))
			assert.NilError(t, json.Unmarshal([]byte(tt.want), &wantMap))
			if !reflect.DeepEqual(gotMap, wantMap) {
				t.Errorf("jsonMergePatch() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestPatchTouchesResource(t *testing.T) {
	onlyLastApplied := fmt.Sprintf(`{"metadata":{"annotations":{%q:"x"}}}`, corev1.LastAppliedConfigAnnotation)
	touches, err := patchTouchesResource([]byte(onlyLastApplied))
	assert.NilError(t, err)
	assert.Assert(t, !touches, "last-applied annotation update alone is not a resource change")

	withOtherAnnotation := fmt.Sprintf(`{"metadata":{"annotations":{%q:"x","foo":"bar"}}}`, corev1.LastAppliedConfigAnnotation)
	touches, err = patchTouchesResource([]byte(withOtherAnnotation))
	assert.NilError(t, err)
	assert.Assert(t, touches, "another annotation is a resource change")

	specChange := `{"metadata":{"annotations":{}},"spec":{"template":{"spec":{"containers":[{"image":"x"}]}}}}`
	touches, err = patchTouchesResource([]byte(specChange))
	assert.NilError(t, err)
	assert.Assert(t, touches, "a spec change is a resource change")

	touches, err = patchTouchesResource([]byte(`{}`))
	assert.NilError(t, err)
	assert.Assert(t, !touches, "empty patch does not touch the resource")
}
