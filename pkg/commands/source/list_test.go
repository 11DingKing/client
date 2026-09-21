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

package source

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"knative.dev/client/pkg/dynamic/fake"

	"gotest.tools/v3/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	clientdynamic "knative.dev/client/pkg/dynamic"
	"knative.dev/client/pkg/commands"
	"knative.dev/client/pkg/util"
	"knative.dev/client/pkg/util/mock"
)

const (
	crdGroup          = "apiextensions.k8s.io"
	crdVersion        = "v1"
	crdKind           = "CustomResourceDefinition"
	sourcesLabelKey   = "duck.knative.dev/source"
	sourcesLabelValue = "true"
	testNamespace     = "current"
)

// sourceFakeCmd takes cmd to be executed using dynamic client
// pass the objects to be registered to dynamic client
func sourceFakeCmd(args []string, objects ...runtime.Object) (output []string, err error) {
	knParams := &commands.KnParams{}
	cmd, _, buf := commands.CreateDynamicTestKnCommand(NewSourceCommand(knParams), knParams, objects...)
	cmd.SetArgs(args)
	err = cmd.Execute()
	if err != nil {
		return
	}
	output = strings.Split(buf.String(), "\n")
	return
}

func TestSourceListTypesNoSourcesInstalled(t *testing.T) {
	_, err := sourceFakeCmd([]string{"source", "list-types"})
	assert.Check(t, err != nil)
	assert.Check(t, util.ContainsAll(err.Error(), "no", "Knative Sources", "found", "backend", "verify", "installation"))
}

func TestSourceListTypesNoSourcesWithJsonOutput(t *testing.T) {
	output, err := sourceFakeCmd([]string{"source", "list-types", "-o", "json"},
		&unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apiextensions.k8s.io/v1",
				"kind":       "CustomResourceDefinitionList",
			},
		})
	assert.NilError(t, err)
	assert.Check(t, util.ContainsAll(strings.Join(output[:], "\n"), "\"apiVersion\": \"apiextensions.k8s.io/v1\"", "\"items\": []", "\"kind\": \"CustomResourceDefinitionList\""))
}

func TestSourceListTypes(t *testing.T) {
	output, err := sourceFakeCmd([]string{"source", "list-types"},
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
		newSourceCRDObjWithSpec("apiserversources", "sources.knative.dev", "v1", "ApiServerSource"),
	)
	assert.NilError(t, err)
	assert.Check(t, util.ContainsAll(output[0], "TYPE", "S", "NAME", "DESCRIPTION"))
	assert.Check(t, util.ContainsAll(output[1], "ApiServerSource", "X", "apiserversources"))
	assert.Check(t, util.ContainsAll(output[2], "PingSource", "X", "pingsources"))
}

func TestSourceListTypesNoHeaders(t *testing.T) {
	output, err := sourceFakeCmd([]string{"source", "list-types", "--no-headers"},
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
	)
	assert.NilError(t, err)
	assert.Check(t, util.ContainsNone(output[0], "TYPE", "NAME", "DESCRIPTION"))
	assert.Check(t, util.ContainsAll(output[0], "PingSource"))
}

func TestListBuiltInSourceTypes(t *testing.T) {
	sources, err := listBuiltInSourceTypes(context.Background(), fake.CreateFakeKnDynamicClient("current"))
	assert.NilError(t, err)
	if sources == nil {
		t.Fatal("sources = nil, want not nil")
	}
	assert.Equal(t, len(sources.Items), 4)
}

func TestSourceListNoSourcesInstalled(t *testing.T) {
	_, err := sourceFakeCmd([]string{"source", "list"})
	assert.Check(t, err != nil)
	assert.Check(t, util.ContainsAll(err.Error(), "no sources", "found", "backend", "verify", "installation"))
}

func TestSourceListEmpty(t *testing.T) {
	output, err := sourceFakeCmd([]string{"source", "list", "-o", "json"},
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
	)
	assert.NilError(t, err)
	outputJson := strings.Join(output[:], "\n")
	assert.Assert(t, util.ContainsAll(outputJson, "\"apiVersion\": \"client.knative.dev/v1alpha1\"", "\"items\": [],", "\"kind\": \"SourceList\""))
}

func TestSourceList(t *testing.T) {
	output, err := sourceFakeCmd([]string{"source", "list"},
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
		newSourceCRDObjWithSpec("sinkbindings", "sources.knative.dev", "v1", "SinkBinding"),
		newSourceCRDObjWithSpec("apiserversources", "sources.knative.dev", "v1", "ApiServerSource"),
		newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource"),
		newSourceUnstructuredObj("s1", "sources.knative.dev/v1", "SinkBinding"),
		newSourceUnstructuredObj("a1", "sources.knative.dev/v1", "ApiServerSource"),
	)
	assert.NilError(t, err)
	assert.Check(t, util.ContainsAll(output[0], "NAME", "TYPE", "RESOURCE", "SINK", "READY"))
	assert.Check(t, util.ContainsAll(output[1], "a1", "ApiServerSource", "apiserversources.sources.knative.dev", "ksvc:foo", "True"))
	assert.Check(t, util.ContainsAll(output[2], "p1", "PingSource", "pingsources.sources.knative.dev", "ksvc:foo", "True"))
	assert.Check(t, util.ContainsAll(output[3], "s1", "SinkBinding", "sinkbindings.sources.knative.dev", "ksvc:foo", "True"))
}

func TestSourceListUntyped(t *testing.T) {
	output, err := sourceFakeCmd([]string{"source", "list"},
		newSourceCRDObjWithSpec("kafkasources", "sources.knative.dev", "v1alpha1", "KafkaSource"),
		newSourceUnstructuredObj("k1", "sources.knative.dev/v1alpha1", "KafkaSource"),
		newSourceUnstructuredObj("k2", "sources.knative.dev/v1alpha1", "KafkaSource"),
	)
	assert.NilError(t, err)
	assert.Check(t, util.ContainsAll(output[0], "NAME", "TYPE", "RESOURCE", "SINK", "READY"))
	assert.Check(t, util.ContainsAll(output[1], "k1", "KafkaSource", "kafkasources.sources.knative.dev", "ksvc:foo", "True"))
	assert.Check(t, util.ContainsAll(output[2], "k2", "KafkaSource", "kafkasources.sources.knative.dev", "ksvc:foo", "True"))
}

func TestSourceListNoHeaders(t *testing.T) {
	output, err := sourceFakeCmd([]string{"source", "list", "--no-headers"},
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
		newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource"),
	)
	assert.NilError(t, err)
	assert.Check(t, util.ContainsNone(output[0], "NAME", "TYPE", "RESOURCE", "SINK", "READY"))
	assert.Check(t, util.ContainsAll(output[0], "p1"))
}

func newSourceCRDObjWithSpec(name, group, version, kind string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": crdGroup + "/" + crdVersion,
			"kind":       crdKind,
			"metadata": map[string]interface{}{
				"namespace": testNamespace,
				"name":      name,
			},
		},
	}
	obj.Object["spec"] = map[string]interface{}{
		"group":   group,
		"version": version,
		"names": map[string]interface{}{
			"kind":   kind,
			"plural": strings.ToLower(kind) + "s",
		},
	}
	obj.SetLabels(labels.Set{sourcesLabelKey: sourcesLabelValue})
	return obj
}

func newSourceUnstructuredObj(name, apiVersion, kind string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata": map[string]interface{}{
				"namespace": "current",
				"name":      name,
			},
			"spec": map[string]interface{}{
				"sink": map[string]interface{}{
					"ref": map[string]interface{}{
						"apiVersion": "serving.knative.dev/v1",
						"kind":       "Service",
						"name":       "foo",
					},
				},
			},
			"status": map[string]interface{}{
				"conditions": []interface{}{
					map[string]interface{}{
						"Type":   "Ready",
						"Status": "True",
					},
				},
			},
		},
	}
}

// sourceFakeCmdWithDynamicMock runs `kn source list` against a mock dynamic
// client and returns the captured output together with the execution error.
func sourceFakeCmdWithDynamicMock(t *testing.T, args []string, client clientdynamic.KnDynamicClient) (string, error) {
	t.Helper()
	knParams := &commands.KnParams{}
	buf := new(bytes.Buffer)
	knParams.Output = buf
	knParams.NewDynamicClient = func(namespace string) (clientdynamic.KnDynamicClient, error) {
		return client, nil
	}
	rootCmd := commands.NewTestCommand(NewSourceCommand(knParams), knParams)
	rootCmd.SetArgs(args)
	err := rootCmd.Execute()
	return buf.String(), err
}

func mockSourceListGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "client.knative.dev", Version: "v1alpha1", Kind: "SourceList"}
}

func mockSourceList(items ...*unstructured.Unstructured) *unstructured.UnstructuredList {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(mockSourceListGVK())
	for _, item := range items {
		list.Items = append(list.Items, *item)
	}
	return list
}

// TestSourceListPartialTypeError verifies the table still shows the confirmed
// source types while independent per-type failures are a non-success result
// naming the omitted type and reason.
func TestSourceListPartialTypeError(t *testing.T) {
	client := clientdynamic.NewMockKnDynamicClient(t, testNamespace)
	recorder := client.Recorder()
	partial := &clientdynamic.PartialListError{Types: []clientdynamic.TypeListError{{
		Type: "PingSource",
		GVR:  schema.GroupVersionResource{Group: "sources.knative.dev", Version: "v1", Resource: "pingsources"},
		Err: apierrors.NewForbidden(schema.GroupResource{Group: "sources.knative.dev", Resource: "pingsources"}, "p1", fmt.Errorf("denied by test")),
	}}}
	recorder.ListSources(mock.Any(),
		mockSourceList(newSourceUnstructuredObj("a1", "sources.knative.dev/v1", "ApiServerSource")),
		partial)

	output, err := sourceFakeCmdWithDynamicMock(t, []string{"source", "list", "--namespace", testNamespace}, client)
	assert.Check(t, err != nil)
	assert.Check(t, util.ContainsAll(err.Error(), "could not list", "PingSource", "pingsources.sources.knative.dev", "forbidden", "denied by test"))
	assert.Check(t, util.ContainsAll(output, "NAME", "a1", "ApiServerSource"))
	// the omitted type contributes no data row
	assert.Check(t, !strings.Contains(output, "p1"))
	recorder.Validate()
}

// TestSourceListPartialTypeErrorJson verifies machine-readable output keeps
// working while the omission is still reported as non-success.
func TestSourceListPartialTypeErrorJson(t *testing.T) {
	client := clientdynamic.NewMockKnDynamicClient(t, testNamespace)
	recorder := client.Recorder()
	partial := &clientdynamic.PartialListError{Types: []clientdynamic.TypeListError{{
		Type: "PingSource",
		GVR:  schema.GroupVersionResource{Group: "sources.knative.dev", Version: "v1", Resource: "pingsources"},
		Err: apierrors.NewServiceUnavailable("backend overloaded"),
	}}}
	recorder.ListSources(mock.Any(),
		mockSourceList(newSourceUnstructuredObj("a1", "sources.knative.dev/v1", "ApiServerSource")),
		partial)

	output, err := sourceFakeCmdWithDynamicMock(t, []string{"source", "list", "-o", "json", "--namespace", testNamespace}, client)
	assert.Check(t, err != nil)
	assert.Check(t, util.ContainsAll(err.Error(), "PingSource", "backend overloaded"))
	assert.Check(t, util.ContainsAll(output, "\"apiVersion\": \"client.knative.dev/v1alpha1\"", "\"kind\": \"SourceList\"", "\"name\": \"a1\""))
	recorder.Validate()
}

// TestSourceListPartialTypeErrorNoConfirmedSources verifies that with no
// confirmed sources left, the omission error is returned instead of a
// misleading "No sources found.".
func TestSourceListPartialTypeErrorNoConfirmedSources(t *testing.T) {
	client := clientdynamic.NewMockKnDynamicClient(t, testNamespace)
	recorder := client.Recorder()
	partial := &clientdynamic.PartialListError{Types: []clientdynamic.TypeListError{{
		Type: "PingSource",
		GVR:  schema.GroupVersionResource{Group: "sources.knative.dev", Version: "v1", Resource: "pingsources"},
		Err: apierrors.NewForbidden(schema.GroupResource{Group: "sources.knative.dev", Resource: "pingsources"}, "", fmt.Errorf("denied by test")),
	}}}
	recorder.ListSources(mock.Any(), mockSourceList(), partial)

	output, err := sourceFakeCmdWithDynamicMock(t, []string{"source", "list", "--namespace", testNamespace}, client)
	assert.Check(t, err != nil)
	assert.Check(t, util.ContainsAll(err.Error(), "PingSource", "forbidden"))
	assert.Check(t, !strings.Contains(output, "No sources found."))
	recorder.Validate()
}

// TestSourceListForbiddenFallback verifies the built-in GVK fallback stays
// successful when CRD access is forbidden and the built-in types list fine.
func TestSourceListForbiddenFallback(t *testing.T) {
	client := clientdynamic.NewMockKnDynamicClient(t, testNamespace)
	recorder := client.Recorder()
	recorder.ListSources(mock.Any(), nil,
		apierrors.NewForbidden(schema.GroupResource{Group: "apiextensions.k8s.io", Resource: "customresourcedefinitions"}, "", fmt.Errorf("crd denied")))
	recorder.ListSourcesUsingGVKs(mock.Any(), mock.Any(),
		mockSourceList(newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource")), nil)

	output, err := sourceFakeCmdWithDynamicMock(t, []string{"source", "list", "--namespace", testNamespace}, client)
	assert.NilError(t, err)
	assert.Check(t, util.ContainsAll(output, "NAME", "p1", "PingSource"))
	recorder.Validate()
}

// TestSourceListForbiddenFallbackNotInstalled verifies that built-in types
// that are not installed (GVR not served) while CRDs are unreadable do not
// turn into a non-success result; the installed types are still printed.
func TestSourceListForbiddenFallbackNotInstalled(t *testing.T) {
	client := clientdynamic.NewMockKnDynamicClient(t, testNamespace)
	recorder := client.Recorder()
	recorder.ListSources(mock.Any(), nil,
		apierrors.NewForbidden(schema.GroupResource{Group: "apiextensions.k8s.io", Resource: "customresourcedefinitions"}, "", fmt.Errorf("crd denied")))
	partial := &clientdynamic.PartialListError{Types: []clientdynamic.TypeListError{{
		Type: "ApiServerSource",
		GVR:  schema.GroupVersionResource{Group: "sources.knative.dev", Version: "v1", Resource: "apiserversources"},
		Err:  apierrors.NewNotFound(schema.GroupResource{Group: "sources.knative.dev", Resource: "apiserversources"}, ""),
	}}}
	recorder.ListSourcesUsingGVKs(mock.Any(), mock.Any(),
		mockSourceList(newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource")), partial)

	output, err := sourceFakeCmdWithDynamicMock(t, []string{"source", "list", "--namespace", testNamespace}, client)
	assert.NilError(t, err)
	assert.Check(t, util.ContainsAll(output, "p1", "PingSource"))
	recorder.Validate()
}

// TestSourceListForbiddenFallbackRealOmission verifies that a real per-type
// failure on the built-in fallback path keeps the confirmed types and is a
// non-success result naming the omitted type.
func TestSourceListForbiddenFallbackRealOmission(t *testing.T) {
	client := clientdynamic.NewMockKnDynamicClient(t, testNamespace)
	recorder := client.Recorder()
	recorder.ListSources(mock.Any(), nil,
		apierrors.NewForbidden(schema.GroupResource{Group: "apiextensions.k8s.io", Resource: "customresourcedefinitions"}, "", fmt.Errorf("crd denied")))
	partial := &clientdynamic.PartialListError{Types: []clientdynamic.TypeListError{
		{
			Type: "ApiServerSource",
			GVR:  schema.GroupVersionResource{Group: "sources.knative.dev", Version: "v1", Resource: "apiserversources"},
			Err:  apierrors.NewNotFound(schema.GroupResource{Group: "sources.knative.dev", Resource: "apiserversources"}, ""),
		},
		{
			Type: "SinkBinding",
			GVR:  schema.GroupVersionResource{Group: "sources.knative.dev", Version: "v1", Resource: "sinkbindings"},
			Err:  apierrors.NewServiceUnavailable("backend overloaded"),
		},
	}}
	recorder.ListSourcesUsingGVKs(mock.Any(), mock.Any(),
		mockSourceList(newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource")), partial)

	output, err := sourceFakeCmdWithDynamicMock(t, []string{"source", "list", "--namespace", testNamespace}, client)
	assert.Check(t, err != nil)
	assert.Check(t, util.ContainsAll(err.Error(), "SinkBinding", "backend overloaded"))
	// the not-installed type is not reported as an omission
	assert.Check(t, !strings.Contains(err.Error(), "ApiServerSource"))
	assert.Check(t, util.ContainsAll(output, "p1", "PingSource"))
	recorder.Validate()
}

func TestSourceListAllNamespace(t *testing.T) {
	output, err := sourceFakeCmd([]string{"source", "list", "--all-namespaces"},
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
		newSourceCRDObjWithSpec("sinkbindings", "sources.knative.dev", "v1", "SinkBinding"),
		newSourceCRDObjWithSpec("apiserversources", "sources.knative.dev", "v1", "ApiServerSource"),
		newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource"),
		newSourceUnstructuredObj("s1", "sources.knative.dev/v1", "SinkBinding"),
		newSourceUnstructuredObj("a1", "sources.knative.dev/v1", "ApiServerSource"),
	)
	assert.NilError(t, err)
	assert.Check(t, util.ContainsAll(output[0], "NAMESPACE", "NAME", "TYPE", "RESOURCE", "SINK", "READY"))
	assert.Check(t, util.ContainsAll(output[1], "current", "a1", "ApiServerSource", "apiserversources.sources.knative.dev", "ksvc:foo", "True"))
	assert.Check(t, util.ContainsAll(output[2], "current", "p1", "PingSource", "pingsources.sources.knative.dev", "ksvc:foo", "True"))
	assert.Check(t, util.ContainsAll(output[3], "current", "s1", "SinkBinding", "sinkbindings.sources.knative.dev", "ksvc:foo", "True"))
}
