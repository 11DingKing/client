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

package dynamic

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"gotest.tools/v3/assert"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/pkg/apis/messaging"
	messagingv1 "knative.dev/eventing/pkg/apis/messaging/v1"
	sourcesv1 "knative.dev/eventing/pkg/apis/sources/v1"
	dynamicclientfake "knative.dev/pkg/injection/clients/dynamicclient/fake"
	servingv1 "knative.dev/serving/pkg/apis/serving/v1"

	"knative.dev/client/pkg/util"
)

const testNamespace = "current"

func TestNamespace(t *testing.T) {
	client := createFakeKnDynamicClient(testNamespace, newSourceCRDObj("foo"))
	assert.Equal(t, client.Namespace(), testNamespace)
}

func TestListCRDs(t *testing.T) {
	client := createFakeKnDynamicClient(
		testNamespace,
		newSourceCRDObj("foo"),
		newSourceCRDObj("bar"),
	)
	assert.Check(t, client.RawClient() != nil)

	t.Run("List CRDs with match", func(t *testing.T) {
		options := metav1.ListOptions{}
		uList, err := client.ListCRDs(context.Background(), options)
		assert.NilError(t, err)
		assert.Equal(t, len(uList.Items), 2)
	})

	t.Run("List CRDs without match", func(t *testing.T) {
		options := metav1.ListOptions{}
		sourcesLabels := labels.Set{"duck.knative.dev/source": "true1"}
		options.LabelSelector = sourcesLabels.String()
		uList, err := client.ListCRDs(context.Background(), options)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, len(uList.Items), 0)
	})
}

func TestListSourceTypes(t *testing.T) {
	client := createFakeKnDynamicClient(
		testNamespace,
		newSourceCRDObj("foo"),
		newSourceCRDObj("bar"),
	)

	t.Run("List source types", func(t *testing.T) {
		uList, err := client.ListSourcesTypes(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, len(uList.Items), 2)
		// List of objects is returned in sorted order according to the (ns first, then name)
		assert.Equal(t, uList.Items[0].GetName(), "bar")
		assert.Equal(t, uList.Items[1].GetName(), "foo")
	})
}

func TestListSources(t *testing.T) {
	t.Run("No GVRs set", func(t *testing.T) {
		obj := newSourceCRDObj("foo")
		client := createFakeKnDynamicClient(testNamespace, obj)
		assert.Check(t, client.RawClient() != nil)
		_, err := client.ListSources(context.Background())
		assert.Check(t, err != nil)
		assert.Check(t, util.ContainsAll(err.Error(), "can't", "find", "source", "kind", "CRD"))
	})

	t.Run("sources not installed", func(t *testing.T) {
		client := createFakeKnDynamicClient(testNamespace)
		_, err := client.ListSources(context.Background())
		assert.Check(t, err != nil)
		assert.Check(t, util.ContainsAll(err.Error(), "no sources", "found", "backend", "verify", "installation"))
	})

	t.Run("source list empty", func(t *testing.T) {
		client := createFakeKnDynamicClient(testNamespace,
			newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
		)
		sources, err := client.ListSources(context.Background())
		assert.NilError(t, err)
		assert.Equal(t, len(sources.Items), 0)
	})

	t.Run("source list non empty", func(t *testing.T) {
		client := createFakeKnDynamicClient(testNamespace,
			newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
			newSourceCRDObjWithSpec("apiserversources", "sources.knative.dev", "v1", "ApiServerSource"),
			newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource"),
			newSourceUnstructuredObj("a1", "sources.knative.dev/v1", "ApiServerSource"),
		)
		sources, err := client.ListSources(context.Background(), WithTypeFilter("pingsource"), WithTypeFilter("ApiServerSource"))
		assert.NilError(t, err)
		assert.Equal(t, len(sources.Items), 2)
		assert.DeepEqual(t, sources.GroupVersionKind(), schema.GroupVersionKind{Group: sourceListGroup, Version: sourceListVersion, Kind: sourceListKind})
	})
}

func TestListSourcesUsingGVKs(t *testing.T) {
	t.Run("No GVKs given", func(t *testing.T) {
		client := createFakeKnDynamicClient(testNamespace)
		assert.Check(t, client.RawClient() != nil)
		s, err := client.ListSourcesUsingGVKs(context.Background(), nil)
		assert.NilError(t, err)
		assert.Check(t, s == nil)
	})

	t.Run("source list with given GVKs", func(t *testing.T) {
		client := createFakeKnDynamicClient(testNamespace,
			newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
			newSourceCRDObjWithSpec("apiserversources", "sources.knative.dev", "v1", "ApiServerSource"),
			newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource"),
			newSourceUnstructuredObj("a1", "sources.knative.dev/v1", "ApiServerSource"),
		)
		assert.Check(t, client.RawClient() != nil)
		gvks := []schema.GroupVersionKind{
			{Group: "sources.knative.dev", Version: "v1", Kind: "PingSource"},
			{Group: "sources.knative.dev", Version: "v1", Kind: "ApiServerSource"},
		}

		s, err := client.ListSourcesUsingGVKs(context.Background(), &gvks)
		assert.NilError(t, err)
		if s == nil {
			t.Fatal("s = nil, want not nil")
		}
		assert.Equal(t, len(s.Items), 2)
		assert.DeepEqual(t, s.GroupVersionKind(), schema.GroupVersionKind{Group: sourceListGroup, Version: sourceListVersion, Kind: sourceListKind})

		// withType
		s, err = client.ListSourcesUsingGVKs(context.Background(), &gvks, WithTypeFilter("PingSource"))
		assert.NilError(t, err)
		if s == nil {
			t.Fatal("s = nil, want not nil")
		}
		assert.Equal(t, len(s.Items), 1)
		assert.DeepEqual(t, s.GroupVersionKind(), schema.GroupVersionKind{Group: sourceListGroup, Version: sourceListVersion, Kind: sourceListKind})
	})

}

// createFakeKnDynamicClient gives you a dynamic client for testing containing the given objects.
// See also the one in the fake package. Duplicated here to avoid a dependency loop.
func createFakeKnDynamicClient(testNamespace string, objects ...runtime.Object) KnDynamicClient {
	scheme := runtime.NewScheme()
	servingv1.AddToScheme(scheme)
	eventingv1.AddToScheme(scheme)
	messagingv1.AddToScheme(scheme)
	sourcesv1.AddToScheme(scheme)
	sourcesv1.AddToScheme(scheme)
	apiextensionsv1.AddToScheme(scheme)
	_, dynamicClient := dynamicclientfake.With(context.TODO(), scheme, objects...)
	return NewKnDynamicClient(dynamicClient, testNamespace)
}

func newSourceCRDObj(name string) *unstructured.Unstructured {
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
	obj.SetLabels(labels.Set{sourcesLabelKey: sourcesLabelValue})
	return obj
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
						"kind": "Service",
						"name": "foo",
					},
				},
			},
		},
	}
}

func newChannelCRDObj(name string) *unstructured.Unstructured {
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
	obj.SetLabels(labels.Set{messaging.SubscribableDuckVersionAnnotation: channelLabelValue})
	return obj
}

func newChannelCRDObjWithSpec(name, group, version, kind string) *unstructured.Unstructured {
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
	obj.SetLabels(labels.Set{messaging.SubscribableDuckVersionAnnotation: channelLabelValue})
	return obj
}

func newChannelUnstructuredObj(name, apiVersion, kind string) *unstructured.Unstructured {
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
						"name": "foo",
					},
				},
			},
		},
	}
}
func TestListChannelsTypes(t *testing.T) {
	t.Run("List channel types", func(t *testing.T) {
		client := createFakeKnDynamicClient(
			testNamespace,
			newChannelCRDObjWithSpec("Channel", "messaging.knative.dev", "v1", "Channel"),
			newChannelCRDObjWithSpec("InMemoryChannel", "messaging.knative.dev", "v1", "InMemoryChannel"),
		)

		uList, err := client.ListChannelsTypes(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, len(uList.Items), 1)
		assert.Equal(t, uList.Items[0].GetName(), "InMemoryChannel")
	})

	t.Run("List channel types error", func(t *testing.T) {
		client := createFakeKnDynamicClient(
			testNamespace,
			newChannelCRDObj("foo"),
		)
		uList, err := client.ListChannelsTypes(context.Background())
		assert.Check(t, err == nil)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, len(uList.Items), 1)
		assert.Equal(t, uList.Items[0].GetName(), "foo")
	})
}

func TestListChannelsUsingGVKs(t *testing.T) {
	t.Run("No GVKs given", func(t *testing.T) {
		client := createFakeKnDynamicClient(testNamespace)
		assert.Check(t, client.RawClient() != nil)
		s, err := client.ListChannelsUsingGVKs(context.Background(), nil)
		assert.NilError(t, err)
		assert.Check(t, s == nil)
	})

	t.Run("channel list with given GVKs", func(t *testing.T) {
		client := createFakeKnDynamicClient(testNamespace,
			newChannelCRDObjWithSpec("InMemoryChannel", "messaging.knative.dev", "v1", "InMemoryChannel"),
			newChannelUnstructuredObj("i1", "messaging.knative.dev/v1", "InMemoryChannel"),
		)
		assert.Check(t, client.RawClient() != nil)
		gv := schema.GroupVersion{Group: "messaging.knative.dev", Version: "v1"}
		gvks := []schema.GroupVersionKind{gv.WithKind("InMemoryChannel")}

		s, err := client.ListChannelsUsingGVKs(context.Background(), &gvks)
		assert.NilError(t, err)
		if s == nil {
			t.Fatal("s = nil, want not nil")
		}
		assert.Equal(t, len(s.Items), 1)
		assert.DeepEqual(t, s.GroupVersionKind(), schema.GroupVersionKind{Group: messaging.GroupName, Version: channelListVersion, Kind: channelListKind})

		// withType
		s, err = client.ListChannelsUsingGVKs(context.Background(), &gvks, WithTypeFilter("InMemoryChannel"))
		assert.NilError(t, err)
		if s == nil {
			t.Fatal("s = nil, want not nil")
		}
		assert.Equal(t, len(s.Items), 1)
		assert.DeepEqual(t, s.GroupVersionKind(), schema.GroupVersionKind{Group: messaging.GroupName, Version: channelListVersion, Kind: channelListKind})
	})

}

// rawFakeDynamic extracts the fake dynamic client used to inject reactors.
func rawFakeDynamic(t *testing.T, client KnDynamicClient) *fakedynamic.FakeDynamicClient {
	t.Helper()
	raw, ok := client.RawClient().(*fakedynamic.FakeDynamicClient)
	if !ok {
		t.Fatalf("raw dynamic client is %T, want *fake.FakeDynamicClient", client.RawClient())
	}
	return raw
}

// expiredResourceVersionError mimics the API server response for a
// continuation token whose resourceVersion expired.
func expiredResourceVersionError() error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status: metav1.StatusFailure,
		Code:   http.StatusGone,
		Reason: metav1.StatusReasonExpired,
		Message: "too old resource version: 1234 (5678)",
	}}
}

func sourceGR(plural string) schema.GroupResource {
	return schema.GroupResource{Group: "sources.knative.dev", Resource: plural}
}

// TestListSourcesPartialTypeError verifies that an independent error of one
// source type does not fail the whole listing: the confirmed types are
// returned together with a PartialListError naming the omitted type.
func TestListSourcesPartialTypeError(t *testing.T) {
	client := createFakeKnDynamicClient(testNamespace,
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
		newSourceCRDObjWithSpec("apiserversources", "sources.knative.dev", "v1", "ApiServerSource"),
		newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource"),
		newSourceUnstructuredObj("a1", "sources.knative.dev/v1", "ApiServerSource"),
	)
	rawFakeDynamic(t, client).PrependReactor("list", "pingsources",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(sourceGR("pingsources"), "p1", fmt.Errorf("denied by test"))
		})

	sources, err := client.ListSources(context.Background())
	assert.Check(t, err != nil)
	partial := AsPartialListError(err)
	if partial == nil {
		t.Fatalf("err = %v, want *PartialListError", err)
	}
	assert.Equal(t, len(partial.Types), 1)
	assert.Equal(t, partial.Types[0].Type, "PingSource")
	assert.Equal(t, partial.Types[0].GVR.Resource, "pingsources")
	assert.Check(t, util.ContainsAll(partial.Types[0].Err.Error(), "forbidden", "denied by test"))
	// the confirmed type is still returned and sorted
	assert.Equal(t, len(sources.Items), 1)
	assert.Equal(t, sources.Items[0].GetName(), "a1")
}

// TestListSourcesTemporaryTypeError verifies a 5xx on one type is partial.
func TestListSourcesTemporaryTypeError(t *testing.T) {
	client := createFakeKnDynamicClient(testNamespace,
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
		newSourceCRDObjWithSpec("apiserversources", "sources.knative.dev", "v1", "ApiServerSource"),
		newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource"),
	)
	rawFakeDynamic(t, client).PrependReactor("list", "apiserversources",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewServiceUnavailable("backend overloaded")
		})

	sources, err := client.ListSources(context.Background())
	partial := AsPartialListError(err)
	if partial == nil {
		t.Fatalf("err = %v, want *PartialListError", err)
	}
	assert.Equal(t, partial.Types[0].Type, "ApiServerSource")
	assert.Assert(t, apierrors.IsServiceUnavailable(partial.Types[0].Err))
	assert.Check(t, util.ContainsAll(partial.Types[0].Err.Error(), "backend overloaded"))
	assert.Equal(t, len(sources.Items), 1)
	assert.Equal(t, sources.Items[0].GetName(), "p1")
}

// TestListSourcesVanishedTypeRestarts verifies that a GVR disappearing
// between discovery and per-type listing refreshes the discovery view and
// re-runs the whole aggregation from the new view instead of dropping the
// vanished type or failing.
func TestListSourcesVanishedTypeRestarts(t *testing.T) {
	client := createFakeKnDynamicClient(testNamespace,
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
		newSourceCRDObjWithSpec("apiserversources", "sources.knative.dev", "v1", "ApiServerSource"),
		newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource"),
		newSourceUnstructuredObj("a1", "sources.knative.dev/v1", "ApiServerSource"),
	)
	pingListCalls := 0
	rawFakeDynamic(t, client).PrependReactor("list", "pingsources",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			pingListCalls++
			if pingListCalls == 1 {
				// CRD got uninstalled after the CRD discovery of round 1
				return true, nil, apierrors.NewNotFound(sourceGR("pingsources"), "")
			}
			// round 2 runs against the refreshed view and succeeds
			return false, nil, nil
		})

	sources, err := client.ListSources(context.Background())
	assert.NilError(t, err)
	if sources == nil {
		t.Fatal("sources = nil, want not nil")
	}
	assert.Equal(t, len(sources.Items), 2)
	assert.Equal(t, pingListCalls, 2)
	assert.Equal(t, sources.Items[0].GetName(), "a1")
	assert.Equal(t, sources.Items[1].GetName(), "p1")
}

// TestListSourcesVanishedTypeKeepsFailing verifies the round budget: a view
// that keeps invalidating does not loop forever and yields a clear error.
func TestListSourcesVanishedTypeKeepsFailing(t *testing.T) {
	client := createFakeKnDynamicClient(testNamespace,
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
		newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource"),
	)
	rawFakeDynamic(t, client).PrependReactor("list", "pingsources",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewNotFound(sourceGR("pingsources"), "")
		})

	sources, err := client.ListSources(context.Background())
	assert.Check(t, err != nil)
	assert.Check(t, sources == nil)
	assert.Check(t, util.ContainsAll(err.Error(), "unable to list sources", "changing", "try again"))
}

// TestListSourcesExpiredRoundRestart verifies that an expired continuation
// token (410) discards the whole round aggregation and restarts from a fresh
// first page.
func TestListSourcesExpiredRoundRestart(t *testing.T) {
	client := createFakeKnDynamicClient(testNamespace,
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
		newSourceCRDObjWithSpec("apiserversources", "sources.knative.dev", "v1", "ApiServerSource"),
		newSourceUnstructuredObj("a1", "sources.knative.dev/v1", "ApiServerSource"),
	)
	// pingsources is served in two pages; the second page expires in round 1
	// and succeeds in round 2. Calls 1 and 3 are the first pages of round 1
	// and the restarted round 2, call 2 is the expired continuation and
	// call 4 the successful second page.
	pingPage2 := newSourceUnstructuredObj("p2", "sources.knative.dev/v1", "PingSource")
	listCalls := 0
	rawFakeDynamic(t, client).PrependReactor("list", "pingsources",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			listCalls++
			switch listCalls {
			case 1, 3:
				page := &unstructured.UnstructuredList{}
				page.SetContinue("page-2-token")
				page.Items = append(page.Items, *newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource"))
				return true, page, nil
			case 2:
				return true, nil, expiredResourceVersionError()
			default:
				page := &unstructured.UnstructuredList{}
				page.Items = append(page.Items, *pingPage2)
				return true, page, nil
			}
		})

	sources, err := client.ListSources(context.Background())
	assert.NilError(t, err)
	if sources == nil {
		t.Fatal("sources = nil, want not nil")
	}
	// round 1 aggregation (a1, p1) was discarded; the final round maps to
	// one valid view and yields each object exactly once, sorted
	assert.Equal(t, len(sources.Items), 3)
	assert.Equal(t, sources.Items[0].GetName(), "a1")
	assert.Equal(t, sources.Items[1].GetName(), "p1")
	assert.Equal(t, sources.Items[2].GetName(), "p2")
	assert.Equal(t, listCalls, 4)
}

// TestListSourcesExpiredRestartsSimple covers the non-paged 410 path.
func TestListSourcesExpiredRestartsSimple(t *testing.T) {
	client := createFakeKnDynamicClient(testNamespace,
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
		newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource"),
	)
	listCalls := 0
	rawFakeDynamic(t, client).PrependReactor("list", "pingsources",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			listCalls++
			if listCalls == 1 {
				return true, nil, expiredResourceVersionError()
			}
			return false, nil, nil
		})

	sources, err := client.ListSources(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, len(sources.Items), 1)
	assert.Equal(t, sources.Items[0].GetName(), "p1")
	assert.Equal(t, listCalls, 2)
}

// TestListSourcesDeduplicateAndSort verifies the final aggregation is
// deduplicated (same object surfacing through overlapping GVRs) and ordered
// stably by namespace/name/type.
func TestListSourcesDeduplicateAndSort(t *testing.T) {
	dupV1 := *newSourceUnstructuredObj("x1", "sources.knative.dev/v1", "PingSource")
	dupV1.SetUID(types.UID("uid-1"))
	dupV1alpha1 := *newSourceUnstructuredObj("x1", "sources.knative.dev/v1alpha1", "PingSource")
	dupV1alpha1.SetUID(types.UID("uid-1"))
	other := *newSourceUnstructuredObj("z2", "sources.knative.dev/v1", "ApiServerSource")
	other.SetUID(types.UID("uid-2"))
	otherNs := *newSourceUnstructuredObj("n3", "sources.knative.dev/v1", "PingSource")
	otherNs.SetUID(types.UID("uid-3"))
	otherNs.SetNamespace("other")
	// objects without UID fall back to namespaced GVK coordinates
	noUIDA := *newSourceUnstructuredObj("q4", "sources.knative.dev/v1", "PingSource")
	noUIDB := *newSourceUnstructuredObj("q4", "sources.knative.dev/v1", "PingSource")
	noUIDOtherVersion := *newSourceUnstructuredObj("q4", "sources.knative.dev/v1alpha1", "PingSource")

	items := deduplicateAndSortSourceItems([]unstructured.Unstructured{
		other, noUIDA, dupV1alpha1, otherNs, noUIDOtherVersion, dupV1, noUIDB,
	})
	assert.Equal(t, len(items), 5)
	// same namespaced name on different GVKs are distinct objects; the exact
	// duplicate (same GVK, no UID) is collapsed
	assert.Equal(t, items[0].GetNamespace(), "current")
	assert.Equal(t, items[0].GetName(), "q4")
	assert.Equal(t, items[1].GetName(), "q4")
	assert.Assert(t, items[0].GetAPIVersion() != items[1].GetAPIVersion())
	assert.Equal(t, items[2].GetName(), "x1")
	assert.Equal(t, items[3].GetName(), "z2")
	assert.Equal(t, items[4].GetNamespace(), "other")
	assert.Equal(t, items[4].GetName(), "n3")
}

// TestListSourcesUsingGVKsNotInstalledAndFailing verifies the built-in
// fallback path: a not-served GVR (type not installed) is reported as a
// not-installed omission while a temporary failure of another type is a
// regular omission, and confirmed types still come back.
func TestListSourcesUsingGVKsNotInstalledAndFailing(t *testing.T) {
	client := createFakeKnDynamicClient(testNamespace,
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
		newSourceCRDObjWithSpec("apiserversources", "sources.knative.dev", "v1", "ApiServerSource"),
		newSourceCRDObjWithSpec("sinkbindings", "sources.knative.dev", "v1", "SinkBinding"),
		newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource"),
		newSourceUnstructuredObj("s1", "sources.knative.dev/v1", "SinkBinding"),
	)
	raw := rawFakeDynamic(t, client)
	raw.PrependReactor("list", "apiserversources",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewNotFound(sourceGR("apiserversources"), "")
		})
	raw.PrependReactor("list", "sinkbindings",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewServiceUnavailable("backend overloaded")
		})

	gvks := []schema.GroupVersionKind{
		{Group: "sources.knative.dev", Version: "v1", Kind: "PingSource"},
		{Group: "sources.knative.dev", Version: "v1", Kind: "ApiServerSource"},
		{Group: "sources.knative.dev", Version: "v1", Kind: "SinkBinding"},
	}
	sources, err := client.ListSourcesUsingGVKs(context.Background(), &gvks)
	partial := AsPartialListError(err)
	if partial == nil {
		t.Fatalf("err = %v, want *PartialListError", err)
	}
	assert.Equal(t, len(partial.Types), 2)
	byType := map[string]TypeListError{}
	for _, tErr := range partial.Types {
		byType[tErr.Type] = tErr
	}
	notInstalled, ok := byType["ApiServerSource"]
	assert.Assert(t, ok)
	assert.Assert(t, IsTypeNotInstalled(notInstalled.Err))
	temporary, ok := byType["SinkBinding"]
	assert.Assert(t, ok)
	assert.Assert(t, !IsTypeNotInstalled(temporary.Err))
	assert.Assert(t, apierrors.IsServiceUnavailable(temporary.Err))
	assert.Equal(t, len(sources.Items), 1)
	assert.Equal(t, sources.Items[0].GetName(), "p1")
}

// TestListSourcesUsingGVKsExpiredRestarts verifies round restarts on the
// fixed GVK view.
func TestListSourcesUsingGVKsExpiredRestarts(t *testing.T) {
	client := createFakeKnDynamicClient(testNamespace,
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
		newSourceUnstructuredObj("p1", "sources.knative.dev/v1", "PingSource"),
	)
	listCalls := 0
	rawFakeDynamic(t, client).PrependReactor("list", "pingsources",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			listCalls++
			if listCalls == 1 {
				return true, nil, expiredResourceVersionError()
			}
			return false, nil, nil
		})

	gvks := []schema.GroupVersionKind{{Group: "sources.knative.dev", Version: "v1", Kind: "PingSource"}}
	sources, err := client.ListSourcesUsingGVKs(context.Background(), &gvks)
	assert.NilError(t, err)
	assert.Equal(t, len(sources.Items), 1)
	assert.Equal(t, sources.Items[0].GetName(), "p1")
	assert.Equal(t, listCalls, 2)
}

// TestPartialListErrorTypeFilter verifies per-type omissions honor the
// requested type filters: unfiltered types are never contacted.
func TestListSourcesPartialErrorWithFilter(t *testing.T) {
	client := createFakeKnDynamicClient(testNamespace,
		newSourceCRDObjWithSpec("pingsources", "sources.knative.dev", "v1", "PingSource"),
		newSourceCRDObjWithSpec("apiserversources", "sources.knative.dev", "v1", "ApiServerSource"),
		newSourceUnstructuredObj("a1", "sources.knative.dev/v1", "ApiServerSource"),
	)
	raw := rawFakeDynamic(t, client)
	raw.PrependReactor("list", "pingsources",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			t.Error("pingsources must not be listed when filtered out")
			return true, nil, nil
		})

	sources, err := client.ListSources(context.Background(), WithTypeFilter("ApiServerSource"))
	assert.NilError(t, err)
	assert.Equal(t, len(sources.Items), 1)
	assert.Equal(t, sources.Items[0].GetName(), "a1")
}
