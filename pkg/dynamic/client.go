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
	"errors"
	"fmt"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"knative.dev/client/pkg/util"
	"knative.dev/eventing/pkg/apis/messaging"
)

const (
	crdGroup           = "apiextensions.k8s.io"
	crdVersion         = "v1"
	crdKind            = "CustomResourceDefinition"
	crdKinds           = "customresourcedefinitions"
	sourcesLabelKey    = "duck.knative.dev/source"
	sourcesLabelValue  = "true"
	sourceListGroup    = "client.knative.dev"
	sourceListVersion  = "v1alpha1"
	sourceListKind     = "SourceList"
	channelLabelValue  = "true"
	channelListVersion = "v1"
	channelListKind    = "ChannelList"
	channelKind        = "Channel"
)

const (
	// sourceListChunkSize is the page size used while listing objects of a
	// single source type. Paginating long lists bounds the lifetime of the
	// server-side resourceVersion anchored in the continuation token.
	sourceListChunkSize int64 = 500
	// maxSourceListRounds bounds restarts from a stale discovery view (CRD
	// vanished between discovery and per-type listing) or from an expired
	// paging resourceVersion. Every round starts from a fresh discovery view.
	maxSourceListRounds = 3
)

// errRestartDiscoveryRound signals that the discovery view used by a list
// round got invalid and the whole aggregation round must be discarded and
// restarted from a freshly discovered view.
var errRestartDiscoveryRound = errors.New("source discovery view changed, restarting list round")

// KnDynamicClient to client-go Dynamic client. All methods are relative to the
// namespace specified during construction
type KnDynamicClient interface {
	// Namespace in which this client is operating for
	Namespace() string

	// ListCRDs returns list of CRDs with their type and name
	ListCRDs(ctx context.Context, options metav1.ListOptions) (*unstructured.UnstructuredList, error)

	// ListSourcesTypes returns list of eventing sources CRDs
	ListSourcesTypes(ctx context.Context) (*unstructured.UnstructuredList, error)

	// ListSources returns list of available source objects
	ListSources(ctx context.Context, types ...WithType) (*unstructured.UnstructuredList, error)

	// ListSourcesUsingGVKs returns list of available source objects using given list of GVKs
	ListSourcesUsingGVKs(context.Context, *[]schema.GroupVersionKind, ...WithType) (*unstructured.UnstructuredList, error)

	// ListChannelsTypes returns installed knative channel CRDs
	ListChannelsTypes(ctx context.Context) (*unstructured.UnstructuredList, error)

	// ListChannelsUsingGVKs returns list of available channel objects using given list of GVKs
	ListChannelsUsingGVKs(context.Context, *[]schema.GroupVersionKind, ...WithType) (*unstructured.UnstructuredList, error)

	// RawClient returns the raw dynamic client interface
	RawClient() dynamic.Interface
}

// knDynamicClient is a combination of client-go Dynamic client interface and namespace
type knDynamicClient struct {
	client    dynamic.Interface
	namespace string
}

// NewKnDynamicClient is to invoke Eventing Sources Client API to create object
func NewKnDynamicClient(client dynamic.Interface, namespace string) KnDynamicClient {
	return &knDynamicClient{
		client:    client,
		namespace: namespace,
	}
}

// Return the client's namespace
func (c *knDynamicClient) Namespace() string {
	return c.namespace
}

// TODO(navidshaikh): Use ListConfigs here instead of ListOptions
// ListCRDs returns list of installed CRDs in the cluster and filters based on the given options
func (c *knDynamicClient) ListCRDs(ctx context.Context, options metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	gvr := schema.GroupVersionResource{
		Group:    crdGroup,
		Version:  crdVersion,
		Resource: crdKinds,
	}

	uList, err := c.client.Resource(gvr).List(ctx, options)
	if err != nil {
		return nil, err
	}

	return uList, nil
}

// ListSourcesTypes returns installed knative eventing sources CRDs
func (c *knDynamicClient) ListSourcesTypes(ctx context.Context) (*unstructured.UnstructuredList, error) {
	options := metav1.ListOptions{}
	sourcesLabels := labels.Set{sourcesLabelKey: sourcesLabelValue}
	options.LabelSelector = sourcesLabels.String()
	return c.ListCRDs(ctx, options)
}

// ListChannelsTypes returns installed knative channel CRDs
func (c *knDynamicClient) ListChannelsTypes(ctx context.Context) (*unstructured.UnstructuredList, error) {
	var ChannelTypeList unstructured.UnstructuredList
	options := metav1.ListOptions{}
	channelsLabels := labels.Set{messaging.SubscribableDuckVersionAnnotation: channelLabelValue}
	options.LabelSelector = channelsLabels.String()
	uList, err := c.ListCRDs(ctx, options)
	if err != nil {
		return nil, err
	}
	ChannelTypeList.Object = uList.Object
	for _, channelType := range uList.Items {
		content := channelType.UnstructuredContent()
		channelTypeKind, _, err := unstructured.NestedString(content, "spec", "names", "kind")
		if err != nil {
			return nil, err
		}
		if !util.SliceContainsIgnoreCase([]string{channelKind}, channelTypeKind) {
			ChannelTypeList.Items = append(ChannelTypeList.Items, channelType)
		}
	}
	return &ChannelTypeList, nil
}

func (c knDynamicClient) RawClient() dynamic.Interface {
	return c.client
}

// ListSources returns list of available sources objects
// Provide the list of source types as for example: WithTypes("pingsource", "apiserversource"...) to list
// only given types of source objects
//
// The whole list operation is bound to a single, retryable discovery view
// (the set of source CRDs discovered in the cluster, each mapped to its
// currently served GVR). When a discovered GVR stops being served while its
// objects are listed (e.g. the source CRD is uninstalled concurrently) or a
// continuation token's resourceVersion expires on a long list, the whole
// aggregation round is discarded and the listing restarts from a freshly
// discovered view. Independent errors of a single source type (temporary or
// permission errors) do not fail the whole listing: the other confirmed
// source types are returned together with a *PartialListError listing the
// omitted types and their reasons.
func (c *knDynamicClient) ListSources(ctx context.Context, types ...WithType) (*unstructured.UnstructuredList, error) {
	filters := WithTypes(types).List()

	var items []unstructured.Unstructured
	var partial *PartialListError
	for round := 0; round < maxSourceListRounds; round++ {
		sourceTypes, err := c.ListSourcesTypes(ctx)
		if err != nil {
			// A forbidden error on listing CRDs is returned unchanged so that
			// callers can fall back to the built-in source GVKs.
			return nil, err
		}

		if sourceTypes == nil || len(sourceTypes.Items) == 0 {
			return nil, errors.New("no sources found on the backend, please verify the installation")
		}

		view, err := newSourceDiscoveryView(sourceTypes, filters)
		if err != nil {
			return nil, err
		}

		items, partial, err = c.listSourceView(ctx, view, sourceViewModeDiscovery)
		switch {
		case errors.Is(err, errRestartDiscoveryRound):
			// A discovered CRD vanished (or a paging resourceVersion expired)
			// during this round: discard the whole aggregation, including any
			// partial failures, and restart from a fresh discovery view.
			items, partial = nil, nil
			continue
		case err != nil:
			return nil, err
		}
		if partial != nil {
			return newSourceList(items), partial
		}
		return newSourceList(items), nil
	}
	return nil, errors.New("unable to list sources: the set of source types kept changing while listing, please try again")
}

// ListSourcesUsingGVKs returns list of available source objects using given list of GVKs
//
// As CRDs cannot be read on this code path (the caller fell back because of
// restricted CRD access), there is no discovery view to refresh: a GVR that
// is not served means the corresponding built-in source type is not
// installed and is reported as a missing type instead of invalidating the
// round. Expired paging resourceVersions still restart the whole aggregation
// round. Other independent per-type errors result in a *PartialListError
// while the remaining types are returned.
func (c *knDynamicClient) ListSourcesUsingGVKs(ctx context.Context, gvks *[]schema.GroupVersionKind, types ...WithType) (*unstructured.UnstructuredList, error) {
	if gvks == nil {
		return nil, nil
	}

	filters := WithTypes(types).List()
	view := make([]sourceDiscoveryViewEntry, 0, len(*gvks))
	for _, gvk := range *gvks {
		if len(filters) > 0 && !util.SliceContainsIgnoreCase(filters, gvk.Kind) {
			continue
		}
		view = append(view, sourceDiscoveryViewEntry{
			kind: gvk.Kind,
			gvr:  gvk.GroupVersion().WithResource(strings.ToLower(gvk.Kind) + "s"),
		})
	}

	var items []unstructured.Unstructured
	var partial *PartialListError
	for round := 0; round < maxSourceListRounds; round++ {
		var err error
		items, partial, err = c.listSourceView(ctx, view, sourceViewModeBuiltinFallback)
		switch {
		case errors.Is(err, errRestartDiscoveryRound):
			// The resourceVersion of a paged list expired: discard the whole
			// round and list the fixed GVK view again.
			items, partial = nil, nil
			continue
		case err != nil:
			return nil, err
		}
		if partial != nil {
			return newSourceList(items), partial
		}
		return newSourceList(items), nil
	}
	return nil, errors.New("unable to list sources: resource versions kept expiring while listing, please try again")
}

// sourceViewMode controls how a list round reacts when a discovered GVR
// stops being served.
type sourceViewMode int

const (
	// sourceViewModeDiscovery: the view was built from the source CRDs, so a
	// vanished GVR invalidates the round and triggers a rediscovery + restart.
	sourceViewModeDiscovery sourceViewMode = iota
	// sourceViewModeBuiltinFallback: the view is a fixed built-in GVK list
	// used while CRD access is forbidden; a not-served GVR means the source
	// type is not installed and is reported as a missing type.
	sourceViewModeBuiltinFallback
)

// sourceDiscoveryViewEntry binds a source kind to the GVR discovered for one
// aggregation round. All objects of a returned list correspond to entries of
// the same round's view.
type sourceDiscoveryViewEntry struct {
	kind string
	gvr  schema.GroupVersionResource
}

// newSourceDiscoveryView builds the retryable discovery view for one list
// round from the discovered source CRDs.
func newSourceDiscoveryView(sourceTypes *unstructured.UnstructuredList, filters []string) ([]sourceDiscoveryViewEntry, error) {
	view := make([]sourceDiscoveryViewEntry, 0, len(sourceTypes.Items))
	for i := range sourceTypes.Items {
		source := &sourceTypes.Items[i]
		// find source kind before hand to fail early
		sourceKind, err := kindFromUnstructured(source)
		if err != nil {
			return nil, err
		}

		if len(filters) > 0 && !util.SliceContainsIgnoreCase(filters, sourceKind) {
			continue
		}

		// find source's GVR from unstructured source type object
		gvr, err := gvrFromUnstructured(source)
		if err != nil {
			return nil, err
		}

		view = append(view, sourceDiscoveryViewEntry{kind: sourceKind, gvr: gvr})
	}
	return view, nil
}

// listSourceView performs one aggregation round against a single discovery
// view: the objects of every view entry are paged through and collected.
// It returns the collected items together with the per-type failures that
// did not invalidate the round. errRestartDiscoveryRound signals that the
// round must be discarded and restarted from a fresh view.
func (c *knDynamicClient) listSourceView(ctx context.Context, view []sourceDiscoveryViewEntry, mode sourceViewMode) ([]unstructured.Unstructured, *PartialListError, error) {
	var (
		items   []unstructured.Unstructured
		missing []TypeListError
	)
	for _, entry := range view {
		typeItems, err := c.listSourceType(ctx, entry.gvr)
		if err == nil {
			items = append(items, typeItems...)
			continue
		}
		switch {
		case isExpiredContinuationError(err):
			// The resourceVersion anchored in a continuation token expired:
			// pages collected so far may miss or duplicate objects, discard
			// the whole round and restart it.
			return nil, nil, errRestartDiscoveryRound
		case isTypeNotServedError(err):
			if mode == sourceViewModeDiscovery {
				// The CRD disappeared between discovery and per-type
				// listing: restart from a refreshed discovery view instead
				// of returning a stale result for this type.
				return nil, nil, errRestartDiscoveryRound
			}
			// Built-in fallback without CRD access: the source type is simply
			// not installed. Record it as a missing type and continue.
			missing = append(missing, TypeListError{Type: entry.kind, GVR: entry.gvr, Err: err})
		case errors.Is(err, context.Canceled):
			return nil, nil, err
		default:
			// Independent error of this single source type (permission,
			// rate limiting, temporary server error, ...): keep the
			// confirmed types and report this type as missing instead of
			// letting the whole command fail.
			missing = append(missing, TypeListError{Type: entry.kind, GVR: entry.gvr, Err: err})
		}
	}
	if len(missing) > 0 {
		return items, &PartialListError{Types: missing}, nil
	}
	return items, nil, nil
}

// listSourceType lists all objects of one source type, following pagination
// continuation tokens. All pages share the resourceVersion anchored in the
// first page, so an expired token is detectable as such by the caller.
func (c *knDynamicClient) listSourceType(ctx context.Context, gvr schema.GroupVersionResource) ([]unstructured.Unstructured, error) {
	var (
		items   []unstructured.Unstructured
		options = metav1.ListOptions{Limit: sourceListChunkSize}
	)
	for {
		page, err := c.client.Resource(gvr).Namespace(c.Namespace()).List(ctx, options)
		if err != nil {
			return nil, err
		}
		items = append(items, page.Items...)
		if page.GetContinue() == "" {
			return items, nil
		}
		options.Continue = page.GetContinue()
	}
}

// newSourceList assembles the final source list of one successful round:
// objects are deduplicated (same object can surface via overlapping GVRs of
// one view) and sorted, and the synthetic SourceList GVK is set when
// non-empty.
func newSourceList(items []unstructured.Unstructured) *unstructured.UnstructuredList {
	sourceList := &unstructured.UnstructuredList{Items: deduplicateAndSortSourceItems(items)}
	if len(sourceList.Items) > 0 {
		sourceList.SetGroupVersionKind(schema.GroupVersionKind{Group: sourceListGroup, Version: sourceListVersion, Kind: sourceListKind})
	}
	return sourceList
}

// deduplicateAndSortSourceItems removes objects already seen in the round and
// returns them in a stable namespace/name/type order.
func deduplicateAndSortSourceItems(items []unstructured.Unstructured) []unstructured.Unstructured {
	seen := make(map[string]struct{}, len(items))
	unique := make([]unstructured.Unstructured, 0, len(items))
	for _, item := range items {
		key := sourceItemIdentity(item)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, item)
	}
	sort.SliceStable(unique, func(i, j int) bool {
		return sourceItemLess(unique[i], unique[j])
	})
	return unique
}

// sourceItemIdentity identifies one source object across the GVRs of one
// discovery round; the server-assigned UID is preferred, the namespaced GVK
// coordinates are used as fallback.
func sourceItemIdentity(u unstructured.Unstructured) string {
	if uid := u.GetUID(); uid != "" {
		return "uid:" + string(uid)
	}
	return fmt.Sprintf("obj:%s/%s/%s/%s", u.GetAPIVersion(), u.GetKind(), u.GetNamespace(), u.GetName())
}

// sourceItemLess defines the stable output order across source types:
// namespace first (for --all-namespaces listings), then name and type.
func sourceItemLess(a, b unstructured.Unstructured) bool {
	if a.GetNamespace() != b.GetNamespace() {
		return a.GetNamespace() < b.GetNamespace()
	}
	if a.GetName() != b.GetName() {
		return a.GetName() < b.GetName()
	}
	if a.GetKind() != b.GetKind() {
		return a.GetKind() < b.GetKind()
	}
	return a.GetAPIVersion() < b.GetAPIVersion()
}

// isExpiredContinuationError reports whether listing failed because the
// resourceVersion bound to a pagination continuation token expired.
func isExpiredContinuationError(err error) bool {
	if apierrors.IsResourceExpired(err) {
		return true
	}
	// Some API servers report expired resource versions as a 410 response with
	// these messages instead of the structured status reason.
	msg := err.Error()
	return strings.Contains(msg, "too old resource version") || strings.Contains(msg, "ResourceVersionDoesNotExist")
}

// isTypeNotServedError reports whether the GVR used for listing is not
// served any more (NoMatch from discovery or a 404 on the resource), which
// happens when the source CRD is uninstalled between discovery and listing.
func isTypeNotServedError(err error) bool {
	return meta.IsNoMatchError(err) || apierrors.IsNotFound(err)
}

// IsTypeNotInstalled tells whether the given per-type listing error means
// that the source type is not installed in the cluster (its GVR is not
// served). Callers of ListSourcesUsingGVKs use it to distinguish not
// installed built-in source types from permission or temporary errors.
func IsTypeNotInstalled(err error) bool {
	return isTypeNotServedError(err)
}

// TypeListError describes a single source type whose objects could not be
// listed in an otherwise successful list round.
type TypeListError struct {
	// Type is the source kind, e.g. PingSource
	Type string
	// GVR is the group-version-resource mapping used while listing the type
	GVR schema.GroupVersionResource
	// Err is the reason why the type could not be listed
	Err error
}

// PartialListError is returned together with a (possibly partial) source
// list when one or more source types of the discovery view could not be
// listed while the remaining types were listed successfully.
type PartialListError struct {
	Types []TypeListError
}

func (e *PartialListError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "could not list %d source type(s):", len(e.Types))
	for _, t := range e.Types {
		fmt.Fprintf(&b, "\n  - %s (%s): %s", t.Type, t.GVR, t.Err.Error())
	}
	return b.String()
}

// AsPartialListError returns the *PartialListError wrapped by err, or nil if
// err does not carry per-type listing failures.
func AsPartialListError(err error) *PartialListError {
	var partial *PartialListError
	if errors.As(err, &partial) && partial != nil {
		return partial
	}
	return nil
}

// ListChannelsUsingGVKs returns list of available channel objects using given list of GVKs
func (c *knDynamicClient) ListChannelsUsingGVKs(ctx context.Context, gvks *[]schema.GroupVersionKind, types ...WithType) (*unstructured.UnstructuredList, error) {
	if gvks == nil {
		return nil, nil
	}

	var (
		channelList unstructured.UnstructuredList
		options     metav1.ListOptions
	)
	namespace := c.Namespace()
	filters := WithTypes(types).List()

	for _, gvk := range *gvks {
		if len(filters) > 0 && !util.SliceContainsIgnoreCase(filters, gvk.Kind) {
			continue
		}

		gvr := gvk.GroupVersion().WithResource(strings.ToLower(gvk.Kind) + "s")

		// list objects of channel type with this GVR
		cList, err := c.client.Resource(gvr).Namespace(namespace).List(ctx, options)
		if err != nil {
			return nil, err
		}

		if len(cList.Items) > 0 {
			channelList.Items = append(channelList.Items, cList.Items...)
		}
	}
	if len(channelList.Items) > 0 {
		channelList.SetGroupVersionKind(schema.GroupVersionKind{Group: messaging.GroupName, Version: channelListVersion, Kind: channelListKind})
	}
	return &channelList, nil
}
