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

package source

import (
	"fmt"

	"knative.dev/client/pkg/sources"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/client/pkg/commands"
	"knative.dev/client/pkg/commands/flags"
	"knative.dev/client/pkg/commands/source/duck"
	"knative.dev/client/pkg/dynamic"
	knerrors "knative.dev/client/pkg/errors"
)

const (
	sourceListGroup   = "client.knative.dev"
	sourceListVersion = "v1alpha1"
	sourceListKind    = "SourceList"
)

var listExample = `
  # List available eventing sources
  kn source list

  # List PingSource type sources
  kn source list --type=PingSource

  # List PingSource and ApiServerSource types sources
  kn source list --type=PingSource --type=apiserversource`

// NewListCommand defines and processes `kn source list`
func NewListCommand(p *commands.KnParams) *cobra.Command {
	filterFlags := &flags.SourceTypeFilters{}
	listFlags := flags.NewListPrintFlags(ListHandlers)
	listCommand := &cobra.Command{
		Use:     "list",
		Short:   "List event sources",
		Aliases: []string{"ls"},
		Example: listExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			namespace, err := p.GetNamespace(cmd)
			if err != nil {
				return err
			}
			dynamicClient, err := p.NewDynamicClient(namespace)
			if err != nil {
				return err
			}

			var filters dynamic.WithTypes
			for _, filter := range filterFlags.Filters {
				filters = append(filters, dynamic.WithTypeFilter(filter))
			}

			sourceList, err := dynamicClient.ListSources(cmd.Context(), filters...)

			switch {
			case knerrors.IsForbiddenError(err):
				// CRD access is forbidden: fall back to the built-in source
				// GVKs, as before. A not-served GVR on this path means the
				// corresponding built-in source type is not installed and is
				// not reported as an omission; independent per-type errors of
				// installed types stay non-success.
				gvks := sources.BuiltInSourcesGVKs()
				sourceList, err = dynamicClient.ListSourcesUsingGVKs(cmd.Context(), &gvks, filters...)
				if err != nil && dynamic.AsPartialListError(err) == nil {
					return knerrors.GetError(err)
				}
				if partial := dynamic.AsPartialListError(err); partial != nil {
					err = partialWithoutNotInstalled(partial)
				}
			case err != nil && dynamic.AsPartialListError(err) == nil:
				return knerrors.GetError(err)
			}

			if sourceList == nil {
				sourceList = &unstructured.UnstructuredList{}
			}
			// Independent per-type failures keep the confirmed source types,
			// but are still a non-success result. With no confirmed sources
			// left, report the omissions instead of "No sources found.".
			partialErr := dynamic.AsPartialListError(err)
			if len(sourceList.Items) == 0 {
				if partialErr != nil {
					return knerrors.GetError(err)
				}
				if !listFlags.GenericPrintFlags.OutputFlagSpecified() {
					fmt.Fprintf(cmd.OutOrStdout(), "No sources found.\n")
					return nil
				}
			}

			if sourceList.GroupVersionKind().Empty() {
				sourceList.SetGroupVersionKind(schema.GroupVersionKind{Group: sourceListGroup, Version: sourceListVersion, Kind: sourceListKind})
			}
			// empty namespace indicates all namespaces flag is specified
			if namespace == "" {
				listFlags.EnsureWithNamespace()
			}
			printer, err := listFlags.ToPrinter()
			if err != nil {
				return nil
			}
			if listFlags.GenericPrintFlags.OutputFlagSpecified() {
				if printErr := printer.PrintObj(sourceList, cmd.OutOrStdout()); printErr != nil {
					return printErr
				}
				// Surface the omitted source types as a non-success result
				if partialErr != nil {
					return knerrors.GetError(partialErr)
				}
				return nil
			}
			// Convert the source list to DuckSourceList only if human readable table printing requested
			sourceDuckList := duck.ToSourceList(sourceList)
			if printErr := printer.PrintObj(sourceDuckList, cmd.OutOrStdout()); printErr != nil {
				return printErr
			}
			// Surface the omitted source types as a non-success result
			if partialErr != nil {
				return knerrors.GetError(partialErr)
			}
			return nil
		},
	}
	commands.AddNamespaceFlags(listCommand.Flags(), true)
	listFlags.AddFlags(listCommand)
	filterFlags.Add(listCommand, "source type")
	return listCommand
}

// partialWithoutNotInstalled drops omissions caused by a built-in source type
// simply not being installed (its GVR is not served), which is expected when
// CRDs cannot be read and the built-in GVK fallback is used. Real omissions
// (permission or temporary errors) are kept and returned as non-success.
func partialWithoutNotInstalled(partial *dynamic.PartialListError) error {
	if partial == nil {
		return nil
	}
	kept := make([]dynamic.TypeListError, 0, len(partial.Types))
	for _, t := range partial.Types {
		if !dynamic.IsTypeNotInstalled(t.Err) {
			kept = append(kept, t)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return &dynamic.PartialListError{Types: kept}
}
