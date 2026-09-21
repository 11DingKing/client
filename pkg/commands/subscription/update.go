/*
Copyright 2020 The Knative Authors

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

package subscription

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"knative.dev/client/pkg/config"
	messagingv1 "knative.dev/eventing/pkg/apis/messaging/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"github.com/spf13/cobra"

	"knative.dev/client/pkg/commands"
	"knative.dev/client/pkg/commands/flags"
	knerrors "knative.dev/client/pkg/errors"
	knmessagingv1 "knative.dev/client/pkg/messaging/v1"
)

// sinkFields holds the three possible destinations of a Subscription that
// can be changed with 'kn subscription update'.
type sinkFields struct {
	subscriber     *duckv1.Destination
	reply          *duckv1.Destination
	deadLetterSink *duckv1.Destination
}

// NewSubscriptionUpdateCommand to update event subscriptions
func NewSubscriptionUpdateCommand(p *commands.KnParams) *cobra.Command {
	var subscriberFlag, replyFlag, dlsFlag flags.SinkFlags
	cmd := &cobra.Command{
		Use:   "update NAME",
		Short: "Update an event subscription",
		Example: `
  # Update a subscription 'sub0' with a subscriber ksvc 'receiver'
  kn subscription update sub0 --sink ksvc:receiver

  # Update a subscription 'sub1' with subscriber ksvc 'mirror', reply to a broker 'nest' and DeadLetterSink to a ksvc 'bucket'
  kn subscription update sub1 --sink mirror --sink-reply broker:nest --sink-dead-letter bucket`,
		ValidArgsFunction: commands.ResourceNameCompletionFunc(p),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if len(args) != 1 {
				return errors.New("'kn subscription update' requires the subscription name given as single argument")
			}
			name := args[0]

			namespace, err := p.GetNamespace(cmd)
			if err != nil {
				return err
			}

			dynamicClient, err := p.NewDynamicClient(namespace)
			if err != nil {
				return err
			}

			client, err := newSubscriptionClient(p, cmd)
			if err != nil {
				return err
			}

			// Only the sinks explicitly given on the command line are
			// re-resolved and changed. Unspecified sinks keep whatever the
			// server returns as the latest state on every retry.
			subscriberChanged := cmd.Flags().Changed("sink")
			replyChanged := cmd.Flags().Changed("sink-reply")
			dlsChanged := cmd.Flags().Changed("sink-dead-letter")

			// submittedSinks records the three destinations of the object
			// used in the last update attempt. Every retry overwrites it, so
			// it never mixes destinations resolved in different retry rounds.
			var submittedSinks sinkFields

			updateFunc := func(origSub *messagingv1.Subscription) (*messagingv1.Subscription, error) {
				sinks := sinkFields{
					subscriber:     origSub.Spec.Subscriber,
					reply:          origSub.Spec.Reply,
					deadLetterSink: deadLetterSinkOf(origSub),
				}

				// Resolve every explicitly requested sink against the
				// current cluster state before the builder is touched. A
				// resolution error aborts this attempt without submitting a
				// partially patched set of destinations.
				if subscriberChanged {
					sub, err := subscriberFlag.ResolveSink(cmd.Context(), dynamicClient, namespace)
					if err != nil {
						return nil, err
					}
					sinks.subscriber = sub
				}
				if replyChanged {
					rep, err := replyFlag.ResolveSink(cmd.Context(), dynamicClient, namespace)
					if err != nil {
						return nil, err
					}
					sinks.reply = rep
				}
				if dlsChanged {
					ds, err := dlsFlag.ResolveSink(cmd.Context(), dynamicClient, namespace)
					if err != nil {
						return nil, err
					}
					sinks.deadLetterSink = ds
				}

				// Start from the latest Subscription (including its
				// resourceVersion) and submit all three destinations
				// together, so unspecified fields retain the server value
				// read in this retry round.
				sb := knmessagingv1.NewSubscriptionBuilderFromExisting(origSub)
				sb.Subscriber(sinks.subscriber)
				sb.Reply(sinks.reply)
				sb.DeadLetterSink(sinks.deadLetterSink)
				updatedSub := sb.Build()

				submittedSinks = sinkFields{
					subscriber:     updatedSub.Spec.Subscriber,
					reply:          updatedSub.Spec.Reply,
					deadLetterSink: deadLetterSinkOf(updatedSub),
				}
				return updatedSub, nil
			}
			err = client.UpdateSubscriptionWithRetry(cmd.Context(), name, updateFunc, config.DefaultRetry.Steps)
			if err != nil {
				return knerrors.GetError(err)
			}

			// Read the object back before reporting success and make sure
			// the server actually stored the three destinations of the
			// last update attempt. Otherwise the user must not be told the
			// update succeeded while another version is persisted.
			persistedSub, err := client.GetSubscription(cmd.Context(), name)
			if err != nil {
				return knerrors.GetError(err)
			}
			if mismatched := mismatchedSinks(submittedSinks, sinkFieldsOf(persistedSub)); len(mismatched) != 0 {
				return fmt.Errorf(
					"subscription '%s' in namespace '%s' cannot be confirmed as updated: the server-side values for %s do not match the requested sinks, please re-run the update",
					name, namespace, strings.Join(mismatched, ", "))
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Subscription '%s' updated in namespace '%s'.\n", name, namespace)
			return nil
		},
	}
	commands.AddNamespaceFlags(cmd.Flags(), false)
	// add subscriber flag as `--sink`
	subscriberFlag.Add(cmd)
	replyFlag.AddWithFlagName(cmd, "sink-reply", "")
	dlsFlag.AddWithFlagName(cmd, "sink-dead-letter", "")
	return cmd
}

// deadLetterSinkOf returns the dead letter sink of a Subscription or nil if
// no delivery options are configured.
func deadLetterSinkOf(sub *messagingv1.Subscription) *duckv1.Destination {
	if sub.Spec.Delivery != nil {
		return sub.Spec.Delivery.DeadLetterSink
	}
	return nil
}

// sinkFieldsOf extracts the three destinations stored in a Subscription.
func sinkFieldsOf(sub *messagingv1.Subscription) sinkFields {
	return sinkFields{
		subscriber:     sub.Spec.Subscriber,
		reply:          sub.Spec.Reply,
		deadLetterSink: deadLetterSinkOf(sub),
	}
}

// mismatchedSinks returns the human-readable names of the destinations which
// differ between the requested and the server-persisted sink configuration.
func mismatchedSinks(want, got sinkFields) []string {
	mismatched := make([]string, 0, 3)
	if !reflect.DeepEqual(want.subscriber, got.subscriber) {
		mismatched = append(mismatched, "subscriber")
	}
	if !reflect.DeepEqual(want.reply, got.reply) {
		mismatched = append(mismatched, "reply")
	}
	if !reflect.DeepEqual(want.deadLetterSink, got.deadLetterSink) {
		mismatched = append(mismatched, "dead-letter sink")
	}
	return mismatched
}
