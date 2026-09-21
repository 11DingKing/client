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
	"fmt"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	dynamicfake "knative.dev/client/pkg/dynamic/fake"
	v1beta1 "knative.dev/client/pkg/messaging/v1"
	"knative.dev/client/pkg/util"
)

func TestUpdateSubscriptionErrorCase(t *testing.T) {
	cClient := v1beta1.NewMockKnSubscriptionsClient(t)
	dynamicClient := dynamicfake.CreateFakeKnDynamicClient("default")

	cRecorder := cClient.Recorder()
	_, err := executeSubscriptionCommand(cClient, dynamicClient, "update")
	assert.Error(t, err, "'kn subscription update' requires the subscription name given as single argument")
	cRecorder.Validate()
}

func TestUpdateSubscriptionErrorCaseUnknownChannelFlag(t *testing.T) {
	cClient := v1beta1.NewMockKnSubscriptionsClient(t)
	dynamicClient := dynamicfake.CreateFakeKnDynamicClient("default")

	cRecorder := cClient.Recorder()
	_, err := executeSubscriptionCommand(cClient, dynamicClient, "update", "sub0", "--channel", "imc:i1")
	assert.Error(t, err, "unknown flag: --channel")
	cRecorder.Validate()
}

func TestUpdateSubscription(t *testing.T) {
	cClient := v1beta1.NewMockKnSubscriptionsClient(t)
	sub0 := createSubscription("sub0", "imc0", "ksvc0", "", "")
	dynamicClient := dynamicfake.CreateFakeKnDynamicClient("default",
		sub0,
		createService("ksvc1"),
		createBroker("b0"),
		createBroker("b1"))

	updated := createSubscription("sub0",
		"imc0",
		"ksvc1",
		"b0",
		"b1")

	cRecorder := cClient.Recorder()
	cRecorder.GetSubscription("sub0", sub0, nil)
	cRecorder.UpdateSubscription(updated, nil)
	// The updated object is read back before success is reported.
	cRecorder.GetSubscription("sub0", updated, nil)

	out, err := executeSubscriptionCommand(cClient, dynamicClient, "update", "sub0",
		"--sink", "ksvc1",
		"--sink-reply", "broker:b0",
		"--sink-dead-letter", "broker:b1")
	assert.NilError(t, err, "subscription should be updated")
	assert.Assert(t, util.ContainsAll(out, "updated", "sub0", "default"))
	cRecorder.Validate()
}

// Only the explicitly given sink is resolved and changed; subscriber, reply
// and dead-letter sink of the latest server object are submitted together.
func TestUpdateSubscriptionSingleSinkKeepsOthers(t *testing.T) {
	cClient := v1beta1.NewMockKnSubscriptionsClient(t)
	sub0 := createSubscription("sub0", "imc0", "ksvc0", "b0", "b1")
	// The reply and dead-letter brokers are intentionally absent from the
	// dynamic client: they must not be resolved again because neither flag
	// was given.
	dynamicClient := dynamicfake.CreateFakeKnDynamicClient("default",
		sub0,
		createService("ksvc1"))

	updated := createSubscription("sub0", "imc0", "ksvc1", "b0", "b1")

	cRecorder := cClient.Recorder()
	cRecorder.GetSubscription("sub0", sub0, nil)
	cRecorder.UpdateSubscription(updated, nil)
	cRecorder.GetSubscription("sub0", updated, nil)

	out, err := executeSubscriptionCommand(cClient, dynamicClient, "update", "sub0",
		"--sink", "ksvc1")
	assert.NilError(t, err, "subscription should be updated")
	assert.Assert(t, util.ContainsAll(out, "updated", "sub0", "default"))
	cRecorder.Validate()
}

// URI sinks do not require a lookup and are persisted and confirmed as-is.
func TestUpdateSubscriptionWithURISink(t *testing.T) {
	cClient := v1beta1.NewMockKnSubscriptionsClient(t)
	sub0 := createSubscription("sub0", "imc0", "ksvc0", "", "")
	dynamicClient := dynamicfake.CreateFakeKnDynamicClient("default", sub0)

	uri, err := apis.ParseURL("http://event-receiver.example.com")
	assert.NilError(t, err)
	updated := createSubscription("sub0", "imc0", "", "", "")
	updated.Spec.Subscriber = &duckv1.Destination{URI: uri}

	cRecorder := cClient.Recorder()
	cRecorder.GetSubscription("sub0", sub0, nil)
	cRecorder.UpdateSubscription(updated, nil)
	cRecorder.GetSubscription("sub0", updated, nil)

	out, err := executeSubscriptionCommand(cClient, dynamicClient, "update", "sub0",
		"--sink", "http://event-receiver.example.com")
	assert.NilError(t, err, "subscription should be updated")
	assert.Assert(t, util.ContainsAll(out, "updated", "sub0", "default"))
	cRecorder.Validate()
}

// On a conflict the update is retried starting from the latest Subscription.
// The changed sink is re-resolved and all three destinations are submitted
// together with the new resourceVersion; server-side changes made between the
// attempts are preserved.
func TestUpdateSubscriptionConflictRetriesWithLatestState(t *testing.T) {
	cClient := v1beta1.NewMockKnSubscriptionsClient(t)

	// State as of the first read.
	subV1 := createSubscription("sub0", "imc0", "ksvc0", "", "")
	subV1.ResourceVersion = "100"

	// Latest state before the second attempt: a concurrent change added a
	// reply sink and bumped the resourceVersion.
	subV2 := createSubscription("sub0", "imc0", "ksvc0", "b0", "")
	subV2.ResourceVersion = "200"

	// Every attempt must use the resourceVersion and unchanged sinks of the
	// object it started from.
	attempt1 := createSubscription("sub0", "imc0", "ksvc1", "", "")
	attempt1.ResourceVersion = "100"
	attempt2 := createSubscription("sub0", "imc0", "ksvc1", "b0", "")
	attempt2.ResourceVersion = "200"

	persisted := createSubscription("sub0", "imc0", "ksvc1", "b0", "")
	persisted.ResourceVersion = "300"

	dynamicClient := dynamicfake.CreateFakeKnDynamicClient("default",
		subV1,
		createService("ksvc1"))

	conflictErr := apierrors.NewConflict(
		schema.GroupResource{Group: "messaging.knative.dev", Resource: "subscriptions"},
		"sub0",
		fmt.Errorf("the object has been modified; please apply your changes to the latest version and try again"))

	cRecorder := cClient.Recorder()
	cRecorder.GetSubscription("sub0", subV1, nil)
	cRecorder.UpdateSubscription(attempt1, conflictErr)
	cRecorder.GetSubscription("sub0", subV2, nil)
	cRecorder.UpdateSubscription(attempt2, nil)
	cRecorder.GetSubscription("sub0", persisted, nil)

	out, err := executeSubscriptionCommand(cClient, dynamicClient, "update", "sub0",
		"--sink", "ksvc1")
	assert.NilError(t, err, "subscription should be updated after the conflict retries")
	assert.Assert(t, util.ContainsAll(out, "updated", "sub0", "default"))
	cRecorder.Validate()
}

// A resolution error aborts the attempt: nothing is submitted and no success
// is reported.
func TestUpdateSubscriptionResolveErrorDoesNotSubmit(t *testing.T) {
	cClient := v1beta1.NewMockKnSubscriptionsClient(t)
	sub0 := createSubscription("sub0", "imc0", "ksvc0", "", "")
	dynamicClient := dynamicfake.CreateFakeKnDynamicClient("default", sub0)

	cRecorder := cClient.Recorder()
	cRecorder.GetSubscription("sub0", sub0, nil)

	out, err := executeSubscriptionCommand(cClient, dynamicClient, "update", "sub0",
		"--sink", "ksvc:ghost")
	assert.ErrorContains(t, err, "not found")
	assert.ErrorContains(t, err, "ghost")
	// Cobra may render the error and usage, but no success line must be printed.
	assert.Assert(t, !strings.Contains(out, "updated in namespace"))
	cRecorder.Validate()
}

// Once the conflict budget is exhausted the error is surfaced without
// reporting success and without an additional confirmation read.
func TestUpdateSubscriptionConflictExhausted(t *testing.T) {
	cClient := v1beta1.NewMockKnSubscriptionsClient(t)
	sub0 := createSubscription("sub0", "imc0", "ksvc0", "", "")
	dynamicClient := dynamicfake.CreateFakeKnDynamicClient("default",
		sub0,
		createService("ksvc1"))

	attempt := createSubscription("sub0", "imc0", "ksvc1", "", "")
	conflictErr := apierrors.NewConflict(
		schema.GroupResource{Group: "messaging.knative.dev", Resource: "subscriptions"},
		"sub0",
		fmt.Errorf("the object has been modified"))

	cRecorder := cClient.Recorder()
	// Default retry budget is five attempts, each starting with a fresh read.
	for i := 0; i < 5; i++ {
		cRecorder.GetSubscription("sub0", sub0, nil)
		cRecorder.UpdateSubscription(attempt, conflictErr)
	}

	out, err := executeSubscriptionCommand(cClient, dynamicClient, "update", "sub0",
		"--sink", "ksvc1")
	assert.Assert(t, apierrors.IsConflict(err), "expected conflict error, got: %v", err)
	assert.Assert(t, !strings.Contains(out, "updated in namespace"))
	cRecorder.Validate()
}

// If the read-back shows the server persisted different destinations than
// requested, no success message is printed.
func TestUpdateSubscriptionReadBackMismatch(t *testing.T) {
	cClient := v1beta1.NewMockKnSubscriptionsClient(t)
	sub0 := createSubscription("sub0", "imc0", "ksvc0", "", "")
	dynamicClient := dynamicfake.CreateFakeKnDynamicClient("default",
		sub0,
		createService("ksvc1"))

	updated := createSubscription("sub0", "imc0", "ksvc1", "", "")

	cRecorder := cClient.Recorder()
	cRecorder.GetSubscription("sub0", sub0, nil)
	cRecorder.UpdateSubscription(updated, nil)
	// The server still reports the old subscriber.
	cRecorder.GetSubscription("sub0", sub0, nil)

	out, err := executeSubscriptionCommand(cClient, dynamicClient, "update", "sub0",
		"--sink", "ksvc1")
	assert.ErrorContains(t, err, "cannot be confirmed as updated")
	assert.ErrorContains(t, err, "subscriber")
	assert.Assert(t, !strings.Contains(out, "updated in namespace"))
	cRecorder.Validate()
}
