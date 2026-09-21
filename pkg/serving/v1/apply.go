package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/jsonmergepatch"
	servingv1 "knative.dev/serving/pkg/apis/serving/v1"

	"knative.dev/client/pkg/util"
)

const (
	// applyMaxAttempts is the maximum number of merge/patch attempts, counting
	// conflict retries and server-side read-back divergences.
	applyMaxAttempts = 5
	// applyRetryDelay is the back-off between failed attempts.
	applyRetryDelay = 1 * time.Second
)

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

// Helper methods supporting Apply()

// patch performs a 3-way merge and returns whether the original service has been changed.
// This method uses a simple JSON 3-way merge which has some severe limitations, like that arrays
// can't be merged. Ideally a strategicpatch merge should be used, which allows a more fine grained
// way for performing the merge (but this is not supported for custom resources)
// See issue https://github.com/knative/client/issues/1073 for more details how this method should be
// improved for a better merge strategy.
//
// The merge is always calculated on the latest server-side object (latest resourceVersion).
// On a conflict the object is re-read and the original configuration as well as the merge
// patch are recomputed from scratch, nothing from the failed attempt is reused.
// A successful PATCH alone is not reported as complete: the object is read back from the
// server and it must match the target declaration before this method returns. When the
// read-back diverges, the actual server-side object is used for the next attempt.
func (cl *knServingClient) patch(ctx context.Context, modifiedService *servingv1.Service, currentService *servingv1.Service) (bool, error) {
	var changed bool
	var lastErr error
	for attempt := 0; attempt < applyMaxAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(applyRetryDelay)

			// Restart from the actual server-side object (which carries the new
			// resourceVersion) instead of reusing any stale state.
			var err error
			currentService, err = cl.GetService(ctx, modifiedService.Name)
			if err != nil {
				return false, err
			}
		}

		// Re-extract the original configuration and recompute the patch on every
		// attempt from the actual current object.
		patchBytes, err := createApplyPatch(modifiedService, currentService)
		if err != nil {
			return false, err
		}

		if string(patchBytes) == "{}" {
			// Actual state already matches the target: either a no-op on the first
			// attempt or a successful convergence after a retry.
			return changed, nil
		}

		// Check if the generation has been counted up, only then the backend detected a change
		savedService, err := cl.patchService(ctx, currentService.Name, types.MergePatchType, patchBytes)
		if apierrors.IsConflict(err) {
			lastErr = err
			continue
		}
		if err != nil {
			return false, err
		}
		changed = savedService.Generation != savedService.Status.ObservedGeneration

		// Read back the object and only declare success when the server-side
		// object matches the target declaration.
		rereadService, err := cl.GetService(ctx, modifiedService.Name)
		if err != nil {
			lastErr = err
			continue
		}
		converged, err := applyConverged(modifiedService, currentService, rereadService)
		if err != nil {
			return false, err
		}
		if converged {
			return changed, nil
		}

		// The server-side object does not (yet) match the target, retry against
		// the object as it actually is now.
		currentService = rereadService
		lastErr = fmt.Errorf("service '%s' has been patched but the server-side object does not match the target declaration", modifiedService.Name)
	}

	if lastErr != nil {
		return false, lastErr
	}
	return false, fmt.Errorf("could not apply service '%s': exhausted %d attempts", modifiedService.Name, applyMaxAttempts)
}

// createApplyPatch builds the three-way JSON merge patch that transforms the
// actual current service into the declared target state. The "original"
// configuration is taken from the last-applied annotation of the given current
// object, so it must be called with a freshly read object on every attempt.
func createApplyPatch(modifiedService, currentService *servingv1.Service) ([]byte, error) {
	uModifiedService, err := getModifiedConfiguration(modifiedService, true)
	if err != nil {
		return nil, err
	}
	uCurrentService, err := encodeService(currentService)
	if err != nil {
		return nil, err
	}
	return jsonmergepatch.CreateThreeWayJSONMergePatch(getOriginalConfiguration(currentService), uModifiedService, uCurrentService)
}

// applyConverged reports whether the actual (read-back) service matches the
// target declaration. The original configuration used for comparison is taken
// from the object as it was before the patch, so that every field touched by
// the patch is verified against the server-side result.
func applyConverged(modifiedService, prePatchService, actualService *servingv1.Service) (bool, error) {
	uModifiedService, err := getModifiedConfiguration(modifiedService, true)
	if err != nil {
		return false, err
	}
	uActualService, err := encodeService(actualService)
	if err != nil {
		return false, err
	}
	// Use the original configuration of the object as it was before the patch
	// together with the target declaration and the read-back object. The resulting
	// patch is empty exactly when every change the patch requested is present in
	// the read-back object and every removed field is gone.
	verifyPatch, err := jsonmergepatch.CreateThreeWayJSONMergePatch(getOriginalConfiguration(prePatchService), uModifiedService, uActualService)
	if err != nil {
		return false, err
	}
	return string(verifyPatch) == "{}", nil
}

// mergeServiceWithPatch applies the given RFC 7386 JSON merge patch to the
// serialized current service and returns the resulting service object.
// It is used by the GitOps client to materialize a three-way merge on disk.
func mergeServiceWithPatch(currentService *servingv1.Service, patchBytes []byte) (*servingv1.Service, error) {
	uCurrentService, err := encodeService(currentService)
	if err != nil {
		return nil, err
	}
	mergedBytes, err := jsonMergePatch(uCurrentService, patchBytes)
	if err != nil {
		return nil, err
	}
	mergedService := &servingv1.Service{}
	if err := json.Unmarshal(mergedBytes, mergedService); err != nil {
		return nil, err
	}
	if err := updateServingGvk(mergedService); err != nil {
		return nil, err
	}
	return mergedService, nil
}

// jsonMergePatch applies an RFC 7386 (https://datatracker.ietf.org/doc/html/rfc7386)
// JSON merge patch to the given JSON document. A null value in the patch removes
// the corresponding member, nested objects are merged recursively and any other
// value replaces the current content (arrays are replaced wholesale).
func jsonMergePatch(original, patch []byte) ([]byte, error) {
	var originalDoc, patchDoc interface{}
	if len(original) > 0 {
		if err := json.Unmarshal(original, &originalDoc); err != nil {
			return nil, err
		}
	}
	if err := json.Unmarshal(patch, &patchDoc); err != nil {
		return nil, err
	}
	merged := mergeJSONValue(originalDoc, patchDoc)
	if merged == nil {
		merged = map[string]interface{}{}
	}
	return json.Marshal(merged)
}

func mergeJSONValue(original, patch interface{}) interface{} {
	patchMap, patchIsObject := patch.(map[string]interface{})
	if !patchIsObject {
		// Arrays, scalars and null replace the original value; null deletions
		// are handled by the parent object.
		return patch
	}
	originalMap, _ := original.(map[string]interface{})
	if originalMap == nil {
		originalMap = map[string]interface{}{}
	}
	for key, value := range patchMap {
		if value == nil {
			delete(originalMap, key)
		} else {
			originalMap[key] = mergeJSONValue(originalMap[key], value)
		}
	}
	return originalMap
}

// patchTouchesResource reports whether the given merge patch changes anything
// besides the internal last-applied-configuration annotation. It is used by the
// GitOps path (which has no server-side generation) to distinguish a real
// declaration change from an annotation-only update.
func patchTouchesResource(patchBytes []byte) (bool, error) {
	var patchMap map[string]interface{}
	if err := json.Unmarshal(patchBytes, &patchMap); err != nil {
		return false, err
	}
	if metadata, ok := patchMap["metadata"].(map[string]interface{}); ok {
		if annotations, ok := metadata["annotations"].(map[string]interface{}); ok {
			delete(annotations, v1.LastAppliedConfigAnnotation)
			if len(annotations) == 0 {
				delete(metadata, "annotations")
			}
		}
		if len(metadata) == 0 {
			delete(patchMap, "metadata")
		}
	}
	return len(patchMap) > 0, nil
}

// patchService patches the given service
func (cl *knServingClient) patchService(ctx context.Context, name string, patchType types.PatchType, patch []byte) (*servingv1.Service, error) {
	service, err := cl.client.Services(cl.namespace).Patch(ctx, name, patchType, patch, metav1.PatchOptions{})
	if err != nil {
		return nil, err
	}
	err = updateServingGvk(service)

	return service, err
}

func getOriginalConfiguration(service *servingv1.Service) []byte {
	annots := service.Annotations
	if annots == nil {
		return nil
	}
	original, ok := annots[v1.LastAppliedConfigAnnotation]
	if !ok {
		return nil
	}
	return []byte(original)
}

func getModifiedConfiguration(service *servingv1.Service, annotate bool) ([]byte, error) {

	// First serialize the object without the annotation to prevent recursion,
	// then add that serialization to it as the annotation and serialize it again.
	var uModifiedService []byte

	// Otherwise, use the server side version of the object.
	// Get the current annotations from the object.
	annots := service.Annotations
	if annots == nil {
		annots = map[string]string{}
	}

	original := annots[v1.LastAppliedConfigAnnotation]
	delete(annots, v1.LastAppliedConfigAnnotation)
	service.Annotations = annots

	uModifiedService, err := encodeService(service)
	if err != nil {
		return nil, err
	}

	if annotate {
		annots[v1.LastAppliedConfigAnnotation] = strings.TrimRight(string(uModifiedService), "\n")

		service.Annotations = annots
		uModifiedService, err = encodeService(service)
		if err != nil {
			return nil, err
		}
	}

	// Restore the object to its original condition.
	annots[v1.LastAppliedConfigAnnotation] = original
	service.Annotations = annots
	return uModifiedService, nil
}

func updateLastAppliedAnnotation(service *servingv1.Service) error {
	annots := service.Annotations
	if annots == nil {
		annots = map[string]string{}
	}
	lastApplied, err := encodeService(service)
	if err != nil {
		return err
	}

	// Cleanup any trailing newlines
	annots[v1.LastAppliedConfigAnnotation] = strings.TrimRight(string(lastApplied), "\n")

	service.Annotations = annots
	return nil
}

func encodeService(service *servingv1.Service) ([]byte, error) {
	scheme := runtime.NewScheme()
	err := servingv1.AddToScheme(scheme)
	if err != nil {
		return nil, err
	}
	factory := serializer.NewCodecFactory(scheme)
	encoder := factory.EncoderForVersion(unstructured.UnstructuredJSONScheme, servingv1.SchemeGroupVersion)
	err = util.UpdateGroupVersionKindWithScheme(service, servingv1.SchemeGroupVersion, scheme)
	if err != nil {
		return nil, err
	}

	serviceUnstructured, err := util.ToUnstructured(service)
	if err != nil {
		return nil, err
	}

	// Remove/adapt service so that it can be used in the apply-annotation
	cleanupServiceUnstructured(serviceUnstructured)

	return runtime.Encode(encoder, serviceUnstructured)
}

func cleanupServiceUnstructured(uService *unstructured.Unstructured) {
	clearCreationTimestamps(uService.Object)
	removeStatus(uService.Object)
	removeContainerNameAndResourcesIfNotSet(uService.Object)

}

func removeContainerNameAndResourcesIfNotSet(uService map[string]interface{}) {
	uContainer := extractUserContainer(uService)
	if uContainer == nil {
		return
	}
	// The container name is not part of the user declaration (it is defaulted
	// by the server, or comes back as an empty string when a merged document is
	// decoded), so it must never destabilize the three-way merge.
	delete(uContainer, "name")

	resources := uContainer["resources"]
	if resources == nil {
		return
	}
	resourcesMap := resources.(map[string]interface{})
	if len(resourcesMap) == 0 {
		delete(uContainer, "resources")
	}
}

func extractUserContainer(uService map[string]interface{}) map[string]interface{} {
	tSpec := extractTemplateSpec(uService)
	if tSpec == nil {
		return nil
	}
	containers := tSpec["containers"]
	if len(containers.([]interface{})) == 0 {
		return nil
	}
	return containers.([]interface{})[0].(map[string]interface{})
}

func removeStatus(uService map[string]interface{}) {
	delete(uService, "status")
}

func clearCreationTimestamps(uService map[string]interface{}) {
	meta := uService["metadata"]
	if meta != nil {
		delete(meta.(map[string]interface{}), "creationTimestamp")
	}
	template := extractTemplate(uService)
	if template != nil {
		meta = template["metadata"]
		if meta != nil {
			delete(meta.(map[string]interface{}), "creationTimestamp")
		}
	}
}

func extractTemplateSpec(uService map[string]interface{}) map[string]interface{} {
	templ := extractTemplate(uService)
	if templ == nil {
		return nil
	}
	templSpec := templ["spec"]
	if templSpec == nil {
		return nil
	}

	return templSpec.(map[string]interface{})
}

func extractTemplate(uService map[string]interface{}) map[string]interface{} {
	spec := uService["spec"]
	if spec == nil {
		return nil
	}
	templ := spec.(map[string]interface{})["template"]
	if templ == nil {
		return nil
	}
	return templ.(map[string]interface{})
}
