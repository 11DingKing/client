// Copyright 2020 The Knative Authors

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/cli-runtime/pkg/genericclioptions"

	"knative.dev/client/pkg/wait"
	servingv1 "knative.dev/serving/pkg/apis/serving/v1"
)

const (
	ksvcKind = "ksvc"
)

// knServingGitOpsClient - kn service client
// to work on a local repo instead of a remote cluster
type knServingGitOpsClient struct {
	dir        string
	namespace  string
	fileMode   bool
	fileFormat string
	KnServingClient
}

// NewKnServingGitOpsClient returns an instance of the
// kn service gitops client
func NewKnServingGitOpsClient(namespace, dir string) KnServingClient {
	mode, format := getFileModeAndType(dir)
	return &knServingGitOpsClient{
		dir:        dir,
		namespace:  namespace,
		fileMode:   mode,
		fileFormat: format,
	}
}

func (cl *knServingGitOpsClient) getKsvcFilePath(name string) string {
	if cl.fileMode {
		return cl.dir
	}
	return filepath.Join(cl.dir, cl.namespace, ksvcKind, name+".yaml")
}

func getFileModeAndType(dir string) (bool, string) {
	switch {
	case strings.HasSuffix(dir, ".yaml"):
		return true, "yaml"
	case strings.HasSuffix(dir, ".yml"):
		return true, "yaml"
	case strings.HasSuffix(dir, ".json"):
		return true, "json"
	}
	return false, "yaml"
}

// Namespace returns the namespace
func (cl *knServingGitOpsClient) Namespace() string {
	return cl.namespace
}

// GetService returns the knative service for the name
func (cl *knServingGitOpsClient) GetService(ctx context.Context, name string) (*servingv1.Service, error) {
	return readServiceFromFile(cl.getKsvcFilePath(name), name)
}

// ListServices lists the services in the path provided
func (cl *knServingGitOpsClient) ListServices(ctx context.Context, opts ...ListConfig) (*servingv1.ServiceList, error) {
	svcs, err := cl.listServicesFromDirectory()
	if err != nil {
		return nil, err
	}
	typeMeta := metav1.TypeMeta{
		APIVersion: "v1",
		Kind:       "List",
	}
	serviceList := &servingv1.ServiceList{
		TypeMeta: typeMeta,
		Items:    svcs,
	}
	return serviceList, nil
}

func (cl *knServingGitOpsClient) listServicesFromDirectory() ([]servingv1.Service, error) {
	if cl.fileMode {
		svc, err := readServiceFromFile(cl.dir, "")
		if err != nil {
			return nil, err
		}
		return []servingv1.Service{*svc}, nil
	}
	var services []servingv1.Service
	root := cl.dir
	if cl.namespace != "" {
		root = filepath.Join(cl.dir, cl.namespace)
	}
	if _, err := os.Stat(root); err != nil {
		return nil, err
	}
	if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		switch {
		// skip if dir is not ksvc
		case info.IsDir():
			return nil

		// skip non yaml files
		case !strings.HasSuffix(info.Name(), ".yaml"):
			return nil

		// skip non ksvc dir
		case !strings.Contains(path, ksvcKind):
			return filepath.SkipDir

		default:
			svc, err := readServiceFromFile(path, "")
			if err != nil {
				return err
			}
			services = append(services, *svc)
			return nil
		}
	}); err != nil {
		return nil, err
	}
	return services, nil
}

// CreateService saves the knative service spec in
// yaml format in the local path provided
func (cl *knServingGitOpsClient) CreateService(ctx context.Context, service *servingv1.Service) error {
	return cl.writeService(service)
}

// ApplyService applies a service declaration to the local GitOps repository
// using the same three-way merge semantics as the cluster-backed client:
//   - a missing target file is created with the declaration
//   - an existing target file is merged against its last-applied annotation
//
// The result is first rendered and validated in a temporary file and then
// published atomically via a rename in the same directory; a failed attempt
// leaves the previously published (and complete) file untouched. Retries
// always re-read the actual file content and recompute the merge patch, never
// reusing the annotation or the rendered output of a failed round. The
// returned bool reports whether the declaration on disk actually changed.
func (cl *knServingGitOpsClient) ApplyService(ctx context.Context, modifiedService *servingv1.Service) (bool, error) {
	currentService, err := cl.GetService(ctx, modifiedService.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}

	containers := modifiedService.Spec.Template.Spec.Containers
	if len(containers) == 0 || containers[0].Image == "" && currentService != nil {
		return false, errors.New("'service apply' requires the image name to run provided with the --image option")
	}

	// No current file --> create a new declaration
	if currentService == nil {
		if err := updateLastAppliedAnnotation(modifiedService); err != nil {
			return false, err
		}
		return true, cl.writeService(modifiedService)
	}

	// Merge against the actual file content
	var changed bool
	for attempt := 0; attempt < applyMaxAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(applyRetryDelay)

			// Re-read the actual file and recompute the merge from it
			currentService, err = cl.GetService(ctx, modifiedService.Name)
			if err != nil {
				return false, err
			}
		}

		patchBytes, err := createApplyPatch(modifiedService, currentService)
		if err != nil {
			return false, err
		}
		if string(patchBytes) == "{}" {
			// File content already matches the target (no-op, or converged after a retry)
			return changed, nil
		}

		touchesResource, err := patchTouchesResource(patchBytes)
		if err != nil {
			return false, err
		}
		mergedService, err := mergeServiceWithPatch(currentService, patchBytes)
		if err != nil {
			return false, err
		}
		if err := cl.writeService(mergedService); err != nil {
			return false, err
		}
		changed = changed || touchesResource

		// Read the published file back and only declare success when its content
		// matches the target declaration.
		rereadService, err := cl.GetService(ctx, modifiedService.Name)
		if err != nil {
			return false, err
		}
		converged, err := applyConverged(modifiedService, currentService, rereadService)
		if err != nil {
			return false, err
		}
		if converged {
			return changed, nil
		}
		currentService = rereadService
	}
	return false, fmt.Errorf("could not apply service '%s' to '%s': the local file does not match the target declaration after %d attempts",
		modifiedService.Name, cl.getKsvcFilePath(modifiedService.Name), applyMaxAttempts)
}

// writeService renders and atomically publishes the service to its target path.
func (cl *knServingGitOpsClient) writeService(service *servingv1.Service) error {
	updateServingGvk(service)
	fp := cl.getKsvcFilePath(service.ObjectMeta.Name)
	if !cl.fileMode {
		//check if dir exist
		if _, err := os.Stat(cl.dir); os.IsNotExist(err) {
			return fmt.Errorf("directory '%s' not present, please create the directory and try again", cl.dir)
		}
	}
	return writeFile(service, fp, cl.fileFormat)
}

// writeFile renders the object into a temporary file in the target directory,
// validates that it can be parsed back as a service and only then publishes it
// atomically with a rename. If rendering, syncing, validation or renaming fails,
// the temporary file is removed and any previously existing file at fp stays
// untouched, so an interrupted write can never leave a truncated target.
func writeFile(obj runtime.Object, fp, format string) error {
	if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
		return err
	}
	yamlPrinter, err := genericclioptions.NewJSONYamlPrintFlags().ToPrinter(format)
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(fp), "."+filepath.Base(fp)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	published := false
	defer func() {
		if !published {
			_ = os.Remove(tmpName)
		}
	}()

	if err := yamlPrinter.PrintObj(obj, tmp); err != nil {
		_ = tmp.Close()
		return err
	}
	// Flush to disk before the file is validated and published
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	// Validate the staged content can be parsed back as a complete service
	if _, err := readServiceFromFile(tmpName, ""); err != nil {
		return err
	}

	// Atomic publish within the same directory, replacing the previous file
	if err := os.Rename(tmpName, fp); err != nil {
		return err
	}
	published = true
	return nil
}

// UpdateService updates the service in
// the local directory
func (cl *knServingGitOpsClient) UpdateService(ctx context.Context, service *servingv1.Service) (bool, error) {
	// check if file exist
	if _, err := cl.GetService(ctx, service.ObjectMeta.Name); err != nil {
		return false, err
	}
	// replace file
	return true, cl.CreateService(ctx, service)
}

// UpdateServiceWithRetry updates the service in the local directory
func (cl *knServingGitOpsClient) UpdateServiceWithRetry(ctx context.Context, name string, updateFunc ServiceUpdateFunc, nrRetries int) (bool, error) {
	return updateServiceWithRetry(ctx, cl, name, updateFunc, nrRetries)
}

// DeleteService removes the file from the local file system
func (cl *knServingGitOpsClient) DeleteService(ctx context.Context, serviceName string, timeout time.Duration) error {
	return os.Remove(cl.getKsvcFilePath(serviceName))
}

// WaitForService always returns success for this client
func (cl *knServingGitOpsClient) WaitForService(ctx context.Context, name string, wconfig WaitConfig, msgCallback wait.MessageCallback) (error, time.Duration) {
	return nil, 1 * time.Second
}

func readServiceFromFile(fileKey, name string) (*servingv1.Service, error) {
	var svc servingv1.Service
	file, err := os.Open(fileKey)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, apierrors.NewNotFound(servingv1.Resource("services"), name)
		}
		return nil, err
	}
	defer file.Close()
	decoder := yaml.NewYAMLOrJSONDecoder(file, 512)
	if err := decoder.Decode(&svc); err != nil {
		return nil, err
	}
	return &svc, nil
}
