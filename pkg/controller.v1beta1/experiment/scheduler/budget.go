/*
Copyright 2026 The Kubeflow Authors.

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

package scheduler

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	configv1beta1 "github.com/kubeflow/katib/pkg/apis/config/v1beta1"
	"github.com/kubeflow/katib/pkg/controller.v1beta1/consts"
)

// LookupNamespaceBudget reads the namespace concurrency budget from the Katib
// config ConfigMap. A missing ConfigMap, missing entry or non-positive limit
// surfaces as an error or zero so the BudgetProvider applies the conservative
// fallback; the lookup never signals unlimited concurrency.
func LookupNamespaceBudget(c client.Client, namespace string) (int32, error) {
	if c == nil {
		return 0, fmt.Errorf("kubernetes client is unavailable")
	}
	configMap := &corev1.ConfigMap{}
	if err := c.Get(context.TODO(), apitypes.NamespacedName{
		Name:      consts.KatibConfigMapName,
		Namespace: consts.DefaultKatibNamespace,
	}, configMap); err != nil {
		return 0, err
	}
	raw, ok := configMap.Data[consts.LabelKatibConfigTag]
	if !ok {
		return 0, fmt.Errorf("katib-config.yaml missing from ConfigMap %s", consts.KatibConfigMapName)
	}

	katibConfig := &configv1beta1.KatibConfig{}
	codecs := serializer.NewCodecFactory(c.Scheme())
	if err := runtime.DecodeInto(codecs.UniversalDecoder(), []byte(raw), katibConfig); err != nil {
		return 0, err
	}

	var fallback int32
	for _, entry := range katibConfig.InitConfig.ControllerConfig.NamespaceScheduling {
		if entry.Namespace == namespace {
			return entry.MaxRunningTrials, nil
		}
		if entry.Namespace == "" {
			fallback = entry.MaxRunningTrials
		}
	}
	if fallback > 0 {
		return fallback, nil
	}
	return 0, fmt.Errorf("no scheduling budget configured for namespace %q", namespace)
}
