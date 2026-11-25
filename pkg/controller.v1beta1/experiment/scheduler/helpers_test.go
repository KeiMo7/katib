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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"

	configv1beta1 "github.com/kubeflow/katib/pkg/apis/config/v1beta1"
	trialsv1beta1 "github.com/kubeflow/katib/pkg/apis/controller/trials/v1beta1"
)

func ptrRuntime(t trialsv1beta1.Trial) runtime.Object { return &t }

func runtimeEncode(scheme *runtime.Scheme, config *configv1beta1.KatibConfig) (string, error) {
	config.TypeMeta.APIVersion = configv1beta1.GroupVersion.String()
	config.TypeMeta.Kind = "KatibConfig"
	codecs := serializer.NewCodecFactory(scheme)
	out, err := runtime.Encode(codecs.LegacyCodec(configv1beta1.GroupVersion), config)
	if err != nil {
		return "", err
	}
	return string(out), nil
}
