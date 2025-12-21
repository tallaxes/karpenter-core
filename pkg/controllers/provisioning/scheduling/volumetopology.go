/*
Copyright The Kubernetes Authors.

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

package scheduling

import (
	"context"
	"fmt"
	"slices"

	"github.com/awslabs/operatorpkg/serrors"
	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	volumeutil "sigs.k8s.io/karpenter/pkg/utils/volume"
)

func NewVolumeTopology(kubeClient client.Client) *VolumeTopology {
	return &VolumeTopology{kubeClient: kubeClient}
}

type VolumeTopology struct {
	kubeClient client.Client
}

func (v *VolumeTopology) Inject(ctx context.Context, pod *v1.Pod) error {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &v1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &v1.NodeAffinity{}
	}
	if pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &v1.NodeSelector{}
	}

	// Start with existing terms. If empty, use a single empty term (matches everything).
	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		terms = []v1.NodeSelectorTerm{{}}
	}

	for _, volume := range pod.Spec.Volumes {
		volTerms, err := v.getRequirements(ctx, pod, volume)
		if err != nil {
			return err
		}
		if len(volTerms) == 0 {
			continue
		}

		// We add our volume topology zonal requirement to every node selector term.  This causes it to be AND'd with every existing
		// requirement so that relaxation won't remove our volume requirement.
		// Since volTerms are ORed, we need to compute the cross product: terms = terms X volTerms
		var newTerms []v1.NodeSelectorTerm
		for _, t1 := range terms {
			for _, t2 := range volTerms {
				newTerms = append(newTerms, v1.NodeSelectorTerm{
					MatchExpressions: slices.Concat(t1.MatchExpressions, t2.MatchExpressions),
					MatchFields:      slices.Concat(t1.MatchFields, t2.MatchFields),
				})
			}
		}
		terms = newTerms
	}

	pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = terms

	log.FromContext(ctx).
		WithValues("Pod", klog.KObj(pod)).
		V(1).Info(fmt.Sprintf("adding requirements derived from pod volumes, %s", terms))
	return nil
}

func (v *VolumeTopology) getRequirements(ctx context.Context, pod *v1.Pod, volume v1.Volume) ([]v1.NodeSelectorTerm, error) {
	pvc, err := volumeutil.GetPersistentVolumeClaim(ctx, v.kubeClient, pod, volume)
	if err != nil {
		return nil, fmt.Errorf("discovering persistent volume claim, %w", err)
	}
	// Not all volume types have PVCs, e.g. emptyDir, hostPath, etc.
	if pvc == nil {
		return nil, nil
	}

	// Persistent Volume Requirements
	if pvc.Spec.VolumeName != "" {
		requirements, err := v.getPersistentVolumeRequirements(ctx, pod, pvc.Spec.VolumeName)
		if err != nil {
			return nil, fmt.Errorf("getting existing requirements, %w", err)
		}
		return requirements, nil
	}
	// Storage Class Requirements
	if sc := lo.FromPtr(pvc.Spec.StorageClassName); sc != "" {
		requirements, err := v.getStorageClassRequirements(ctx, sc)
		if err != nil {
			return nil, err
		}
		return requirements, nil
	}
	return nil, nil
}

func (v *VolumeTopology) getStorageClassRequirements(ctx context.Context, storageClassName string) ([]v1.NodeSelectorTerm, error) {
	storageClass := &storagev1.StorageClass{}
	if err := v.kubeClient.Get(ctx, types.NamespacedName{Name: storageClassName}, storageClass); err != nil {
		return nil, serrors.Wrap(fmt.Errorf("getting storage class, %w", err), "StorageClass", klog.KRef("", storageClassName))
	}
	var terms []v1.NodeSelectorTerm
	for _, topology := range storageClass.AllowedTopologies {
		var requirements []v1.NodeSelectorRequirement
		for _, requirement := range topology.MatchLabelExpressions {
			requirements = append(requirements, v1.NodeSelectorRequirement{Key: requirement.Key, Operator: v1.NodeSelectorOpIn, Values: requirement.Values})
		}
		terms = append(terms, v1.NodeSelectorTerm{MatchExpressions: requirements})
	}
	return terms, nil
}

func (v *VolumeTopology) getPersistentVolumeRequirements(ctx context.Context, pod *v1.Pod, volumeName string) ([]v1.NodeSelectorTerm, error) {
	pv := &v1.PersistentVolume{}
	if err := v.kubeClient.Get(ctx, types.NamespacedName{Name: volumeName, Namespace: pod.Namespace}, pv); err != nil {
		return nil, serrors.Wrap(fmt.Errorf("getting persistent volume, %w", err), "PersistentVolume", klog.KRef("", volumeName))
	}
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return nil, nil
	}

	var terms []v1.NodeSelectorTerm
	for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		// If we are using a Local volume or a HostPath volume, then we should ignore the Hostname affinity
		// on it because re-scheduling this pod to a new node means not using the same Hostname affinity that we currently have
		if pv.Spec.Local != nil || pv.Spec.HostPath != nil {
			term.MatchExpressions = lo.Reject(term.MatchExpressions, func(req v1.NodeSelectorRequirement, _ int) bool {
				return req.Key == v1.LabelHostname
			})
		}
		terms = append(terms, term)
	}
	return terms, nil
}

// ValidatePersistentVolumeClaims returns an error if the pod doesn't appear to be valid with respect to
// PVCs (e.g. the PVC is not found or references an unknown storage class).
func (v *VolumeTopology) ValidatePersistentVolumeClaims(ctx context.Context, pod *v1.Pod) error {
	for _, volume := range pod.Spec.Volumes {
		pvc, err := volumeutil.GetPersistentVolumeClaim(ctx, v.kubeClient, pod, volume)
		if err != nil {
			return err
		}
		// Not all volume types have PVCs, e.g. emptyDir, hostPath, etc.
		if pvc == nil {
			continue
		}

		if pvc.Spec.VolumeName != "" {
			if err := v.validateVolume(ctx, pvc.Spec.VolumeName); err != nil {
				return serrors.Wrap(fmt.Errorf("failed to validate pvc, %w", err), "PersistentVolumeClaim", klog.KRef("", pvc.Name), "PersistentVolume", klog.KRef("", pvc.Spec.VolumeName))
			}
			continue
		}

		// PVC is unbound, we can't schedule unless the pod defines a valid storage class
		storageClassName := lo.FromPtr(pvc.Spec.StorageClassName)
		if storageClassName == "" {
			return serrors.Wrap(fmt.Errorf("unbound pvc must define a storage class"), "PersistentVolumeClaim", klog.KRef("", pvc.Name))
		}
		if err := v.validateStorageClass(ctx, storageClassName); err != nil {
			return serrors.Wrap(fmt.Errorf("failed to validate storage class, %w", err), "PersistentVolumeClaim", klog.KRef("", pvc.Name), "StorageClass", klog.KRef("", storageClassName))
		}
	}
	return nil
}

func (v *VolumeTopology) validateVolume(ctx context.Context, volumeName string) error {
	// we have a volume name, so ensure that it exists
	if volumeName != "" {
		pv := &v1.PersistentVolume{}
		if err := v.kubeClient.Get(ctx, types.NamespacedName{Name: volumeName}, pv); err != nil {
			return err
		}
	}
	return nil
}

func (v *VolumeTopology) validateStorageClass(ctx context.Context, storageClassName string) error {
	storageClass := &storagev1.StorageClass{}
	if err := v.kubeClient.Get(ctx, types.NamespacedName{Name: storageClassName}, storageClass); err != nil {
		return err
	}
	return nil
}
