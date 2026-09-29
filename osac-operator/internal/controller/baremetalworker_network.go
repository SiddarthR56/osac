/*
Copyright 2025.

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

package controller

import (
	"fmt"

	bmfov1alpha1 "github.com/osac-project/osac/bare-metal-fulfillment-operator/api/v1alpha1"
	"github.com/osac-project/osac/osac-operator/api/v1alpha1"
)

// buildWorkerNetworkAttachments builds the one-entry BareMetalInstance
// network_attachments list for BMI Create from the ClusterOrder's shared
// networkAttachment plus the NodeRequest's stored FabricInterface.
//
// The fabric interface must already have been resolved at Cluster creation and
// copied onto the NodeRequest; this helper never looks up BareMetalInstanceType
// or HostType. Concrete BMIProvider.CreateBMI implementations must use this
// (or equivalent) and must not re-resolve the NIC name.
func buildWorkerNetworkAttachments(
	order *v1alpha1.ClusterOrder,
	nodeRequest v1alpha1.NodeRequest,
) ([]bmfov1alpha1.BareMetalNetworkAttachment, error) {
	if order == nil {
		return nil, fmt.Errorf("cluster order is required")
	}
	na := order.Spec.NetworkAttachment
	if na == nil {
		return nil, fmt.Errorf("spec.networkAttachment is required")
	}
	if na.SubnetRef == "" {
		return nil, fmt.Errorf("spec.networkAttachment.subnetRef is required")
	}
	if nodeRequest.FabricInterface == "" {
		return nil, fmt.Errorf("nodeRequest.fabricInterface is required")
	}
	return []bmfov1alpha1.BareMetalNetworkAttachment{{
		SubnetRef:         na.SubnetRef,
		SecurityGroupRefs: append([]string(nil), na.SecurityGroupRefs...),
		Interface:         nodeRequest.FabricInterface,
		Primary:           true,
	}}, nil
}
