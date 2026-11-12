/*
Copyright 2026.

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
	"context"
	"testing"
	"time"

	bmfov1alpha1 "github.com/osac-project/osac/bare-metal-fulfillment-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

func TestBareMetalInstanceCleanupDoesNotRequestAutomaticAttachmentAfterDeletionStarts(t *testing.T) {
	bmi := cleanupTestBMI(cleanupBMID)
	bmi.Finalizers = []string{cleanupFinalizer}
	bmi.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	bmi.Status = bmfov1alpha1.BareMetalInstanceStatus{
		Phase:       bmfov1alpha1.BareMetalInstancePhaseReady,
		RunStrategy: bmfov1alpha1.RunStrategyAlways,
		NetworkAttachmentStatuses: []bmfov1alpha1.BareMetalNetworkAttachmentStatus{{
			Primary: true, IPAddress: "192.0.2.12",
		}},
	}
	target := testAutomaticAttachmentTarget(bareMetalInstanceOwner, cleanupBMID, "tenant-1")
	ip := testAutomaticExternalIP("eip-1", target, "", "198.51.100.20")
	c := cleanupTestClient(t, bmi, ip)
	instancesClient := &mockBareMetalInstancesClient{getResponse: automaticExternalAccessBMIResponse()}
	attachmentsClient := &testExternalIPAttachmentsClient{}
	r := NewBareMetalInstanceCleanupReconciler(c, cleanupBMNamespace, "networking")
	r.BareMetalInstancesClient = instancesClient
	r.ExternalIPAttachmentsClient = attachmentsClient

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Name: "instance", Namespace: cleanupBMNamespace,
	}}); err != nil {
		t.Fatalf("reconcile deleting BMI: %v", err)
	}
	if instancesClient.getCount != 0 {
		t.Errorf("Fulfillment Get calls = %d, want 0 after deletion starts", instancesClient.getCount)
	}
	if len(attachmentsClient.created) != 0 {
		t.Errorf("created %d attachments after deletion started", len(attachmentsClient.created))
	}
}
