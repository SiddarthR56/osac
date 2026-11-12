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
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bmfov1alpha1 "github.com/osac-project/osac/bare-metal-fulfillment-operator/api/v1alpha1"
	osacv1alpha1 "github.com/osac-project/osac/osac-operator/api/v1alpha1"
)

var _ = Describe("BareMetalInstance automatic ExternalIP readiness Envtest", func() {
	var (
		bareMetalNamespace  string
		networkingNamespace string
		bmi                 *bmfov1alpha1.BareMetalInstance
	)

	BeforeEach(func() {
		suffix := uuid.NewString()[:8]
		bareMetalNamespace = "bmi-cleanup-" + suffix
		networkingNamespace = "net-cleanup-" + suffix
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: bareMetalNamespace}})).To(Succeed())
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: networkingNamespace}})).To(Succeed())

		bmi = &bmfov1alpha1.BareMetalInstance{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "instance",
				Namespace:  bareMetalNamespace,
				Labels:     map[string]string{osacBareMetalInstanceIDLabel: "bmi-" + suffix},
				Finalizers: []string{bareMetalInstanceCleanupFinalizer},
			},
			Spec: bmfov1alpha1.BareMetalInstanceSpec{
				ExternalHostID: "host-" + suffix,
				Selector:       bmfov1alpha1.HostSelectorSpec{HostSelector: map[string]string{"managedBy": "test"}},
				TemplateID:     "test_template",
				RunStrategy:    bmfov1alpha1.RunStrategyAlways,
			},
			Status: bmfov1alpha1.BareMetalInstanceStatus{
				Phase:       bmfov1alpha1.BareMetalInstancePhaseReady,
				RunStrategy: bmfov1alpha1.RunStrategyAlways,
				NetworkAttachmentStatuses: []bmfov1alpha1.BareMetalNetworkAttachmentStatus{{
					Primary: true, IPAddress: "192.0.2.12",
				}},
			},
		}
		Expect(k8sClient.Create(ctx, bmi)).To(Succeed())
		bmi.Status = bmfov1alpha1.BareMetalInstanceStatus{
			Phase:       bmfov1alpha1.BareMetalInstancePhaseReady,
			RunStrategy: bmfov1alpha1.RunStrategyAlways,
			NetworkAttachmentStatuses: []bmfov1alpha1.BareMetalNetworkAttachmentStatus{{
				Primary: true, IPAddress: "192.0.2.12",
			}},
		}
		Expect(k8sClient.Status().Update(ctx, bmi)).To(Succeed())
	})

	AfterEach(func() {
		instances := &bmfov1alpha1.BareMetalInstanceList{}
		Expect(k8sClient.List(ctx, instances, client.InNamespace(bareMetalNamespace))).To(Succeed())
		for i := range instances.Items {
			instance := &instances.Items[i]
			instance.Finalizers = nil
			Expect(k8sClient.Update(ctx, instance)).To(Succeed())
			Expect(k8sClient.Delete(ctx, instance)).To(Succeed())
		}
		for _, namespace := range []string{bareMetalNamespace, networkingNamespace} {
			current := &corev1.Namespace{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: namespace}, current); err == nil {
				Expect(k8sClient.Delete(ctx, current)).To(Succeed())
			} else {
				Expect(apierrors.IsNotFound(err)).To(BeTrue(), "get namespace %s: %v", namespace, err)
			}
		}
	})

	It("uses persisted Ready and Always state to create one automatic attachment", func() {
		target := testAutomaticAttachmentTarget(bareMetalInstanceOwner, bmi.Labels[osacBareMetalInstanceIDLabel], "tenant-1")
		ip := testAutomaticExternalIP("eip-"+bmi.Name, target, "", "198.51.100.20")
		ip.Namespace = networkingNamespace
		allocatedStatus := ip.Status
		Expect(k8sClient.Create(ctx, ip)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(ip), ip)).To(Succeed())
		ip.Status = allocatedStatus
		Expect(k8sClient.Status().Update(ctx, ip)).To(Succeed())
		waitForIndexedClientObject(ip)
		persistedIP := &osacv1alpha1.ExternalIP{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(ip), persistedIP)).To(Succeed())
		Expect(persistedIP.Status.Address).To(Equal("198.51.100.20"))

		instancesClient := &mockBareMetalInstancesClient{getResponse: automaticExternalAccessBMIResponse()}
		attachmentsClient := &testExternalIPAttachmentsClient{}
		reconciler := NewBareMetalInstanceCleanupReconciler(autoExternalIPCleanupTestClient(), bareMetalNamespace, networkingNamespace)
		reconciler.BareMetalInstancesClient = instancesClient
		reconciler.ExternalIPAttachmentsClient = attachmentsClient
		request := ctrl.Request{NamespacedName: types.NamespacedName{Name: bmi.Name, Namespace: bareMetalNamespace}}

		result, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeZero())
		Expect(instancesClient.getCount).To(Equal(1))
		Expect(attachmentsClient.created).To(HaveLen(1))

		result, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeZero())
		Expect(attachmentsClient.created).To(HaveLen(1), "a second reconcile must not duplicate the attachment")
	})

	It("does not request an automatic attachment after the API sets deletionTimestamp", func() {
		instancesClient := &mockBareMetalInstancesClient{getResponse: automaticExternalAccessBMIResponse()}
		attachmentsClient := &testExternalIPAttachmentsClient{}
		reconciler := NewBareMetalInstanceCleanupReconciler(autoExternalIPCleanupTestClient(), bareMetalNamespace, networkingNamespace)
		reconciler.BareMetalInstancesClient = instancesClient
		reconciler.ExternalIPAttachmentsClient = attachmentsClient
		Expect(k8sClient.Delete(ctx, bmi)).To(Succeed())
		Eventually(func() error {
			return k8sClient.Get(ctx, client.ObjectKeyFromObject(bmi), bmi)
		}).Should(Succeed())
		Expect(bmi.DeletionTimestamp.IsZero()).To(BeFalse())

		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(bmi)})
		Expect(err).NotTo(HaveOccurred())
		Expect(instancesClient.getCount).To(BeZero())
		Expect(attachmentsClient.created).To(BeEmpty())
		err = k8sClient.Get(ctx, client.ObjectKeyFromObject(bmi), &bmfov1alpha1.BareMetalInstance{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})
})
