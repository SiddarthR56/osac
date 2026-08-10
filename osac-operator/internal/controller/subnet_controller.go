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
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mc "sigs.k8s.io/multicluster-runtime/pkg/multicluster"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	"github.com/osac-project/osac/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac/osac-operator/pkg/dispatcher"
	"github.com/osac-project/osac/osac-operator/pkg/provisioning"
)

const (
	// osacSubnetFinalizer is the finalizer for Subnet resources
	osacSubnetFinalizer = "osac.openshift.io/subnet-finalizer"
)

// SubnetReconciler reconciles a Subnet object
type SubnetReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
	// mgr and targetCluster are stored for future multi-cluster target client resolution
	mgr                  mcmanager.Manager
	NetworkingNamespace  string
	ProvisioningProvider provisioning.ProvisioningProvider
	StatusPollInterval   time.Duration
	MaxJobHistory        int
	targetCluster        mc.ClusterName
	// Resolver resolves a NetworkClass to its registered managers. Nil when the
	// two-manager model isn't configured (no gRPC connection / networking namespace),
	// in which case the controller always uses the legacy implementation-strategy path.
	Resolver *dispatcher.Resolver
}

// NewSubnetReconciler creates a new reconciler for Subnet resources.
func NewSubnetReconciler(
	mgr mcmanager.Manager,
	networkingNamespace string,
	provisioningProvider provisioning.ProvisioningProvider,
	statusPollInterval time.Duration,
	maxJobHistory int,
	targetCluster mc.ClusterName,
	resolver *dispatcher.Resolver,
) *SubnetReconciler {
	if mgr == nil {
		panic("mgr must not be nil")
	}
	if statusPollInterval <= 0 {
		statusPollInterval = provisioning.DefaultStatusPollInterval
	}
	if maxJobHistory <= 0 {
		maxJobHistory = provisioning.DefaultMaxJobHistory
	}
	return &SubnetReconciler{
		Client:               mgr.GetLocalManager().GetClient(),
		APIReader:            mgr.GetLocalManager().GetAPIReader(),
		Scheme:               mgr.GetLocalManager().GetScheme(),
		mgr:                  mgr,
		NetworkingNamespace:  networkingNamespace,
		ProvisioningProvider: provisioningProvider,
		StatusPollInterval:   statusPollInterval,
		MaxJobHistory:        maxJobHistory,
		targetCluster:        targetCluster,
		Resolver:             resolver,
	}
}

// +kubebuilder:rbac:groups=osac.openshift.io,resources=subnets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=osac.openshift.io,resources=subnets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=osac.openshift.io,resources=subnets/finalizers,verbs=update
// +kubebuilder:rbac:groups=osac.openshift.io,resources=virtualnetworks,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *SubnetReconciler) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
	log := ctrllog.FromContext(ctx)

	subnet := &v1alpha1.Subnet{}
	err := r.Client.Get(ctx, req.NamespacedName, subnet)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	val, exists := subnet.Annotations[osacManagementStateAnnotation]
	if subnet.ObjectMeta.DeletionTimestamp.IsZero() && exists && val == ManagementStateUnmanaged {
		log.Info("ignoring Subnet due to management-state annotation", "management-state", val)
		return ctrl.Result{}, nil
	}

	log.Info("start reconcile")

	oldstatus := subnet.Status.DeepCopy()

	var res ctrl.Result
	if subnet.ObjectMeta.DeletionTimestamp.IsZero() {
		res, err = r.handleUpdate(ctx, subnet)
	} else {
		res, err = r.handleDelete(ctx, subnet)
	}

	if !equality.Semantic.DeepEqual(subnet.Status, *oldstatus) {
		log.Info("status requires update")
		if err := r.updateStatusWithRetry(ctx, client.ObjectKeyFromObject(subnet), subnet.Status); err != nil {
			return res, err
		}
	}

	log.Info("end reconcile")
	return res, err
}

// updateStatusWithRetry updates the subnet status with retry on conflict.
func (r *SubnetReconciler) updateStatusWithRetry(ctx context.Context, key client.ObjectKey, newStatus v1alpha1.SubnetStatus) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &v1alpha1.Subnet{}
		if err := r.Get(ctx, key, latest); err != nil {
			return err
		}
		latest.Status = newStatus
		return r.Status().Update(ctx, latest)
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *SubnetReconciler) SetupWithManager(mgr mcmanager.Manager) error {
	return mcbuilder.ControllerManagedBy(mgr).
		For(&v1alpha1.Subnet{},
			mcbuilder.WithPredicates(NetworkingNamespacePredicate(r.NetworkingNamespace)),
			mcbuilder.WithEngageWithLocalCluster(true),
			mcbuilder.WithEngageWithProviderClusters(false)).
		Complete(r)
}

func (r *SubnetReconciler) handleUpdate(ctx context.Context, subnet *v1alpha1.Subnet) (ctrl.Result, error) {
	log := ctrllog.FromContext(ctx)

	// Add finalizer if not present
	if controllerutil.AddFinalizer(subnet, osacSubnetFinalizer) {
		if err := r.Update(ctx, subnet); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Set phase to Progressing only on first reconcile (empty phase).
	// Subsequent reconciles preserve the current phase — it gets updated
	// by OnSuccess/OnFailed callbacks in RunProvisioningLifecycle.
	if subnet.Status.Phase == "" {
		subnet.Status.Phase = v1alpha1.SubnetPhaseProgressing
	}

	// Get parent VirtualNetwork by UUID label to read implementation strategy
	vnetList := &v1alpha1.VirtualNetworkList{}
	err := r.List(ctx, vnetList,
		client.InNamespace(subnet.Namespace),
		client.MatchingLabels{osacVirtualNetworkIDLabel: subnet.Spec.VirtualNetwork},
	)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(vnetList.Items) == 0 {
		log.Info("parent VirtualNetwork not found, requeueing", "uuid", subnet.Spec.VirtualNetwork)
		return ctrl.Result{RequeueAfter: defaultPreconditionRequeueInterval}, nil
	}
	if len(vnetList.Items) > 1 {
		return ctrl.Result{}, fmt.Errorf(
			"expected exactly one parent VirtualNetwork with uuid %q but found %d",
			subnet.Spec.VirtualNetwork, len(vnetList.Items))
	}
	vnet := &vnetList.Items[0]

	// Resolve the dispatch plan: dispatcher path when the parent VirtualNetwork's
	// NetworkClass has a fabricManager registered (plan non-nil), else the legacy
	// implementation_strategy annotation path (from the parent VirtualNetwork spec,
	// plan nil). Subnet is the only resource kind whose plan can carry a k8s target
	// alongside the fabric one — see pkg/dispatcher's dispatch table.
	plan, err := resolveDispatchPlan(ctx, r.Resolver, "Subnet", vnet.Spec.NetworkClass)
	if err != nil {
		return ctrl.Result{}, err
	}
	implementationStrategy := vnet.Spec.ImplementationStrategy
	if fabricTarget := plan.FabricTarget(); fabricTarget != nil {
		implementationStrategy = fabricTarget.Manager.Name
	}
	if implementationStrategy == "" {
		log.Info("implementation strategy not set on parent VirtualNetwork, requeueing", "virtualNetwork", vnet.Name)
		return ctrl.Result{RequeueAfter: defaultPreconditionRequeueInterval}, nil
	}

	// Add implementation-strategy annotation(s) if not present or different.
	// This allows AAP playbooks to select the appropriate role without doing lookups.
	// osacImplementationStrategyAnnotation always holds the fabric manager's name; when
	// the plan also resolves a k8s target (dual-dispatch), osacK8sImplementationStrategyAnnotation
	// persists the k8s manager's name so handleDeprovisioning can build its
	// DeprovisionTarget without re-resolving the plan against a parent VirtualNetwork
	// that may already be gone at delete time. The k8s annotation is compared and, when
	// absent from the plan, removed unconditionally (not just when present) so a Subnet
	// that transitions from dual-dispatch to fabric-only (NetworkClass drops its
	// k8sManager) doesn't leave a stale k8s target for handleDeprovisioning to act on.
	if subnet.Annotations == nil {
		subnet.Annotations = make(map[string]string)
	}
	k8sTarget := plan.K8sTarget()
	k8sStrategy := ""
	if k8sTarget != nil {
		k8sStrategy = k8sTarget.Manager.Name
	}
	annotationsChanged := subnet.Annotations[osacImplementationStrategyAnnotation] != implementationStrategy ||
		subnet.Annotations[osacK8sImplementationStrategyAnnotation] != k8sStrategy
	if annotationsChanged {
		subnet.Annotations[osacImplementationStrategyAnnotation] = implementationStrategy
		if k8sTarget != nil {
			subnet.Annotations[osacK8sImplementationStrategyAnnotation] = k8sStrategy
		} else {
			delete(subnet.Annotations, osacK8sImplementationStrategyAnnotation)
		}
		log.Info("setting implementation-strategy annotation", "strategy", implementationStrategy, "k8sStrategy", k8sStrategy)
		if err := r.Update(ctx, subnet); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Compute desired config version from spec and inherited implementation strategy
	desiredVersion, err := provisioning.ComputeDesiredConfigVersion(struct {
		Spec                   v1alpha1.SubnetSpec
		ImplementationStrategy string
	}{subnet.Spec, implementationStrategy})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to compute desired config version: %w", err)
	}
	subnet.Status.DesiredConfigVersion = desiredVersion

	// Set phase to Progressing only on first provision (empty phase) or when spec changed
	// after a previous success. Don't override Failed during backoff.
	if subnet.Status.Phase == "" || (subnet.Status.Phase == v1alpha1.SubnetPhaseReady &&
		!provisioning.IsConfigApplied(&subnet.Status.ProvisioningJobs, subnet.Status.DesiredConfigVersion)) {
		subnet.Status.Phase = v1alpha1.SubnetPhaseProgressing
	}

	// Handle provisioning
	return r.handleProvisioning(ctx, subnet, plan)
}

func (r *SubnetReconciler) handleDelete(ctx context.Context, subnet *v1alpha1.Subnet) (ctrl.Result, error) {
	log := ctrllog.FromContext(ctx)
	log.Info("deleting subnet")

	subnet.Status.Phase = v1alpha1.SubnetPhaseDeleting

	// Base finalizer has already been removed, cleanup complete
	if !controllerutil.ContainsFinalizer(subnet, osacSubnetFinalizer) {
		return ctrl.Result{}, nil
	}

	// Handle deprovisioning
	result, err := r.handleDeprovisioning(ctx, subnet)
	if err != nil {
		return result, err
	}

	// If we need to requeue (jobs still running), do so
	if result.RequeueAfter > 0 {
		return result, nil
	}

	// Deprovisioning complete or skipped, remove base finalizer
	if controllerutil.RemoveFinalizer(subnet, osacSubnetFinalizer) {
		if err := r.Update(ctx, subnet); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// subnetProvisioningJobsExtractor extracts the Subnet-typed jobs array used by
// CheckAPIServerForNonTerminalProvisionJob(AndTarget) to read jobs from a fresh
// API server copy of the resource.
func subnetProvisioningJobsExtractor(obj client.Object) []v1alpha1.JobStatus {
	return obj.(*v1alpha1.Subnet).Status.ProvisioningJobs //nolint:forcetypeassert // always called with a *v1alpha1.Subnet
}

// handleProvisioning manages the provisioning job lifecycle for a Subnet. When plan
// has no k8s target (fabric-only dispatch or the no-dispatcher legacy path), this is
// the single-target RunProvisioningLifecycle unchanged. When plan has both a fabric
// and a k8s target (dual-dispatch), it drives both targets in parallel via
// RunMultiTargetProvisioningLifecycle: each target triggers/polls its own AAP job
// through a dispatchTargetProvider routing to that target's manager, and the Subnet
// only reaches Ready once bothProvisionTargetsSucceeded reports both targets'
// latest jobs succeeded at the current desired config version — one target
// succeeding does not flip Ready on its own, and one target failing/backing off
// does not block the other target's independent retry.
func (r *SubnetReconciler) handleProvisioning(ctx context.Context, subnet *v1alpha1.Subnet, plan *dispatcher.DispatchPlan) (ctrl.Result, error) {
	if r.ProvisioningProvider == nil {
		ctrllog.FromContext(ctx).Info("no provisioning provider configured, skipping provisioning")
		return ctrl.Result{}, nil
	}

	k8sTarget := plan.K8sTarget()
	if k8sTarget == nil {
		return provisioning.RunProvisioningLifecycle(ctx, r.ProvisioningProvider, subnet,
			&provisioning.State{Jobs: &subnet.Status.ProvisioningJobs, DesiredConfigVersion: subnet.Status.DesiredConfigVersion},
			r.MaxJobHistory, r.StatusPollInterval,
			&provisioning.PollCallbacks{
				OnFailed: func(message string) {
					subnet.Status.Phase = v1alpha1.SubnetPhaseFailed
					setReadyConditionFailed(&subnet.Status.Conditions, message)
				},
				OnSuccess: func(_ provisioning.ProvisionStatus) {
					subnet.Status.Phase = v1alpha1.SubnetPhaseReady
					setReadyConditionTrue(&subnet.Status.Conditions)
				},
			},
			func() bool {
				return provisioning.CheckAPIServerForNonTerminalProvisionJob(ctx, r.APIReader, client.ObjectKeyFromObject(subnet), &v1alpha1.Subnet{}, subnetProvisioningJobsExtractor)
			},
			func() error {
				return r.updateStatusWithRetry(ctx, client.ObjectKeyFromObject(subnet), subnet.Status)
			},
		)
	}

	fabricTarget := plan.FabricTarget()
	if fabricTarget == nil {
		// Defensive: pkg/dispatcher's dispatch table always requests the fabric role
		// for Subnet, so a resolved plan with a k8s target always has a fabric target too.
		return ctrl.Result{}, fmt.Errorf("dispatch plan for subnet %s/%s has a k8s target but no fabric target", subnet.Namespace, subnet.Name)
	}

	fabricName := string(dispatcher.ManagerRoleFabric)
	k8sName := string(dispatcher.ManagerRoleK8s)

	onFailedFor := func(targetName string) func(string) {
		return func(message string) {
			subnet.Status.Phase = v1alpha1.SubnetPhaseFailed
			setReadyConditionFailed(&subnet.Status.Conditions, fmt.Sprintf("%s target: %s", targetName, message))
		}
	}
	onSuccess := func(_ provisioning.ProvisionStatus) {
		if bothProvisionTargetsSucceeded(subnet.Status.ProvisioningJobs, subnet.Status.DesiredConfigVersion, fabricName, k8sName) {
			subnet.Status.Phase = v1alpha1.SubnetPhaseReady
			setReadyConditionTrue(&subnet.Status.Conditions)
		}
	}
	checkAPIServerFor := func(targetName string) func() bool {
		return func() bool {
			return provisioning.CheckAPIServerForNonTerminalProvisionJobAndTarget(
				ctx, r.APIReader, client.ObjectKeyFromObject(subnet), &v1alpha1.Subnet{}, subnetProvisioningJobsExtractor, targetName)
		}
	}

	targets := []provisioning.JobTarget{
		{
			Name:           fabricName,
			Provider:       newDispatchTargetProvider(r.ProvisioningProvider, fabricTarget.Manager.Name),
			Callbacks:      &provisioning.PollCallbacks{OnFailed: onFailedFor(fabricName), OnSuccess: onSuccess},
			CheckAPIServer: checkAPIServerFor(fabricName),
			// Subnet was fabric-only (single, untargeted job history) before
			// dual-dispatch existed, so fabric inherits any pre-existing
			// Target=="" jobs when a NetworkClass gains a k8sManager.
			AbsorbsLegacyHistory: true,
		},
		{
			Name:           k8sName,
			Provider:       newDispatchTargetProvider(r.ProvisioningProvider, k8sTarget.Manager.Name),
			Callbacks:      &provisioning.PollCallbacks{OnFailed: onFailedFor(k8sName), OnSuccess: onSuccess},
			CheckAPIServer: checkAPIServerFor(k8sName),
		},
	}

	return provisioning.RunMultiTargetProvisioningLifecycle(ctx, targets, subnet,
		&provisioning.State{Jobs: &subnet.Status.ProvisioningJobs, DesiredConfigVersion: subnet.Status.DesiredConfigVersion},
		r.MaxJobHistory, r.StatusPollInterval,
		func() error {
			return r.updateStatusWithRetry(ctx, client.ObjectKeyFromObject(subnet), subnet.Status)
		},
	)
}

// bothProvisionTargetsSucceeded reports whether every named target's most recent
// provision job succeeded at desiredVersion (or is a pre-Target legacy success with
// ConfigVersion == "" — mirrors IsConfigApplied's same accommodation). Used from
// each target's OnSuccess callback to decide whether the aggregate Subnet Phase can
// flip to Ready: Ready requires ALL targets independently confirmed successful, not
// just the one whose callback just fired.
func bothProvisionTargetsSucceeded(jobs []v1alpha1.JobStatus, desiredVersion string, targetNames ...string) bool {
	for _, name := range targetNames {
		job := provisioning.FindLatestJobByTypeAndTarget(jobs, v1alpha1.JobTypeProvision, name)
		if job == nil || job.State != v1alpha1.JobStateSucceeded {
			return false
		}
		if job.ConfigVersion != desiredVersion && job.ConfigVersion != "" {
			return false
		}
	}
	return true
}

// handleDeprovisioning manages the deprovisioning job lifecycle for a Subnet. It
// trusts the annotations persisted by handleUpdate rather than re-resolving the
// DispatchPlan against the parent VirtualNetwork's NetworkClass, since the parent
// may already be gone or deleting concurrently by the time a Subnet is deleted.
// When osacK8sImplementationStrategyAnnotation is absent (fabric-only dispatch, the
// no-dispatcher legacy path, or a Subnet that predates this feature), this is the
// single-target RunDeprovisioningLifecycle unchanged. When present, both managers
// are torn down in parallel via RunMultiTargetDeprovisioningLifecycle, and the
// finalizer is only removed once both reach a terminal, non-blocking state.
func (r *SubnetReconciler) handleDeprovisioning(ctx context.Context, subnet *v1alpha1.Subnet) (ctrl.Result, error) {
	if r.ProvisioningProvider == nil {
		ctrllog.FromContext(ctx).Info("no provisioning provider configured, skipping deprovisioning")
		return ctrl.Result{}, nil
	}

	k8sStrategy := subnet.Annotations[osacK8sImplementationStrategyAnnotation]
	if k8sStrategy == "" {
		result, done, err := provisioning.RunDeprovisioningLifecycle(ctx, r.ProvisioningProvider, subnet,
			&subnet.Status.ProvisioningJobs, r.MaxJobHistory, r.StatusPollInterval)
		if err != nil || !done {
			return result, err
		}
		return ctrl.Result{}, nil
	}

	fabricStrategy := subnet.Annotations[osacImplementationStrategyAnnotation]
	if fabricStrategy == "" {
		// Defensive: handleUpdate always sets the fabric annotation whenever it sets the
		// k8s one, so this should not happen in practice. An empty manager name would
		// otherwise route the fabric deprovision job to an undefined AAP role.
		return ctrl.Result{}, fmt.Errorf(
			"subnet %s/%s has %s set but %s is empty", subnet.Namespace, subnet.Name,
			osacK8sImplementationStrategyAnnotation, osacImplementationStrategyAnnotation)
	}
	targets := []provisioning.DeprovisionTarget{
		// AbsorbsLegacyHistory: true — see the matching comment in handleProvisioning.
		{Name: string(dispatcher.ManagerRoleFabric), Provider: newDispatchTargetProvider(r.ProvisioningProvider, fabricStrategy), AbsorbsLegacyHistory: true},
		{Name: string(dispatcher.ManagerRoleK8s), Provider: newDispatchTargetProvider(r.ProvisioningProvider, k8sStrategy)},
	}

	result, done, err := provisioning.RunMultiTargetDeprovisioningLifecycle(ctx, targets, subnet,
		&subnet.Status.ProvisioningJobs, r.MaxJobHistory, r.StatusPollInterval)
	if err != nil || !done {
		return result, err
	}
	return ctrl.Result{}, nil
}
