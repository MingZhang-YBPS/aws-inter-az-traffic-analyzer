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
	"os"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go/ptr"

	reportv1alpha1 "github.com/MingZhang-YBPS/aws-inter-az-traffic-analyzer/api/v1alpha1"
	"github.com/MingZhang-YBPS/aws-inter-az-traffic-analyzer/internal/util"
)

// InterAZTrafficReconciler reconciles a InterAZTraffic object
type InterAZTrafficReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	AWSCfg aws.Config
}

const finalizer = "report.k8s.aws/finalizer"

// +kubebuilder:rbac:groups=report.k8s.aws,resources=interaztraffics,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=report.k8s.aws,resources=interaztraffics/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=report.k8s.aws,resources=interaztraffics/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the InterAZTraffic object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *InterAZTrafficReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	traffic := &reportv1alpha1.InterAZTraffic{}
	err := r.Get(ctx, req.NamespacedName, traffic)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			log.Info("InterAZTraffic resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "Failed to get InterAZTraffic")
		return ctrl.Result{}, err
	}

	// Check if the InterAZTraffic instance is marked to be deleted, which is
	// indicated by the deletion timestamp being set.
	isInstanceToBeDeleted := traffic.GetDeletionTimestamp() != nil
	if isInstanceToBeDeleted {
		if controllerutil.ContainsFinalizer(traffic, finalizer) {
			// Run finalization logic. If the finalization logic fails, don't remove the finalizer
			// so that we can retry during the next reconciliation.
			if err := r.finalize(ctx); err != nil {
				return ctrl.Result{}, err
			}

			// Remove finalizer. Once all finalizers have been
			// removed, the object will be deleted.
			controllerutil.RemoveFinalizer(traffic, finalizer)
			err := r.Update(ctx, traffic)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(traffic, finalizer) {
		controllerutil.AddFinalizer(traffic, finalizer)
		err = r.Update(ctx, traffic)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	vpcId := traffic.Spec.VPCId
	startFrom := traffic.Spec.StartFrom
	endTo := traffic.Spec.EndTo
	log.Info("Reconciling VPC", "vpcId", vpcId, "startFrom", startFrom, "endTo", endTo)

	err = util.EnsurePrerequisites(ctx, r.AWSCfg, vpcId)
	if err != nil {
		// retry after the duration, avoid request throttled by AWS
		return ctrl.Result{RequeueAfter: 60 * time.Second}, err
	}

	err = r.createOrUpdateJobs(ctx, traffic)
	if err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}

	return ctrl.Result{}, nil
}

func (r *InterAZTrafficReconciler) finalize(ctx context.Context) error {
	return r.cleanUpAWSResources(ctx)
}

func (r *InterAZTrafficReconciler) cleanUpAWSResources(ctx context.Context) error {
	// TODO
	return nil
}

func (r *InterAZTrafficReconciler) createOrUpdateAnalyzeJob(ctx context.Context, traffic *reportv1alpha1.InterAZTraffic) error {
	// TODO
	// each VPC has multiple analyze job, each job associated with a InterAZTraffic instance
	return nil
}

func (r *InterAZTrafficReconciler) createOrUpdateJobs(ctx context.Context, traffic *reportv1alpha1.InterAZTraffic) error {
	if _, err := r.createOrUpdatePodMetadataCronjob(ctx, traffic); err != nil {
		return err
	}
	return r.createOrUpdateAnalyzeJob(ctx, traffic)
}

func (r *InterAZTrafficReconciler) createOrUpdatePodMetadataCronjob(ctx context.Context,
	traffic *reportv1alpha1.InterAZTraffic) (job string, err error) {
	// each VPC has one single dedicated pod metadata extractor cronjob
	resourceName := types.NamespacedName{Name: "pod-metadata-" + traffic.Spec.VPCId, Namespace: traffic.Namespace}

	sa := corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName.Name,
			Namespace: resourceName.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, &sa, func() error {
		return nil
	})
	if err != nil {
		klog.Error(err)
		return "", err
	}
	err = controllerutil.SetControllerReference(traffic, &sa, r.Scheme)
	if err != nil {
		klog.Error(err)
		return "", err
	}

	clusterRole := rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: resourceName.Name,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods", "nodes"},
				Verbs:     []string{"get", "list", "watch"},
			},
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, &clusterRole, func() error {
		return nil
	})
	if err != nil {
		klog.Error(err)
		return "", err
	}
	err = controllerutil.SetControllerReference(traffic, &clusterRole, r.Scheme)
	if err != nil {
		klog.Error(err)
		return "", err
	}

	binding := rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: resourceName.Name,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      sa.Name,
				Namespace: sa.Namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     clusterRole.Name,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, &binding, func() error {
		return nil
	})
	if err != nil {
		klog.Error(err)
		return "", err
	}
	err = controllerutil.SetControllerReference(traffic, &binding, r.Scheme)
	if err != nil {
		klog.Error(err)
		return "", err
	}

	cronJob := batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName.Name,
			Namespace: resourceName.Namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, &cronJob, func() error {
		cronJob.Spec = batchv1.CronJobSpec{
			Schedule:          "*/10 * * * *",           // every 10 minutes
			ConcurrencyPolicy: batchv1.ForbidConcurrent, // don't run if previous job still running
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					BackoffLimit:          ptr.Int32(3),
					ActiveDeadlineSeconds: ptr.Int64(60), // timeout per job
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{

							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{
								{
									Name:            "extractor",
									Image:           os.Getenv("MY_POD_IMAGE"),
									ImagePullPolicy: corev1.PullAlways,
									Env: []corev1.EnvVar{
										{
											Name:  "AWS_REGION",
											Value: os.Getenv("AWS_REGION"),
										},
										{
											Name:  "VPC_ID",
											Value: traffic.Spec.VPCId,
										},
									},
								},
							},
						},
					},
				},
			},
		}
		return nil
	})
	if err != nil {
		klog.Error(err)
		return "", err
	}
	err = controllerutil.SetControllerReference(traffic, &cronJob, r.Scheme)
	if err != nil {
		klog.Error(err)
		return "", err
	}

	return resourceName.Name, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *InterAZTrafficReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&reportv1alpha1.InterAZTraffic{}).
		Named("interaztraffic").
		WithEventFilter(predicate.GenerationChangedPredicate{}). // TODO: no update allowed
		Complete(r)
}
