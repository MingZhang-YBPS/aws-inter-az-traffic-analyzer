package controller

import (
	"context"
	log "github.com/sirupsen/logrus"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	reportv1alpha1 "github.com/MingZhang-YBPS/aws-inter-az-traffic-analyzer/api/v1alpha1"
	"github.com/MingZhang-YBPS/aws-inter-az-traffic-analyzer/internal/util"
)

type AnalyzerJobWatcherReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=report.k8s.aws,resources=interaztraffics,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=report.k8s.aws,resources=interaztraffics/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=report.k8s.aws,resources=interaztraffics/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch
func (r *AnalyzerJobWatcherReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {

	job := batchv1.Job{}
	if err := r.Get(ctx, req.NamespacedName, &job); err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			log.Info("Job resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "Failed to get Job")
		return ctrl.Result{}, err
	}

	if isJobComplete(&job) {

		interaztraffic := reportv1alpha1.InterAZTraffic{}
		if err := r.Get(ctx, req.NamespacedName, &interaztraffic); err != nil {
			log.Error(err, "Failed to get InterAZTraffic")
			return ctrl.Result{}, err
		}
		interaztraffic.Status.LastestReportLocation = job.Annotations[util.AnalyzerReportLocationAnnotation]
		interaztraffic.Status.LastestReportCreationTimeStamp = &v1.Time{Time: time.Now()}
		if err := r.Status().Update(ctx, &interaztraffic); err != nil {
			log.Error(err, "Failed to update InterAZTraffic")
			return ctrl.Result{}, err
		}

	} else {
		log.Warnf("Job resource is not complete. Ignoring it")
	}

	return ctrl.Result{}, nil
}

func (r *AnalyzerJobWatcherReconciler) SetupWithManager(mgr ctrl.Manager) error {

	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.Job{}).
		WithEventFilter(predicate.Funcs{
			UpdateFunc: func(e event.UpdateEvent) bool {
				oldJob, ok1 := e.ObjectOld.(*batchv1.Job)
				newJob, ok2 := e.ObjectNew.(*batchv1.Job)
				if !ok1 || !ok2 {
					return false
				}
				if oldJob.Labels["app"] != util.AnalyzerJobLabel ||
					newJob.Labels["app"] != util.AnalyzerJobLabel {
					return false
				}

				oldComplete := isJobComplete(oldJob)
				newComplete := isJobComplete(newJob)
				return !oldComplete && newComplete
			},
			CreateFunc: func(e event.CreateEvent) bool {
				return false
			},
			DeleteFunc:  func(e event.DeleteEvent) bool { return false },
			GenericFunc: func(e event.GenericEvent) bool { return false },
		}).
		Complete(r)
}

func isJobComplete(job *batchv1.Job) bool {
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobComplete && cond.Status == "True" {
			return true
		}
	}
	return false
}
