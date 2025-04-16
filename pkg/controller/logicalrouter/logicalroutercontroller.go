package logicalrouter

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"golang.org/x/time/rate"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	multiovnv1 "github.com/multiovn/multi-ovn/pkg/apis/multiovn/v1"
	clientset "github.com/multiovn/multi-ovn/pkg/client/clientset/versioned"
	informers "github.com/multiovn/multi-ovn/pkg/client/informers/externalversions"
	listers "github.com/multiovn/multi-ovn/pkg/client/listers/multiovn/v1"
	"github.com/multiovn/multi-ovn/pkg/ovn"
	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnnb"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

const maxRetries = 10 // Set your desired maximum retry count

type Controller struct {
	client      clientset.Interface
	lrSynced    cache.InformerSynced
	lrLister    listers.LogicalRouterLister
	workqueue   workqueue.TypedRateLimitingInterface[cache.ObjectName]
	ovnnbClient *ovn.OVNNBClient
}

func NewController(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
	ovnnbClient *ovn.OVNNBClient) *Controller {

	lrInformer := informerFactory.Multiovn().V1().LogicalRouters()

	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](time.Second, 60*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)})

	controller := &Controller{
		client:      client,
		lrLister:    lrInformer.Lister(),
		lrSynced:    lrInformer.Informer().HasSynced,
		workqueue:   workqueue.NewTypedRateLimitingQueue(ratelimiter),
		ovnnbClient: ovnnbClient,
	}

	lrInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueLogicalRouter,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueLogicalRouter(new)
		},
		DeleteFunc: controller.enqueueLogicalRouter,
	})

	return controller
}

func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting logicalrouter controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	logger.Info("Starting workers", "count", workers)
	// Launch workers to process LogicalRouter resources
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	logger.Info("Started workers")
	<-ctx.Done()
	logger.Info("Shutting down workers")

	return nil
}

func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	objRef, shutdown := c.workqueue.Get()
	logger := klog.FromContext(ctx)

	if shutdown {
		return false
	}

	// We call Done at the end of this func so the workqueue knows we have
	// finished processing this item.
	defer c.workqueue.Done(objRef)

	// Run the syncHandler, passing it the structured reference to the object to be synced.
	err := c.syncHandler(ctx, objRef)
	if err == nil {
		// If no error occurs then we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(objRef)
		logger.Info("Successfully synced", "objectName", objRef)
		return true
	}

	// Get the number of times this item has been requeued
	numRequeues := c.workqueue.NumRequeues(objRef)

	// Check if we've exceeded the maximum number of retries
	if numRequeues >= maxRetries {
		logger.Error(err, "Dropping item from queue after max retries",
			"objectName", objRef,
			"maxRetries", maxRetries)
		c.workqueue.Forget(objRef)
		utilruntime.HandleErrorWithContext(ctx, err, "Max retries reached, dropping item", "objectReference", objRef)
		return true
	}

	// there was a failure so be sure to report it.
	logger.Error(err, "Failed synced", "objectName", objRef, "attempt", numRequeues+1, "maxRetries", maxRetries)
	utilruntime.HandleErrorWithContext(ctx, err, "Error syncing; requeuing for later retry", "objectReference", objRef)

	// since we failed, we should requeue the item to work on later.
	c.workqueue.AddRateLimited(objRef)
	return true
}

func (c *Controller) syncHandler(ctx context.Context, objectRef cache.ObjectName) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "objectRef", objectRef)
	logger.Info("handleSync logical router")
	lr, err := c.lrLister.LogicalRouters(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return c.handleDelete(objectRef.Namespace, objectRef.Name)
		}
		return err
	}

	err = c.handleSync(lr)
	if err != nil {
		logger.Error(err, "Failed to sync logical router")
		return err
	}
	return nil
}

func (c *Controller) handleSync(lr *multiovnv1.LogicalRouter) error {
	err := c.handleCreateOrUpdateOVNLogicalRouter(lr)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) handleCreateOrUpdateOVNLogicalRouter(lr *multiovnv1.LogicalRouter) error {
	lrName := ovn.GetEntityName(lr.Namespace, lr.Name)
	existLr, err := c.ovnnbClient.GetLogicalRouter(lrName, true)
	if err != nil {
		return fmt.Errorf("failed to get logical router from OVN: %v", err)
	}

	if existLr == nil {
		return c.handleCreateOVNLogicalRouter(lr)
	} else {
		return c.handleUpdateOVNLogicalRouter(lr, existLr)
	}
}

func (c *Controller) handleCreateOVNLogicalRouter(lr *multiovnv1.LogicalRouter) error {
	ovnLR := &ovnnb.LogicalRouter{
		Name:        ovn.GetEntityName(lr.Namespace, lr.Name),
		Enabled:     lr.Spec.Enabled,
		Options:     lr.Spec.Options,
		ExternalIDs: lr.Spec.ExternalIDs,
	}
	// Create logical router
	uuid, err := c.ovnnbClient.CreateLogicalRouter(ovnLR)
	if err != nil {
		return fmt.Errorf("failed to create logical router: %v", err)
	}
	lr.Status.UUID = uuid
	_, err = c.client.MultiovnV1().LogicalRouters(lr.Namespace).UpdateStatus(context.Background(), lr, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update logical router status: %v", err)
	}
	return nil
}

func (c *Controller) handleUpdateOVNLogicalRouter(lr *multiovnv1.LogicalRouter, existLr *ovnnb.LogicalRouter) error {
	ovnLR := &ovnnb.LogicalRouter{
		Name:        ovn.GetEntityName(lr.Namespace, lr.Name),
		Enabled:     lr.Spec.Enabled,
		Options:     lr.Spec.Options,
		ExternalIDs: lr.Spec.ExternalIDs,
	}

	if !checkLogicalRouterEqual(ovnLR, existLr) {
		if err := c.ovnnbClient.UpdateLogicalRouter(ovnLR); err != nil {
			return fmt.Errorf("failed to update logical router: %v", err)
		}
		return nil
	}
	return nil
}

func checkLogicalRouterEqual(lr1 *ovnnb.LogicalRouter, lr2 *ovnnb.LogicalRouter) bool {
	// Check if two LogicalRouter objects are equal
	// Mainly compare Enabled, Options, and ExternalIDs fields
	if lr1.Name != lr2.Name {
		return false
	}

	// Compare Enabled
	if (lr1.Enabled == nil && lr2.Enabled != nil) ||
		(lr1.Enabled != nil && lr2.Enabled == nil) ||
		(lr1.Enabled != nil && lr2.Enabled != nil && *lr1.Enabled != *lr2.Enabled) {
		return false
	}

	// Compare Options
	if !reflect.DeepEqual(lr1.Options, lr2.Options) {
		return false
	}

	// Compare ExternalIDs
	if !reflect.DeepEqual(lr1.ExternalIDs, lr2.ExternalIDs) {
		return false
	}

	return true
}

func (c *Controller) handleDelete(namespace, name string) error {
	// Delete logical router from OVN
	if err := c.ovnnbClient.DeleteLogicalRouter(ovn.GetEntityName(namespace, name)); err != nil {
		return fmt.Errorf("failed to delete logical router from OVN: %v", err)
	}
	return nil
}

func (c *Controller) enqueueLogicalRouter(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
	} else {
		c.workqueue.Add(objectRef)
	}
}
