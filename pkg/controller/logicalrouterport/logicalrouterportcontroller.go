package logicalrouterport

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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const maxRetries = 10 // Set your desired maximum retry count

type Controller struct {
	client      clientset.Interface
	lrpSynced   cache.InformerSynced
	lrpLister   listers.LogicalRouterPortLister
	workqueue   workqueue.TypedRateLimitingInterface[cache.ObjectName]
	ovnnbClient *ovn.OVNNBClient
}

func NewController(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
	ovnnbClient *ovn.OVNNBClient) *Controller {

	lrpInformer := informerFactory.Multiovn().V1().LogicalRouterPorts()

	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](time.Second, 60*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)})

	controller := &Controller{
		client:      client,
		lrpLister:   lrpInformer.Lister(),
		lrpSynced:   lrpInformer.Informer().HasSynced,
		workqueue:   workqueue.NewTypedRateLimitingQueue(ratelimiter),
		ovnnbClient: ovnnbClient,
	}

	lrpInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueLogicalRouterPort,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueLogicalRouterPort(new)
		},
		DeleteFunc: controller.enqueueLogicalRouterPort,
	})

	return controller
}

func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting logicalrouterport controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.lrpSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch workers to process LogicalRouterPort resources
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
	logger.Info("handleSync logical router port")
	lrp, err := c.lrpLister.LogicalRouterPorts(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return c.handleDelete(objectRef.Namespace, objectRef.Name)
		}
		return err
	}

	err = c.handleSync(lrp)
	if err != nil {
		logger.Error(err, "Failed to sync logical router port")
		return err
	}
	return nil
}

func (c *Controller) handleSync(lrp *multiovnv1.LogicalRouterPort) error {
	if lrp.DeletionTimestamp != nil && controllerutil.ContainsFinalizer(lrp, ovn.ResourceFinalizer) {
		return c.handleRemovePortFromOVNLogicalRouter(lrp)
	} else if lrp.DeletionTimestamp == nil && !controllerutil.ContainsFinalizer(lrp, ovn.ResourceFinalizer) {
		return c.handleAddFinalizerToLogicalRouterPort(lrp)
	}

	err := c.handleCreateOrUpdateOVNLogicalRouterPort(lrp)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) handleRemovePortFromOVNLogicalRouter(lrp *multiovnv1.LogicalRouterPort) error {

	lrName := ovn.GetEntityName(lrp.Spec.LogicalRouterNamespace, lrp.Spec.LogicalRouterName)
	// Get the logical router
	lr, err := c.ovnnbClient.GetLogicalRouter(lrName, true)
	if err != nil {
		return fmt.Errorf("failed to get logical router from OVN: %v", err)
	}

	lrpName := ovn.GetEntityName(lrp.Namespace, lrp.Name)
	ovnLrp, err := c.ovnnbClient.GetLogicalRouterPort(lrpName, true)
	if err != nil {
		return fmt.Errorf("failed to get logical router port from OVN: %v", err)
	}

	if ovnLrp != nil {
		err := c.ovnnbClient.DeleteLogicalRouterPort(ovnLrp, lr)
		if err != nil {
			return fmt.Errorf("failed to remove port from OVN logical router: %v", err)
		}
	}

	controllerutil.RemoveFinalizer(lrp, ovn.ResourceFinalizer)
	_, err = c.client.MultiovnV1().LogicalRouterPorts(lrp.Namespace).Update(context.Background(), lrp, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from logical router port: %v", err)
	}

	return nil
}

func (c *Controller) handleCreateOrUpdateOVNLogicalRouterPort(lrp *multiovnv1.LogicalRouterPort) error {
	lrpName := ovn.GetEntityName(lrp.Namespace, lrp.Name)
	existLrp, err := c.ovnnbClient.GetLogicalRouterPort(lrpName, true)
	if err != nil {
		return fmt.Errorf("failed to get logical router port from OVN: %v", err)
	}

	if existLrp == nil {
		return c.handleCreateOVNLogicalRouterPort(lrp)
	} else {
		return c.handleUpdateOVNLogicalRouterPort(lrp, existLrp)
	}
}

func (c *Controller) handleCreateOVNLogicalRouterPort(lrp *multiovnv1.LogicalRouterPort) error {
	ovnLRP := &ovnnb.LogicalRouterPort{
		Name:           ovn.GetEntityName(lrp.Namespace, lrp.Name),
		Enabled:        lrp.Spec.Enabled,
		ExternalIDs:    lrp.Spec.ExternalIDs,
		Options:        lrp.Spec.Options,
		HaChassisGroup: lrp.Spec.HaChassisGroup,
		Ipv6Prefix:     lrp.Spec.Ipv6Prefix,
		Ipv6RaConfigs:  lrp.Spec.Ipv6RaConfigs,
		MAC:            lrp.Spec.MAC,
		Networks:       lrp.Spec.Networks,
		Peer:           lrp.Spec.Peer,
	}

	lrName := ovn.GetEntityName(lrp.Spec.LogicalRouterNamespace, lrp.Spec.LogicalRouterName)

	// Get the logical router
	lr, err := c.ovnnbClient.GetLogicalRouter(lrName, false)
	if err != nil {
		return fmt.Errorf("failed to get logical router from OVN: %v", err)
	}

	// Create logical router port
	uuid, err := c.ovnnbClient.CreateLogicalRouterPort(ovnLRP, lr)
	if err != nil {
		return fmt.Errorf("failed to create logical router port: %v", err)
	}

	lrp.Status.UUID = uuid
	lrp.Status.LogicalRouterUUID = lr.UUID
	_, err = c.client.MultiovnV1().LogicalRouterPorts(lrp.Namespace).UpdateStatus(context.Background(), lrp, metav1.UpdateOptions{})

	if err != nil {
		return fmt.Errorf("failed to update logical router port status: %v", err)
	}
	// Add finalizer if it doesn't exist
	if !controllerutil.ContainsFinalizer(lrp, ovn.ResourceFinalizer) {
		return c.handleAddFinalizerToLogicalRouterPort(lrp)
	}

	return nil
}

func (c *Controller) handleAddFinalizerToLogicalRouterPort(lrp *multiovnv1.LogicalRouterPort) error {
	controllerutil.AddFinalizer(lrp, ovn.ResourceFinalizer)
	_, err := c.client.MultiovnV1().LogicalRouterPorts(lrp.Namespace).Update(context.Background(), lrp, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to add finalizer to logical router port: %v", err)
	}
	return nil
}

func (c *Controller) handleUpdateOVNLogicalRouterPort(lrp *multiovnv1.LogicalRouterPort, existLrp *ovnnb.LogicalRouterPort) error {
	ovnLRP := &ovnnb.LogicalRouterPort{
		Name:           ovn.GetEntityName(lrp.Namespace, lrp.Name),
		Enabled:        lrp.Spec.Enabled,
		ExternalIDs:    lrp.Spec.ExternalIDs,
		Options:        lrp.Spec.Options,
		HaChassisGroup: lrp.Spec.HaChassisGroup,
		Ipv6Prefix:     lrp.Spec.Ipv6Prefix,
		Ipv6RaConfigs:  lrp.Spec.Ipv6RaConfigs,
		MAC:            lrp.Spec.MAC,
		Networks:       lrp.Spec.Networks,
		Peer:           lrp.Spec.Peer,
	}

	if !checkLogicalRouterPortEqual(ovnLRP, existLrp) {
		if err := c.ovnnbClient.UpdateLogicalRouterPort(ovnLRP); err != nil {
			return fmt.Errorf("failed to update logical router port: %v", err)
		}
		return nil
	}
	return nil
}

func checkLogicalRouterPortEqual(lrp1 *ovnnb.LogicalRouterPort, lrp2 *ovnnb.LogicalRouterPort) bool {
	// Compare Enabled
	if (lrp1.Enabled == nil && lrp2.Enabled != nil) ||
		(lrp1.Enabled != nil && lrp2.Enabled == nil) ||
		(lrp1.Enabled != nil && lrp2.Enabled != nil && *lrp1.Enabled != *lrp2.Enabled) {
		return false
	}

	// Compare ExternalIDs
	if !reflect.DeepEqual(lrp1.ExternalIDs, lrp2.ExternalIDs) {
		return false
	}

	// Compare Options
	if !reflect.DeepEqual(lrp1.Options, lrp2.Options) {
		return false
	}

	// Compare HaChassisGroup
	if (lrp1.HaChassisGroup == nil && lrp2.HaChassisGroup != nil) ||
		(lrp1.HaChassisGroup != nil && lrp2.HaChassisGroup == nil) ||
		(lrp1.HaChassisGroup != nil && lrp2.HaChassisGroup != nil && *lrp1.HaChassisGroup != *lrp2.HaChassisGroup) {
		return false
	}

	// Compare Ipv6Prefix
	if !reflect.DeepEqual(lrp1.Ipv6Prefix, lrp2.Ipv6Prefix) {
		return false
	}

	// Compare Ipv6RaConfigs
	if !reflect.DeepEqual(lrp1.Ipv6RaConfigs, lrp2.Ipv6RaConfigs) {
		return false
	}

	// Compare MAC
	if lrp1.MAC != lrp2.MAC {
		return false
	}

	// Compare Networks
	if !reflect.DeepEqual(lrp1.Networks, lrp2.Networks) {
		return false
	}

	// Compare Peer
	if (lrp1.Peer == nil && lrp2.Peer != nil) ||
		(lrp1.Peer != nil && lrp2.Peer == nil) ||
		(lrp1.Peer != nil && lrp2.Peer != nil && *lrp1.Peer != *lrp2.Peer) {
		return false
	}

	return true
}

func (c *Controller) handleDelete(namespace, name string) error {
	//do nothing
	return nil
}

func (c *Controller) enqueueLogicalRouterPort(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
		return
	} else {
		c.workqueue.AddRateLimited(objectRef)
	}
}
