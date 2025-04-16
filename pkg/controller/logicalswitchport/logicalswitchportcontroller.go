package logicalswitchport

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
	lspSynced   cache.InformerSynced
	lspLister   listers.LogicalSwitchPortLister
	workqueue   workqueue.TypedRateLimitingInterface[cache.ObjectName]
	ovnnbClient *ovn.OVNNBClient
}

func NewController(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
	ovnnbClient *ovn.OVNNBClient) *Controller {

	lspInformer := informerFactory.Multiovn().V1().LogicalSwitchPorts()

	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](time.Second, 60*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)})

	controller := &Controller{
		client:      client,
		lspLister:   lspInformer.Lister(),
		lspSynced:   lspInformer.Informer().HasSynced,
		workqueue:   workqueue.NewTypedRateLimitingQueue(ratelimiter),
		ovnnbClient: ovnnbClient,
	}

	lspInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueLogicalSwitchPort,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueLogicalSwitchPort(new)
		},
		DeleteFunc: controller.enqueueLogicalSwitchPort,
	})

	return controller
}

func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting logicalswitchport controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.lspSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch workers to process LogicalSwitchPort resources
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
	logger.Info("handleSync logical switch port")
	lsp, err := c.lspLister.LogicalSwitchPorts(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return c.handleDelete(objectRef.Namespace, objectRef.Name)
		}
		return err
	}

	err = c.handleSync(lsp)
	if err != nil {
		logger.Error(err, "Failed to sync logical switch port")
		return err
	}
	return nil
}

func (c *Controller) handleSync(lsp *multiovnv1.LogicalSwitchPort) error {
	if lsp.DeletionTimestamp != nil && controllerutil.ContainsFinalizer(lsp, ovn.ResourceFinalizer) {
		return c.handleRemovePortFromOVNLogicalSwitch(lsp)
	} else if lsp.DeletionTimestamp == nil && !controllerutil.ContainsFinalizer(lsp, ovn.ResourceFinalizer) {
		return c.handleAddFinalizerToLogicalSwitchPort(lsp)
	}

	err := c.handleCreateOrUpdateOVNLogicalSwitchPort(lsp)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) handleRemovePortFromOVNLogicalSwitch(lsp *multiovnv1.LogicalSwitchPort) error {

	lsName := ovn.GetEntityName(lsp.Spec.LogicalSwitchNamespace, lsp.Spec.LogicalSwitchName)
	ls, err := c.ovnnbClient.GetLogicalSwitch(lsName, true)
	if err != nil {
		return fmt.Errorf("failed to get logical switch from OVN: %v", err)
	}

	lspName := ovn.GetEntityName(lsp.Namespace, lsp.Name)

	ovnLsp, err := c.ovnnbClient.GetLogicalSwitchPort(lspName, true)
	if err != nil {
		return fmt.Errorf("failed to get logical switch port from OVN: %v", err)
	}

	if ovnLsp != nil {
		err = c.ovnnbClient.DeleteLogicalSwitchPort(ovnLsp, ls)
		if err != nil {
			return fmt.Errorf("failed to remove port from OVN logical switch: %v", err)
		}
	}

	controllerutil.RemoveFinalizer(lsp, ovn.ResourceFinalizer)
	_, err = c.client.MultiovnV1().LogicalSwitchPorts(lsp.Namespace).Update(context.Background(), lsp, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from logical switch port: %v", err)
	}

	return nil
}

func (c *Controller) handleCreateOrUpdateOVNLogicalSwitchPort(lsp *multiovnv1.LogicalSwitchPort) error {
	lspName := ovn.GetEntityName(lsp.Namespace, lsp.Name)
	existLsp, err := c.ovnnbClient.GetLogicalSwitchPort(lspName, true)
	if err != nil {
		return fmt.Errorf("failed to get logical switch port from OVN: %v", err)
	}

	if existLsp == nil {
		return c.handleCreateOVNLogicalSwitchPort(lsp)
	} else {
		return c.handleUpdateOVNLogicalSwitchPort(lsp, existLsp)
	}
}

func (c *Controller) handleCreateOVNLogicalSwitchPort(lsp *multiovnv1.LogicalSwitchPort) error {
	ovnLSP := &ovnnb.LogicalSwitchPort{
		Name:         ovn.GetEntityName(lsp.Namespace, lsp.Name),
		Addresses:    lsp.Spec.Addresses,
		Enabled:      lsp.Spec.Enabled,
		ExternalIDs:  lsp.Spec.ExternalIDs,
		Options:      lsp.Spec.Options,
		ParentName:   lsp.Spec.ParentName,
		PortSecurity: lsp.Spec.PortSecurity,
		TagRequest:   lsp.Spec.TagRequest,
		Type:         lsp.Spec.Type,
	}

	lsName := ovn.GetEntityName(lsp.Spec.LogicalSwitchNamespace, lsp.Spec.LogicalSwitchName)
	ls, err := c.ovnnbClient.GetLogicalSwitch(lsName, false)
	if err != nil {
		return fmt.Errorf("failed to get logical switch from OVN: %v", err)
	}

	if lsp.Spec.LogicalRouterPortName != "" && lsp.Spec.LogicalRouterPortNamespace != "" {
		lrpName := ovn.GetEntityName(lsp.Spec.LogicalRouterPortNamespace, lsp.Spec.LogicalRouterPortName)
		lrp, err := c.ovnnbClient.GetLogicalRouterPort(lrpName, false)
		if err != nil {
			return fmt.Errorf("failed to get logical router port from OVN: %v", err)
		}

		if ovnLSP.Options == nil {
			ovnLSP.Options = make(map[string]string)
		}
		lrp.Options["router-port"] = lrpName
		lsp.Status.LogicalRouterPortUUID = lrp.UUID
	}

	// create logical switch port
	uuid, err := c.ovnnbClient.CreateLogicalSwitchPort(ovnLSP, ls)
	if err != nil {
		return fmt.Errorf("failed to create logical switch port: %v", err)
	}

	lsp.Status.UUID = uuid
	lsp.Status.LogicalSwitchUUID = ls.UUID
	_, err = c.client.MultiovnV1().LogicalSwitchPorts(lsp.Namespace).UpdateStatus(context.Background(), lsp, metav1.UpdateOptions{})

	if err != nil {
		return fmt.Errorf("failed to update logical switch port status: %v", err)
	}
	// Add finalizer if it doesn't exist
	if !controllerutil.ContainsFinalizer(lsp, ovn.ResourceFinalizer) {
		return c.handleAddFinalizerToLogicalSwitchPort(lsp)

	}

	return nil
}

func (c *Controller) handleAddFinalizerToLogicalSwitchPort(lsp *multiovnv1.LogicalSwitchPort) error {
	controllerutil.AddFinalizer(lsp, ovn.ResourceFinalizer)
	_, err := c.client.MultiovnV1().LogicalSwitchPorts(lsp.Namespace).Update(context.Background(), lsp, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to add finalizer to logical switch port: %v", err)
	}
	return nil
}

func (c *Controller) handleUpdateOVNLogicalSwitchPort(lsp *multiovnv1.LogicalSwitchPort, existLsp *ovnnb.LogicalSwitchPort) error {
	ovnLSP := &ovnnb.LogicalSwitchPort{
		Name:         ovn.GetEntityName(lsp.Namespace, lsp.Name),
		Addresses:    lsp.Spec.Addresses,
		Enabled:      lsp.Spec.Enabled,
		ExternalIDs:  lsp.Spec.ExternalIDs,
		Options:      lsp.Spec.Options,
		ParentName:   lsp.Spec.ParentName,
		PortSecurity: lsp.Spec.PortSecurity,
		TagRequest:   lsp.Spec.TagRequest,
		Type:         lsp.Spec.Type,
	}

	if lsp.Spec.LogicalRouterPortName != "" && lsp.Spec.LogicalRouterPortNamespace != "" {
		lrpName := ovn.GetEntityName(lsp.Spec.LogicalRouterPortNamespace, lsp.Spec.LogicalRouterPortName)
		if ovnLSP.Options == nil {
			ovnLSP.Options = make(map[string]string)
		}
		ovnLSP.Options["router-port"] = lrpName
	}

	if !checkLogicalSwitchPortEqual(ovnLSP, existLsp) {
		if err := c.ovnnbClient.UpdateLogicalSwitchPort(ovnLSP); err != nil {
			return fmt.Errorf("failed to update logical switch port: %v", err)
		}
		return nil
	}
	return nil
}

func checkLogicalSwitchPortEqual(lsp1 *ovnnb.LogicalSwitchPort, lsp2 *ovnnb.LogicalSwitchPort) bool {

	// Compare Addresses
	if !reflect.DeepEqual(lsp1.Addresses, lsp2.Addresses) {
		return false
	}

	// Compare Enabled
	if (lsp1.Enabled == nil && lsp2.Enabled != nil) ||
		(lsp1.Enabled != nil && lsp2.Enabled == nil) ||
		(lsp1.Enabled != nil && lsp2.Enabled != nil && *lsp1.Enabled != *lsp2.Enabled) {
		return false
	}

	// Compare ExternalIDs
	if !reflect.DeepEqual(lsp1.ExternalIDs, lsp2.ExternalIDs) {
		return false
	}

	// Compare Options
	if !reflect.DeepEqual(lsp1.Options, lsp2.Options) {
		return false
	}

	// Compare ParentName
	if (lsp1.ParentName == nil && lsp2.ParentName != nil) ||
		(lsp1.ParentName != nil && lsp2.ParentName == nil) ||
		(lsp1.ParentName != nil && lsp2.ParentName != nil && *lsp1.ParentName != *lsp2.ParentName) {
		return false
	}

	// Compare PortSecurity
	if !reflect.DeepEqual(lsp1.PortSecurity, lsp2.PortSecurity) {
		return false
	}

	// Compare TagRequest
	if (lsp1.TagRequest == nil && lsp2.TagRequest != nil) ||
		(lsp1.TagRequest != nil && lsp2.TagRequest == nil) ||
		(lsp1.TagRequest != nil && lsp2.TagRequest != nil && *lsp1.TagRequest != *lsp2.TagRequest) {
		return false
	}

	// Compare Type
	if lsp1.Type != lsp2.Type {
		return false
	}

	return true

}

func (c *Controller) handleDelete(namespace, name string) error {

	// do nothing
	return nil
}

func (c *Controller) enqueueLogicalSwitchPort(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
		return
	} else {
		c.workqueue.AddRateLimited(objectRef)
	}
}
