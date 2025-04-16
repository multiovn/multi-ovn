package portgroupport

import (
	"context"
	"fmt"
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
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const maxRetries = 10 // Set your desired maximum retry count

type Controller struct {
	client      clientset.Interface
	pgpSynced   cache.InformerSynced
	pgpLister   listers.PortGroupPortLister
	pgLister    listers.PortGroupLister
	lspLister   listers.LogicalSwitchPortLister
	workqueue   workqueue.TypedRateLimitingInterface[cache.ObjectName]
	ovnnbClient *ovn.OVNNBClient
}

func NewController(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
	ovnnbClient *ovn.OVNNBClient) *Controller {

	pgpInformer := informerFactory.Multiovn().V1().PortGroupPorts()
	pgInformer := informerFactory.Multiovn().V1().PortGroups()
	lspInformer := informerFactory.Multiovn().V1().LogicalSwitchPorts()

	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](time.Second, 60*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)})

	controller := &Controller{
		client:      client,
		pgpLister:   pgpInformer.Lister(),
		pgpSynced:   pgpInformer.Informer().HasSynced,
		pgLister:    pgInformer.Lister(),
		lspLister:   lspInformer.Lister(),
		workqueue:   workqueue.NewTypedRateLimitingQueue(ratelimiter),
		ovnnbClient: ovnnbClient,
	}

	pgpInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueuePortGroupPort,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueuePortGroupPort(new)
		},
		DeleteFunc: controller.enqueuePortGroupPort,
	})

	return controller
}

func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting portgroupport controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.pgpSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch workers to process PortGroupPort resources
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
	logger.Info("handleSync port group port")
	pgp, err := c.pgpLister.PortGroupPorts(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			//Do nothing
			return nil
		}
		return err
	}

	err = c.handleSync(pgp)
	if err != nil {
		logger.Error(err, "Failed to sync port group port")
		return err
	}
	return nil
}

func (c *Controller) handleSync(pgp *multiovnv1.PortGroupPort) error {
	if pgp.DeletionTimestamp != nil && controllerutil.ContainsFinalizer(pgp, ovn.ResourceFinalizer) {
		return c.handleDeleteBeforeFinalizer(pgp)
	} else if pgp.DeletionTimestamp == nil && !controllerutil.ContainsFinalizer(pgp, ovn.ResourceFinalizer) {
		return c.handleAddFinalizerToPortGroupPort(pgp)
	}

	err := c.handleCreateOrUpdateOVNPortGroupPort(pgp)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) handleCreateOrUpdateOVNPortGroupPort(pgp *multiovnv1.PortGroupPort) error {

	pgName := ovn.GetEntityName(pgp.Spec.PortGroupNamespace, pgp.Spec.PortGroupName)
	ovnPG, err := c.ovnnbClient.GetPortGroup(pgName, false)
	if err != nil {
		return fmt.Errorf("failed to get port group from OVN: %v", err)
	}

	// Get the OVN LogicalSwitchPort
	lspName := ovn.GetEntityName(pgp.Spec.LogicalSwitchPortNamespace, pgp.Spec.LogicalSwitchPortName)
	ovnLSP, err := c.ovnnbClient.GetLogicalSwitchPort(lspName, false)
	if err != nil {
		return fmt.Errorf("failed to get logical switch port from OVN: %v", err)
	}

	// Add the LogicalSwitchPort to the PortGroup
	err = c.ovnnbClient.AddPortToPortGroup(ovnPG, ovnLSP.UUID)
	if err != nil {
		return err
	}

	// Update the Status of the PortGroupPort
	return c.updatePortGroupPortStatus(pgp, ovnPG.UUID, ovnLSP.UUID)
}

func (c *Controller) updatePortGroupPortStatus(pgp *multiovnv1.PortGroupPort, pgUUID, lspUUID string) error {
	// Update the status with the UUIDs
	pgp.Status.PortGroupUUID = pgUUID
	pgp.Status.LogicalSwitchPortUUID = lspUUID

	// Update the status in the API server
	_, err := c.client.MultiovnV1().PortGroupPorts(pgp.Namespace).UpdateStatus(context.Background(), pgp, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update port group port status: %v", err)
	}

	return nil
}

func (c *Controller) handleAddFinalizerToPortGroupPort(pgp *multiovnv1.PortGroupPort) error {
	controllerutil.AddFinalizer(pgp, ovn.ResourceFinalizer)
	_, err := c.client.MultiovnV1().PortGroupPorts(pgp.Namespace).Update(context.Background(), pgp, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to add finalizer to logical switch port: %v", err)
	}
	return nil
}

func (c *Controller) handleDeleteBeforeFinalizer(pgp *multiovnv1.PortGroupPort) error {

	pgName := ovn.GetEntityName(pgp.Spec.PortGroupNamespace, pgp.Spec.PortGroupName)
	ovnPG, err := c.ovnnbClient.GetPortGroup(pgName, false)
	if err != nil {
		return fmt.Errorf("failed to get port group from OVN: %v", err)
	}

	if ovnPG == nil {
		return nil
	}

	err = c.ovnnbClient.DeletePortToPortGroup(ovnPG, pgp.Status.LogicalSwitchPortUUID)
	if err != nil {
		return fmt.Errorf("failed to get logical switch port from OVN: %v", err)
	}

	controllerutil.RemoveFinalizer(pgp, ovn.ResourceFinalizer)
	_, err = c.client.MultiovnV1().PortGroupPorts(pgp.Namespace).Update(context.Background(), pgp, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from port group port: %v", err)
	}

	return nil
}

func (c *Controller) enqueuePortGroupPort(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
	} else {
		c.workqueue.Add(objectRef)
	}
}
