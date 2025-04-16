package hachassis

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
	client        clientset.Interface
	hcSynced      cache.InformerSynced
	hcLister      listers.HAChassisLister
	workqueue     workqueue.TypedRateLimitingInterface[cache.ObjectName]
	ovnnbClient   *ovn.OVNNBClient
	chassisLister listers.ChassisLister
	chassisSynced cache.InformerSynced
	hcgLister     listers.HAChassisGroupLister
	hcgSynced     cache.InformerSynced
}

func NewController(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
	ovnnbClient *ovn.OVNNBClient) *Controller {

	hcInformer := informerFactory.Multiovn().V1().HAChassises()
	chassisInformer := informerFactory.Multiovn().V1().Chassises()
	hcgInformer := informerFactory.Multiovn().V1().HAChassisGroups()

	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](time.Second, 60*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)})

	controller := &Controller{
		client:        client,
		hcLister:      hcInformer.Lister(),
		hcSynced:      hcInformer.Informer().HasSynced,
		workqueue:     workqueue.NewTypedRateLimitingQueue(ratelimiter),
		ovnnbClient:   ovnnbClient,
		chassisLister: chassisInformer.Lister(),
		chassisSynced: chassisInformer.Informer().HasSynced,
		hcgLister:     hcgInformer.Lister(),
		hcgSynced:     hcgInformer.Informer().HasSynced,
	}

	hcInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueHAChassis,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueHAChassis(new)
		},
		DeleteFunc: controller.enqueueHAChassis,
	})

	return controller
}

func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting HAChassis controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.hcSynced, c.chassisSynced, c.hcgSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch workers to process HAChassis resources
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
	logger.Info("handleSync HAChassis")
	hc, err := c.hcLister.HAChassises(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			// Handle deletion
			return c.handleDelete(objectRef.Namespace, objectRef.Name)
		}
		return err
	}

	err = c.handleSync(hc)
	if err != nil {
		logger.Error(err, "Failed to sync HAChassis")
		return err
	}
	return nil
}

func (c *Controller) handleSync(hc *multiovnv1.HAChassis) error {
	if hc.DeletionTimestamp != nil && controllerutil.ContainsFinalizer(hc, ovn.ResourceFinalizer) {
		return c.handleRemoveHAChassisFromOVN(hc)
	} else if hc.DeletionTimestamp == nil && !controllerutil.ContainsFinalizer(hc, ovn.ResourceFinalizer) {
		return c.handleAddFinalizerToHAChassis(hc)
	}

	return c.handleCreateOrUpdateOVNHAChassis(hc)
}

func (c *Controller) handleRemoveHAChassisFromOVN(hc *multiovnv1.HAChassis) error {

	if hc.Status.UUID != "" {
		hcgName := ovn.GetEntityName(hc.Spec.HAChassisGroupNamespace, hc.Spec.HAChassisGroupName)
		hcg, err := c.ovnnbClient.GetHAChassisGroup(hcgName, true)
		if err != nil {
			return fmt.Errorf("failed to get HAChassisGroup: %v", err)
		}

		ovnHc := &ovnnb.HAChassis{
			UUID: hc.Status.UUID,
		}

		// Delete the HAChassis from OVN
		if err := c.ovnnbClient.DeleteHAChassis(ovnHc, hcg); err != nil {
			return fmt.Errorf("failed to remove HAChassis from OVN: %v", err)
		}
	}

	// Remove the finalizer
	controllerutil.RemoveFinalizer(hc, ovn.ResourceFinalizer)
	_, err := c.client.MultiovnV1().HAChassises(hc.Namespace).Update(context.Background(), hc, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from HAChassis: %v", err)
	}

	return nil
}

func (c *Controller) handleAddFinalizerToHAChassis(hc *multiovnv1.HAChassis) error {
	// Add the finalizer
	controllerutil.AddFinalizer(hc, ovn.ResourceFinalizer)
	_, err := c.client.MultiovnV1().HAChassises(hc.Namespace).Update(context.Background(), hc, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to add finalizer to HAChassis: %v", err)
	}
	return nil
}

func (c *Controller) handleCreateOrUpdateOVNHAChassis(hc *multiovnv1.HAChassis) error {
	uuid := hc.Status.UUID
	var existHC *ovnnb.HAChassis = nil
	var err error = nil
	if uuid != "" {
		existHC, err = c.ovnnbClient.GetHAChassis(uuid, true)
		if err != nil {
			return fmt.Errorf("failed to get HAChassis from OVN: %v", err)
		}
	}

	if existHC == nil {
		return c.handleCreateOVNHAChassis(hc)
	} else {
		return c.handleUpdateOVNHAChassis(hc, existHC)
	}
}

func (c *Controller) handleCreateOVNHAChassis(hc *multiovnv1.HAChassis) error {

	hcgName := ovn.GetEntityName(hc.Spec.HAChassisGroupNamespace, hc.Spec.HAChassisGroupName)
	hcg, err := c.ovnnbClient.GetHAChassisGroup(hcgName, false)
	if err != nil {
		return fmt.Errorf("failed to get HAChassisGroup: %v", err)
	}

	// Get the Chassis
	chassis, err := c.chassisLister.Get(hc.Spec.ChassisName)
	if err != nil {
		return fmt.Errorf("failed to get Chassis: %v", err)
	}

	// Create a new HAChassis in OVN
	ovnHC := &ovnnb.HAChassis{
		ChassisName: chassis.Name,
		Priority:    hc.Spec.Priority,
		ExternalIDs: hc.Spec.ExternalIDs,
	}

	// Create the HAChassis in OVN
	uuid, err := c.ovnnbClient.CreateHAChassis(ovnHC, hcg)
	if err != nil {
		return fmt.Errorf("failed to create HAChassis in OVN: %v", err)
	}

	// Update the status with the UUID
	hc.Status.UUID = uuid
	hc.Status.HAChassisGroupUUID = hcg.UUID
	_, err = c.client.MultiovnV1().HAChassises(hc.Namespace).UpdateStatus(context.Background(), hc, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update HAChassis status: %v", err)
	}

	return nil
}

func (c *Controller) handleUpdateOVNHAChassis(hc *multiovnv1.HAChassis, existHC *ovnnb.HAChassis) error {

	// Create updated HAChassis object
	updatedHC := &ovnnb.HAChassis{
		UUID:        existHC.UUID,
		Priority:    hc.Spec.Priority,
		ExternalIDs: hc.Spec.ExternalIDs,
	}

	// Check if an update is needed
	if !checkHAChassisEqual(updatedHC, existHC) {
		if err := c.ovnnbClient.UpdateHAChassis(updatedHC); err != nil {
			return fmt.Errorf("failed to update HAChassis in OVN: %v", err)
		}
	}

	return nil
}

func (c *Controller) handleDelete(namespace, name string) error {
	//do nothing

	return nil
}

func (c *Controller) enqueueHAChassis(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
	} else {
		c.workqueue.Add(objectRef)
	}
}

func checkHAChassisEqual(hc1 *ovnnb.HAChassis, hc2 *ovnnb.HAChassis) bool {

	if hc1.Priority != hc2.Priority {
		return false
	}

	// Compare ExternalIDs
	if !reflect.DeepEqual(hc1.ExternalIDs, hc2.ExternalIDs) {
		return false
	}

	return true
}
