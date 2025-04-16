package hachassissgroup

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
	hcgSynced   cache.InformerSynced
	hcgLister   listers.HAChassisGroupLister
	workqueue   workqueue.TypedRateLimitingInterface[cache.ObjectName]
	ovnnbClient *ovn.OVNNBClient
}

func NewController(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
	ovnnbClient *ovn.OVNNBClient) *Controller {

	hcgInformer := informerFactory.Multiovn().V1().HAChassisGroups()

	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](time.Second, 60*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)})

	controller := &Controller{
		client:      client,
		hcgLister:   hcgInformer.Lister(),
		hcgSynced:   hcgInformer.Informer().HasSynced,
		workqueue:   workqueue.NewTypedRateLimitingQueue(ratelimiter),
		ovnnbClient: ovnnbClient,
	}

	hcgInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueHAChassisGroup,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueHAChassisGroup(new)
		},
		DeleteFunc: controller.enqueueHAChassisGroup,
	})

	return controller
}

func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting hachassissgroup controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(ctx.Done(), c.hcgSynced) {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch workers to process HAChassisGroup resources
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
	logger.Info("handleSync HA chassis group")
	hcg, err := c.hcgLister.HAChassisGroups(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return c.handleDelete(objectRef.Namespace, objectRef.Name)
		}
		return err
	}

	err = c.handleSync(hcg)
	if err != nil {
		logger.Error(err, "Failed to sync HA chassis group")
		return err
	}
	return nil
}

func (c *Controller) handleSync(hcg *multiovnv1.HAChassisGroup) error {
	err := c.handleCreateOrUpdateOVNHAChassisGroup(hcg)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) handleCreateOrUpdateOVNHAChassisGroup(hcg *multiovnv1.HAChassisGroup) error {
	hcgName := ovn.GetEntityName(hcg.Namespace, hcg.Name)
	existHcg, err := c.ovnnbClient.GetHAChassisGroup(hcgName, true)
	if err != nil {
		return fmt.Errorf("failed to get HA chassis group from OVN: %v", err)
	}

	if existHcg == nil {
		return c.handleCreateOVNHAChassisGroup(hcg)
	} else {
		return c.handleUpdateOVNHAChassisGroup(hcg, existHcg)
	}
}

func (c *Controller) handleCreateOVNHAChassisGroup(hcg *multiovnv1.HAChassisGroup) error {
	ovnHCG := &ovnnb.HAChassisGroup{
		Name:        ovn.GetEntityName(hcg.Namespace, hcg.Name),
		ExternalIDs: hcg.Spec.ExternalIDs,
	}
	// Create HA chassis group
	uuid, err := c.ovnnbClient.CreateHAChassisGroup(ovnHCG)
	if err != nil {
		return fmt.Errorf("failed to create HA chassis group: %v", err)
	}
	hcg.Status.UUID = uuid
	_, err = c.client.MultiovnV1().HAChassisGroups(hcg.Namespace).UpdateStatus(context.Background(), hcg, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update HA chassis group status: %v", err)
	}
	return nil
}

func (c *Controller) handleUpdateOVNHAChassisGroup(hcg *multiovnv1.HAChassisGroup, existHcg *ovnnb.HAChassisGroup) error {
	ovnHCG := &ovnnb.HAChassisGroup{
		Name:        ovn.GetEntityName(hcg.Namespace, hcg.Name),
		ExternalIDs: hcg.Spec.ExternalIDs,
	}

	if !checkHAChassisGroupEqual(ovnHCG, existHcg) {
		if err := c.ovnnbClient.UpdateHAChassisGroup(ovnHCG); err != nil {
			return fmt.Errorf("failed to update HA chassis group: %v", err)
		}
		return nil
	}
	return nil
}

func checkHAChassisGroupEqual(hcg1 *ovnnb.HAChassisGroup, hcg2 *ovnnb.HAChassisGroup) bool {
	// Check if two HAChassisGroup objects are equal
	if hcg1.Name != hcg2.Name {
		return false
	}

	// Compare ExternalIDs
	if !reflect.DeepEqual(hcg1.ExternalIDs, hcg2.ExternalIDs) {
		return false
	}

	return true
}

func (c *Controller) handleDelete(namespace, name string) error {
	hcgName := ovn.GetEntityName(namespace, name)
	err := c.ovnnbClient.DeleteHAChassisGroup(hcgName)
	if err != nil {
		return fmt.Errorf("failed to delete HA chassis group: %v", err)
	}
	return nil
}

func (c *Controller) enqueueHAChassisGroup(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
	} else {
		c.workqueue.Add(objectRef)
	}
}
