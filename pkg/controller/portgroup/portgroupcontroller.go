package portgroup

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
	pgSynced    cache.InformerSynced
	pgLister    listers.PortGroupLister
	workqueue   workqueue.TypedRateLimitingInterface[cache.ObjectName]
	ovnnbClient *ovn.OVNNBClient
}

func NewController(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
	ovnnbClient *ovn.OVNNBClient) *Controller {

	pgInformer := informerFactory.Multiovn().V1().PortGroups()

	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](time.Second, 60*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)})

	controller := &Controller{
		client:      client,
		pgLister:    pgInformer.Lister(),
		pgSynced:    pgInformer.Informer().HasSynced,
		workqueue:   workqueue.NewTypedRateLimitingQueue(ratelimiter),
		ovnnbClient: ovnnbClient,
	}

	pgInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueuePortGroup,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueuePortGroup(new)
		},
		DeleteFunc: controller.enqueuePortGroup,
	})

	return controller
}

func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting portgroup controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.pgSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch workers to process PortGroup resources
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
	logger.Info("handleSync port group")
	pg, err := c.pgLister.PortGroups(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return c.handleDelete(objectRef.Namespace, objectRef.Name)
		}
		return err
	}

	err = c.handleSync(pg)
	if err != nil {
		logger.Error(err, "Failed to sync port group")
		return err
	}
	return nil
}

func (c *Controller) handleSync(pg *multiovnv1.PortGroup) error {
	err := c.handleCreateOrUpdateOVNPortGroup(pg)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) handleCreateOrUpdateOVNPortGroup(pg *multiovnv1.PortGroup) error {
	pgName := ovn.GetEntityName(pg.Namespace, pg.Name)
	existPg, err := c.ovnnbClient.GetPortGroup(pgName, true)
	if err != nil {
		return fmt.Errorf("failed to get port group from OVN: %v", err)
	}

	if existPg == nil {
		return c.handleCreateOVNPortGroup(pg)
	} else {
		return c.handleUpdateOVNPortGroup(pg, existPg)
	}
}

func (c *Controller) handleCreateOVNPortGroup(pg *multiovnv1.PortGroup) error {
	ovnPG := &ovnnb.PortGroup{
		Name:        ovn.GetEntityName(pg.Namespace, pg.Name),
		ExternalIDs: pg.Spec.ExternalIDs,
		Ports:       []string{}, // Initialize with empty ports
		ACLs:        []string{}, // Initialize with empty ACLs
	}
	// Create port group
	uuid, err := c.ovnnbClient.CreatePortGroup(ovnPG)
	if err != nil {
		return fmt.Errorf("failed to create port group: %v", err)
	}
	pg.Status.UUID = uuid
	_, err = c.client.MultiovnV1().PortGroups(pg.Namespace).UpdateStatus(context.Background(), pg, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update port group status: %v", err)
	}
	return nil
}

func (c *Controller) handleUpdateOVNPortGroup(pg *multiovnv1.PortGroup, existPg *ovnnb.PortGroup) error {
	ovnPG := &ovnnb.PortGroup{
		Name:        ovn.GetEntityName(pg.Namespace, pg.Name),
		ExternalIDs: pg.Spec.ExternalIDs,
	}

	if !checkPortGroupEqual(ovnPG, existPg) {
		if err := c.ovnnbClient.UpdatePortGroup(ovnPG); err != nil {
			return fmt.Errorf("failed to update port group: %v", err)
		}
		return nil
	}
	return nil
}

func checkPortGroupEqual(pg1 *ovnnb.PortGroup, pg2 *ovnnb.PortGroup) bool {
	// Check if two PortGroup objects are equal

	// Compare ExternalIDs
	if !reflect.DeepEqual(pg1.ExternalIDs, pg2.ExternalIDs) {
		return false
	}

	return true
}

func (c *Controller) handleDelete(namespace, name string) error {
	// Delete port group from OVN
	if err := c.ovnnbClient.DeletePortGroup(ovn.GetEntityName(namespace, name)); err != nil {
		return fmt.Errorf("failed to delete port group from OVN: %v", err)
	}
	return nil
}

func (c *Controller) enqueuePortGroup(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
	} else {
		c.workqueue.Add(objectRef)
	}
}
