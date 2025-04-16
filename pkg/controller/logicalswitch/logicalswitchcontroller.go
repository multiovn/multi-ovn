package logicalswitch

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
	lsSynced    cache.InformerSynced
	lsLister    listers.LogicalSwitchLister
	workqueue   workqueue.TypedRateLimitingInterface[cache.ObjectName]
	ovnnbClient *ovn.OVNNBClient
}

func NewController(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
	ovnnbClient *ovn.OVNNBClient) *Controller {

	lsInformer := informerFactory.Multiovn().V1().LogicalSwitches()

	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](time.Second, 60*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)})

	controller := &Controller{
		client:      client,
		lsLister:    lsInformer.Lister(),
		lsSynced:    lsInformer.Informer().HasSynced,
		workqueue:   workqueue.NewTypedRateLimitingQueue(ratelimiter),
		ovnnbClient: ovnnbClient,
	}

	lsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueLogicalSwitch,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueLogicalSwitch(new)
		},
		DeleteFunc: controller.enqueueLogicalSwitch,
	})

	return controller
}

func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting logicalswitch controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	// if ok := cache.WaitForCacheSync(ctx.Done(), c.vpcproxiesSynced, c.podsSynced); !ok {
	// 	return fmt.Errorf("failed to wait for caches to sync")
	// }

	logger.Info("Starting workers", "count", workers)
	// Launch two workers to process Foo resources
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
	logger.Info("handleSync logical switch")
	ls, err := c.lsLister.LogicalSwitches(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return c.handleDelete(objectRef.Namespace, objectRef.Name)
		}
		return err
	}

	err = c.handleSync(ls)
	if err != nil {
		logger.Error(err, "Failed to sync logical switch")
		return err
	}
	return nil
}

func (c *Controller) handleSync(ls *multiovnv1.LogicalSwitch) error {
	err := c.handleCreateOrUpdateOVNLogicalSwitch(ls)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) handleCreateOrUpdateOVNLogicalSwitch(ls *multiovnv1.LogicalSwitch) error {
	lsName := ovn.GetEntityName(ls.Namespace, ls.Name)
	existLs, err := c.ovnnbClient.GetLogicalSwitch(lsName, true)
	if err != nil {
		return fmt.Errorf("failed to get logical switch from OVN: %v", err)
	}

	if existLs == nil {
		return c.handleCreateOVNLogicalSwitch(ls)
	} else {
		return c.handleUpdateOVNLogicalSwitch(ls, existLs)
	}
}

func (c *Controller) handleCreateOVNLogicalSwitch(ls *multiovnv1.LogicalSwitch) error {
	ovnLS := &ovnnb.LogicalSwitch{
		Name:        ovn.GetEntityName(ls.Namespace, ls.Name),
		OtherConfig: ls.Spec.OtherConfig,
		ExternalIDs: ls.Spec.ExternalIDs,
	}
	// 创建logical switch
	uuid, err := c.ovnnbClient.CreateLogicalSwitch(ovnLS)
	if err != nil {
		return fmt.Errorf("failed to create logical switch: %v", err)
	}
	ls.Status.UUID = uuid
	_, err = c.client.MultiovnV1().LogicalSwitches(ls.Namespace).UpdateStatus(context.Background(), ls, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update logical switch status: %v", err)
	}
	return nil
}

func (c *Controller) handleUpdateOVNLogicalSwitch(ls *multiovnv1.LogicalSwitch, existLs *ovnnb.LogicalSwitch) error {

	ovnLS := &ovnnb.LogicalSwitch{
		Name:        ovn.GetEntityName(ls.Namespace, ls.Name),
		OtherConfig: ls.Spec.OtherConfig,
		ExternalIDs: ls.Spec.ExternalIDs,
	}

	if !checkLogicalSwitchEqual(ovnLS, existLs) {
		if err := c.ovnnbClient.UpdateLogicalSwitch(ovnLS); err != nil {
			return fmt.Errorf("failed to update logical switch: %v", err)
		}
		return nil
	}
	return nil
}

func checkLogicalSwitchEqual(ls1 *ovnnb.LogicalSwitch, ls2 *ovnnb.LogicalSwitch) bool {
	// 检查两个LogicalSwitch对象是否相等
	// 主要比较OtherConfig和ExternalIDs字段
	if ls1.Name != ls2.Name {
		return false
	}

	// 比较OtherConfig
	if !reflect.DeepEqual(ls1.OtherConfig, ls2.OtherConfig) {
		return false
	}

	// 比较ExternalIDs
	if !reflect.DeepEqual(ls1.ExternalIDs, ls2.ExternalIDs) {
		return false
	}

	return true
}

func (c *Controller) handleDelete(namespace, name string) error {
	// 从OVN中删除logical switch
	if err := c.ovnnbClient.DeleteLogicalSwitch(ovn.GetEntityName(namespace, name)); err != nil {
		return fmt.Errorf("failed to delete logical switch from OVN: %v", err)
	}
	return nil
}

func (c *Controller) enqueueLogicalSwitch(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
		return
	} else {
		c.workqueue.AddRateLimited(objectRef)
	}
}
