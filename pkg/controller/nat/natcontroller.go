package nat

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
	natSynced   cache.InformerSynced
	natLister   listers.NatLister
	workqueue   workqueue.TypedRateLimitingInterface[cache.ObjectName]
	ovnnbClient *ovn.OVNNBClient
}

func NewController(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
	ovnnbClient *ovn.OVNNBClient) *Controller {

	natInformer := informerFactory.Multiovn().V1().Nats()

	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](time.Second, 60*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)})

	controller := &Controller{
		client:      client,
		natLister:   natInformer.Lister(),
		natSynced:   natInformer.Informer().HasSynced,
		workqueue:   workqueue.NewTypedRateLimitingQueue(ratelimiter),
		ovnnbClient: ovnnbClient,
	}

	natInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueNat,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueNat(new)
		},
		DeleteFunc: controller.enqueueNat,
	})

	return controller
}

func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting nat controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.natSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch workers to process NAT resources
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
	logger.Info("handleSync nat")
	nat, err := c.natLister.Nats(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return c.handleDelete(objectRef.Namespace, objectRef.Name)
		}
		return err
	}

	err = c.handleSync(nat)
	if err != nil {
		logger.Error(err, "Failed to sync nat")
		return err
	}
	return nil
}

func (c *Controller) handleSync(nat *multiovnv1.Nat) error {
	if nat.DeletionTimestamp != nil && controllerutil.ContainsFinalizer(nat, ovn.ResourceFinalizer) {
		return c.handleRemoveNatFromOVNLogicalRouter(nat)
	} else if nat.DeletionTimestamp == nil && !controllerutil.ContainsFinalizer(nat, ovn.ResourceFinalizer) {
		return c.handleAddFinalizerToNat(nat)
	}

	err := c.handleCreateOrUpdateOVNNat(nat)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) handleRemoveNatFromOVNLogicalRouter(nat *multiovnv1.Nat) error {
	if nat.Status.UUID != "" {
		ovnNat := &ovnnb.NAT{
			UUID: nat.Status.UUID,
		}

		lrName := ovn.GetEntityName(nat.Spec.LogicalRouterNamespace, nat.Spec.LogicalRouterName)
		// Get the logical router
		lr, err := c.ovnnbClient.GetLogicalRouter(lrName, true)
		if err != nil {
			return fmt.Errorf("failed to get logical router from OVN: %v", err)
		}

		err = c.ovnnbClient.RemoveNatFromLogicalRouter(ovnNat, lr)
		if err != nil {
			return fmt.Errorf("failed to remove nat from OVN logical router: %v", err)
		}
	}

	controllerutil.RemoveFinalizer(nat, ovn.ResourceFinalizer)
	_, err := c.client.MultiovnV1().Nats(nat.Namespace).Update(context.Background(), nat, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from nat: %v", err)
	}

	return nil
}

func (c *Controller) handleCreateOrUpdateOVNNat(nat *multiovnv1.Nat) error {
	uuid := nat.Status.UUID
	var existNat *ovnnb.NAT = nil
	var err error = nil
	if uuid != "" {
		existNat, err = c.ovnnbClient.GetNat(uuid, true)
		if err != nil {
			return fmt.Errorf("failed to get nat from OVN: %v", err)
		}
	}

	if existNat == nil {
		return c.handleCreateOVNNat(nat)
	} else {
		return c.handleUpdateOVNNat(nat, existNat)
	}
}

func (c *Controller) handleCreateOVNNat(nat *multiovnv1.Nat) error {
	ovnNat := &ovnnb.NAT{
		AllowedExtIPs:     nat.Spec.AllowedExtIPs,
		ExemptedExtIPs:    nat.Spec.ExemptedExtIPs,
		ExternalIDs:       nat.Spec.ExternalIDs,
		ExternalIP:        nat.Spec.ExternalIP,
		ExternalMAC:       nat.Spec.ExternalMAC,
		ExternalPortRange: nat.Spec.ExternalPortRange,
		GatewayPort:       nat.Spec.GatewayPort,
		LogicalIP:         nat.Spec.LogicalIP,
		LogicalPort:       nat.Spec.LogicalPort,
		Options:           nat.Spec.Options,
		Type:              ovnnb.NATType(nat.Spec.Type),
	}

	lrName := ovn.GetEntityName(nat.Spec.LogicalRouterNamespace, nat.Spec.LogicalRouterName)
	lr, err := c.ovnnbClient.GetLogicalRouter(lrName, false)
	if err != nil {
		return fmt.Errorf("failed to get logical router from OVN: %v", err)
	}

	uuid, err := c.ovnnbClient.CreateNat(ovnNat, lr)
	if err != nil {
		return fmt.Errorf("failed to create nat in OVN: %v", err)
	}

	// Update status
	nat.Status.UUID = uuid
	nat.Status.LogicalRouterUUID = lr.UUID

	_, err = c.client.MultiovnV1().Nats(nat.Namespace).UpdateStatus(context.Background(), nat, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update nat status: %v", err)
	}

	return nil
}

func (c *Controller) handleAddFinalizerToNat(nat *multiovnv1.Nat) error {
	controllerutil.AddFinalizer(nat, ovn.ResourceFinalizer)
	_, err := c.client.MultiovnV1().Nats(nat.Namespace).Update(context.Background(), nat, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to add finalizer to nat: %v", err)
	}
	return nil
}

func (c *Controller) handleUpdateOVNNat(nat *multiovnv1.Nat, existNat *ovnnb.NAT) error {
	// Check if the NAT needs to be updated
	ovnNat := &ovnnb.NAT{
		UUID:              existNat.UUID,
		AllowedExtIPs:     nat.Spec.AllowedExtIPs,
		ExemptedExtIPs:    nat.Spec.ExemptedExtIPs,
		ExternalIDs:       nat.Spec.ExternalIDs,
		ExternalIP:        nat.Spec.ExternalIP,
		ExternalMAC:       nat.Spec.ExternalMAC,
		ExternalPortRange: nat.Spec.ExternalPortRange,
		GatewayPort:       nat.Spec.GatewayPort,
		LogicalIP:         nat.Spec.LogicalIP,
		LogicalPort:       nat.Spec.LogicalPort,
		Options:           nat.Spec.Options,
		Type:              ovnnb.NATType(nat.Spec.Type),
	}

	if checkNatEqual(ovnNat, existNat) {
		// No update needed
		return nil
	}

	// Update NAT
	err := c.ovnnbClient.UpdateNat(ovnNat)
	if err != nil {
		return fmt.Errorf("failed to update nat in OVN: %v", err)
	}

	return nil
}

func checkNatEqual(nat1 *ovnnb.NAT, nat2 *ovnnb.NAT) bool {
	if !reflect.DeepEqual(nat1.AllowedExtIPs, nat2.AllowedExtIPs) {
		return false
	}
	if !reflect.DeepEqual(nat1.ExemptedExtIPs, nat2.ExemptedExtIPs) {
		return false
	}
	if !reflect.DeepEqual(nat1.ExternalIDs, nat2.ExternalIDs) {
		return false
	}
	if nat1.ExternalIP != nat2.ExternalIP {
		return false
	}
	if !reflect.DeepEqual(nat1.ExternalMAC, nat2.ExternalMAC) {
		return false
	}
	if nat1.ExternalPortRange != nat2.ExternalPortRange {
		return false
	}
	if !reflect.DeepEqual(nat1.GatewayPort, nat2.GatewayPort) {
		return false
	}
	if nat1.LogicalIP != nat2.LogicalIP {
		return false
	}
	if !reflect.DeepEqual(nat1.LogicalPort, nat2.LogicalPort) {
		return false
	}
	if !reflect.DeepEqual(nat1.Options, nat2.Options) {
		return false
	}
	if nat1.Type != nat2.Type {
		return false
	}
	return true
}

func (c *Controller) handleDelete(namespace, name string) error {
	//do nothing
	return nil
}

func (c *Controller) enqueueNat(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
	} else {
		c.workqueue.Add(objectRef)
	}
}
