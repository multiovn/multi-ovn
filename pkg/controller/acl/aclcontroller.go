package acl

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
	aclSynced   cache.InformerSynced
	aclLister   listers.ACLLister
	workqueue   workqueue.TypedRateLimitingInterface[cache.ObjectName]
	ovnnbClient *ovn.OVNNBClient
}

func NewController(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
	ovnnbClient *ovn.OVNNBClient) *Controller {

	aclInformer := informerFactory.Multiovn().V1().ACLs()

	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](time.Second, 60*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)})

	controller := &Controller{
		client:      client,
		aclLister:   aclInformer.Lister(),
		aclSynced:   aclInformer.Informer().HasSynced,
		workqueue:   workqueue.NewTypedRateLimitingQueue(ratelimiter),
		ovnnbClient: ovnnbClient,
	}

	aclInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueACL,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueACL(new)
		},
		DeleteFunc: controller.enqueueACL,
	})

	return controller
}

func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting ACL controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.aclSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch workers to process ACL resources
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
	logger.Info("handleSync ACL")
	acl, err := c.aclLister.ACLs(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			//do nothing
			return nil
		}
		return err
	}

	err = c.handleSync(acl)
	if err != nil {
		logger.Error(err, "Failed to sync ACL")
		return err
	}
	return nil
}

func (c *Controller) handleSync(acl *multiovnv1.ACL) error {
	if acl.DeletionTimestamp != nil && controllerutil.ContainsFinalizer(acl, ovn.ResourceFinalizer) {
		return c.handleRemoveACLFromOVN(acl)
	} else if acl.DeletionTimestamp == nil && !controllerutil.ContainsFinalizer(acl, ovn.ResourceFinalizer) {
		return c.handleAddFinalizerToACL(acl)
	}

	err := c.handleCreateOrUpdateOVNACL(acl)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) handleRemoveACLFromOVN(acl *multiovnv1.ACL) error {
	if acl.Status.UUID != "" {
		// Get the ACL from OVN
		ovnACL := &ovnnb.ACL{
			UUID: acl.Status.UUID,
		}

		var ls *ovnnb.LogicalSwitch = nil
		var pg *ovnnb.PortGroup = nil
		var err error = nil
		if acl.Spec.ParentType == "LogicalSwitch" {
			lsName := ovn.GetEntityName(acl.Spec.ParentNamespace, acl.Spec.ParentName)
			ls, err = c.ovnnbClient.GetLogicalSwitch(lsName, true)
			if err != nil {
				return fmt.Errorf("failed to get logical switch from OVN: %v", err)
			}
		} else {
			pgName := ovn.GetEntityName(acl.Spec.ParentNamespace, acl.Spec.ParentName)
			pg, err = c.ovnnbClient.GetPortGroup(pgName, true)
			if err != nil {
				return fmt.Errorf("failed to get port group from OVN: %v", err)
			}
		}

		// Remove the ACL from OVN
		err = c.ovnnbClient.DeleteACL(ovnACL, ls, pg)
		if err != nil {
			return fmt.Errorf("failed to remove ACL from OVN: %v", err)
		}

	}

	// Remove the finalizer
	controllerutil.RemoveFinalizer(acl, ovn.ResourceFinalizer)
	_, err := c.client.MultiovnV1().ACLs(acl.Namespace).Update(context.Background(), acl, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from ACL: %v", err)
	}

	return nil
}

func (c *Controller) handleCreateOrUpdateOVNACL(acl *multiovnv1.ACL) error {
	uuid := acl.Status.UUID
	var existACL *ovnnb.ACL = nil
	var err error = nil
	if uuid != "" {
		existACL, err = c.ovnnbClient.GetACL(uuid, true)
		if err != nil {
			return fmt.Errorf("failed to get ACL from OVN: %v", err)
		}
	}

	if existACL == nil {
		return c.handleCreateOVNACL(acl)
	} else {
		return c.handleUpdateOVNACL(acl, existACL)
	}
}

func (c *Controller) handleCreateOVNACL(acl *multiovnv1.ACL) error {
	// Create a new ACL in OVN
	var severity *ovnnb.ACLSeverity = nil
	if acl.Spec.Severity != nil {
		sev := ovnnb.ACLSeverity(*acl.Spec.Severity)
		severity = &sev
	}

	var ls *ovnnb.LogicalSwitch = nil
	var pg *ovnnb.PortGroup = nil
	var err error = nil
	if acl.Spec.ParentType == "LogicalSwitch" {
		lsName := ovn.GetEntityName(acl.Spec.ParentNamespace, acl.Spec.ParentName)
		ls, err = c.ovnnbClient.GetLogicalSwitch(lsName, false)
		if err != nil {
			return fmt.Errorf("failed to get logical switch from OVN: %v", err)
		}

	} else {
		pgName := ovn.GetEntityName(acl.Spec.ParentNamespace, acl.Spec.ParentName)
		pg, err = c.ovnnbClient.GetPortGroup(pgName, false)
		if err != nil {
			return fmt.Errorf("failed to get port group from OVN: %v", err)
		}
	}

	ovnACL := &ovnnb.ACL{
		Action:      ovnnb.ACLAction(acl.Spec.Action),
		Direction:   ovnnb.ACLDirection(acl.Spec.Direction),
		ExternalIDs: acl.Spec.ExternalIDs,
		Label:       acl.Spec.Label,
		Log:         acl.Spec.Log,
		Match:       acl.Spec.Match,
		Meter:       acl.Spec.Meter,
		Name:        acl.Spec.Name,
		Options:     acl.Spec.Options,
		Priority:    acl.Spec.Priority,
		Severity:    severity,
	}

	// Create the ACL in OVN
	uuid, err := c.ovnnbClient.CreateACL(ovnACL, ls, pg)
	if err != nil {
		return fmt.Errorf("failed to create ACL in OVN: %v", err)
	}

	// Update status
	acl.Status.UUID = uuid
	if ls != nil {
		acl.Status.ParentUUID = ls.UUID
	} else {
		acl.Status.ParentUUID = pg.UUID
	}

	_, err = c.client.MultiovnV1().ACLs(acl.Namespace).UpdateStatus(context.Background(), acl, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update ACL status: %v", err)
	}

	return nil
}

func (c *Controller) handleAddFinalizerToACL(acl *multiovnv1.ACL) error {
	controllerutil.AddFinalizer(acl, ovn.ResourceFinalizer)
	_, err := c.client.MultiovnV1().ACLs(acl.Namespace).Update(context.Background(), acl, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to add finalizer to ACL: %v", err)
	}
	return nil
}

func (c *Controller) handleUpdateOVNACL(acl *multiovnv1.ACL, existACL *ovnnb.ACL) error {
	// Check if the ACL needs to be updated
	var severity *ovnnb.ACLSeverity = nil
	if acl.Spec.Severity != nil {
		sev := ovnnb.ACLSeverity(*acl.Spec.Severity)
		severity = &sev
	}

	ovnACL := &ovnnb.ACL{
		UUID:        existACL.UUID,
		Action:      ovnnb.ACLAction(acl.Spec.Action),
		Direction:   ovnnb.ACLDirection(acl.Spec.Direction),
		ExternalIDs: acl.Spec.ExternalIDs,
		Label:       acl.Spec.Label,
		Log:         acl.Spec.Log,
		Match:       acl.Spec.Match,
		Meter:       acl.Spec.Meter,
		Name:        acl.Spec.Name,
		Options:     acl.Spec.Options,
		Priority:    acl.Spec.Priority,
		Severity:    severity,
	}

	if checkACLEqual(ovnACL, existACL) {
		// No update needed
		return nil
	}

	// Update ACL
	err := c.ovnnbClient.UpdateACL(ovnACL)
	if err != nil {
		return fmt.Errorf("failed to update ACL in OVN: %v", err)
	}

	return nil
}

func checkACLEqual(acl1 *ovnnb.ACL, acl2 *ovnnb.ACL) bool {
	if acl1.Action != acl2.Action {
		return false
	}
	if acl1.Direction != acl2.Direction {
		return false
	}
	if !reflect.DeepEqual(acl1.ExternalIDs, acl2.ExternalIDs) {
		return false
	}
	if acl1.Label != acl2.Label {
		return false
	}
	if acl1.Log != acl2.Log {
		return false
	}
	if acl1.Match != acl2.Match {
		return false
	}
	if !reflect.DeepEqual(acl1.Meter, acl2.Meter) {
		return false
	}
	if !reflect.DeepEqual(acl1.Name, acl2.Name) {
		return false
	}
	if !reflect.DeepEqual(acl1.Options, acl2.Options) {
		return false
	}
	if acl1.Priority != acl2.Priority {
		return false
	}
	if !reflect.DeepEqual(acl1.Severity, acl2.Severity) {
		return false
	}
	return true
}

func (c *Controller) enqueueACL(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
	} else {
		c.workqueue.Add(objectRef)
	}
}
