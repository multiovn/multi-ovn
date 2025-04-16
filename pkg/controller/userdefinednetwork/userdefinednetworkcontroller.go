package userdefinednetwork

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"golang.org/x/time/rate"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	nadv1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	nadclientset "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned"
	nadinformers "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/informers/externalversions"
	nadlisters "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/listers/k8s.cni.cncf.io/v1"
	multiovnv1 "github.com/multiovn/multi-ovn/pkg/apis/multiovn/v1"
	clientset "github.com/multiovn/multi-ovn/pkg/client/clientset/versioned"
	informers "github.com/multiovn/multi-ovn/pkg/client/informers/externalversions"
	listers "github.com/multiovn/multi-ovn/pkg/client/listers/multiovn/v1"
	"github.com/multiovn/multi-ovn/pkg/ipam"
	"github.com/multiovn/multi-ovn/pkg/ovn"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const maxRetries = 10

var IPAM *ipam.IPAM = ipam.NewIPAM()

func IPAMSubnet(namespace, name string) string {
	return fmt.Sprintf("%s-%s", namespace, name)
}

type Controller struct {
	client    clientset.Interface
	udnSynced cache.InformerSynced
	udnLister listers.UserDefinedNetworkLister
	lsLister  listers.LogicalSwitchLister
	lsSynced  cache.InformerSynced
	workqueue workqueue.TypedRateLimitingInterface[cache.ObjectName]

	nadClient nadclientset.Interface
	nadLister nadlisters.NetworkAttachmentDefinitionLister
	nadSynced cache.InformerSynced
}

func NewController(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
	nadClient nadclientset.Interface,
	nadInformerFactory nadinformers.SharedInformerFactory) *Controller {

	udnInformer := informerFactory.Multiovn().V1().UserDefinedNetworks()
	lsInformer := informerFactory.Multiovn().V1().LogicalSwitches()

	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](time.Second, 60*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)})

	controller := &Controller{
		client:    client,
		udnLister: udnInformer.Lister(),
		udnSynced: udnInformer.Informer().HasSynced,
		lsLister:  lsInformer.Lister(),
		lsSynced:  lsInformer.Informer().HasSynced,
		nadClient: nadClient,
		nadLister: nadInformerFactory.K8sCniCncfIo().V1().NetworkAttachmentDefinitions().Lister(),
		nadSynced: nadInformerFactory.K8sCniCncfIo().V1().NetworkAttachmentDefinitions().Informer().HasSynced,
		workqueue: workqueue.NewTypedRateLimitingQueue(ratelimiter),
	}

	udnInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueUserDefinedNetwork,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueUserDefinedNetwork(new)
		},
		DeleteFunc: controller.enqueueUserDefinedNetwork,
	})

	return controller
}

func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting userdefinednetwork controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.udnSynced, c.lsSynced, c.nadSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch workers to process UserDefinedNetwork resources
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
	logger.Info("handleSync user defined network")
	udn, err := c.udnLister.UserDefinedNetworks(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return c.handleDelete(objectRef.Namespace, objectRef.Name)
		}
		return err
	}

	err = c.handleSync(udn)
	if err != nil {
		logger.Error(err, "Failed to sync user defined network")
		return err
	}
	return nil
}

func (c *Controller) handleSync(udn *multiovnv1.UserDefinedNetwork) error {
	if udn.DeletionTimestamp != nil && controllerutil.ContainsFinalizer(udn, ovn.ResourceFinalizer) {
		return c.handleDeleteUserDefinedNetwork(udn)
	} else if udn.DeletionTimestamp == nil && !controllerutil.ContainsFinalizer(udn, ovn.ResourceFinalizer) {
		return c.handleAddFinalizerToUserDefinedNetwork(udn)
	}

	// Check if logical switch exists
	ls, err := c.lsLister.LogicalSwitches(udn.Spec.LogicalSwitchNamespace).Get(udn.Spec.LogicalSwitchName)
	if err != nil {
		if errors.IsNotFound(err) {
			return fmt.Errorf("logical switch %s/%s not found", udn.Spec.LogicalSwitchNamespace, udn.Spec.LogicalSwitchName)
		}
		return fmt.Errorf("failed to get logical switch: %v", err)
	}

	if ls.Status.UUID == "" {
		return fmt.Errorf("logical switch %s/%s is not ready", udn.Spec.LogicalSwitchNamespace, udn.Spec.LogicalSwitchName)
	}

	// Create or update network attachment definition
	err = c.handleCreateOrUpdateNetworkAttachmentDefinition(udn)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) handleCreateOrUpdateNetworkAttachmentDefinition(udn *multiovnv1.UserDefinedNetwork) error {
	// Create CNI configuration
	cniConfig := map[string]interface{}{
		"cniVersion":             "0.3.0",
		"type":                   "multi-ovn",
		"logicalSwitchName":      udn.Spec.LogicalSwitchName,
		"logicalSwitchNamespace": udn.Spec.LogicalSwitchNamespace,
		"serverSocket":           "/run/openvswitch/cniserver.sock",
	}

	cniConfigBytes, err := json.Marshal(cniConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal CNI config: %v", err)
	}

	// Create NetworkAttachmentDefinition
	nad := &nadv1.NetworkAttachmentDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:      udn.Name,
			Namespace: udn.Namespace,
		},
		Spec: nadv1.NetworkAttachmentDefinitionSpec{
			Config: string(cniConfigBytes),
		},
	}

	// Try to get existing NAD
	existingNAD, err := c.nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Get(context.Background(), udn.Name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// Create new NAD
			_, err = c.nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Create(context.Background(), nad, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("failed to create network attachment definition: %v", err)
			}

			return nil
		}
		return fmt.Errorf("failed to get network attachment definition: %v", err)
	}

	ipamKey := IPAMSubnet(udn.Namespace, udn.Name)
	_, ok := IPAM.Subnets[ipamKey]
	if !ok {
		IPAM.AddOrUpdateSubnet(ipamKey, udn.Spec.CIDR, udn.Spec.Gateway, []string{udn.Spec.Gateway})
	}

	// Update existing NAD
	if existingNAD.Spec.Config != string(cniConfigBytes) {
		_, err = c.nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Update(context.Background(), nad, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update network attachment definition: %v", err)
		}

	}

	return nil
}

func (c *Controller) handleDeleteUserDefinedNetwork(udn *multiovnv1.UserDefinedNetwork) error {
	// Delete network attachment definition
	err := c.handleDeleteNetworkAttachmentDefinition(udn)
	if err != nil {
		return err
	}

	ipamKey := IPAMSubnet(udn.Namespace, udn.Name)
	IPAM.DeleteSubnet(ipamKey)

	// Remove finalizer
	controllerutil.RemoveFinalizer(udn, ovn.ResourceFinalizer)
	_, err = c.client.MultiovnV1().UserDefinedNetworks(udn.Namespace).Update(context.Background(), udn, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from user defined network: %v", err)
	}

	return nil
}

func (c *Controller) handleDeleteNetworkAttachmentDefinition(udn *multiovnv1.UserDefinedNetwork) error {
	err := c.nadClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(udn.Namespace).Delete(context.Background(), udn.Name, metav1.DeleteOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete network attachment definition: %v", err)
	}
	return nil
}

func (c *Controller) handleAddFinalizerToUserDefinedNetwork(udn *multiovnv1.UserDefinedNetwork) error {
	controllerutil.AddFinalizer(udn, ovn.ResourceFinalizer)
	_, err := c.client.MultiovnV1().UserDefinedNetworks(udn.Namespace).Update(context.Background(), udn, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to add finalizer to user defined network: %v", err)
	}
	return nil
}

func (c *Controller) handleDelete(namespace, name string) error {
	// do nothing
	return nil
}

func (c *Controller) enqueueUserDefinedNetwork(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
		return
	} else {
		c.workqueue.AddRateLimited(objectRef)
	}
}

func (c *Controller) Init() error {
	udns, err := c.client.MultiovnV1().UserDefinedNetworks("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list user defined networks: %v", err)
	}

	for _, udn := range udns.Items {
		klog.Infof("init user defined network: %s/%s", udn.Namespace, udn.Name)
		ipamKey := IPAMSubnet(udn.Namespace, udn.Name)

		IPAM.AddOrUpdateSubnet(ipamKey, udn.Spec.CIDR, udn.Spec.Gateway, []string{udn.Spec.Gateway})

	}

	return nil
}
