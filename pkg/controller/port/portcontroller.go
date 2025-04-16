package port

import (
	"context"
	"fmt"
	"net"
	"strconv"
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
	udncontroller "github.com/multiovn/multi-ovn/pkg/controller/userdefinednetwork"
	"github.com/multiovn/multi-ovn/pkg/ovn"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const maxRetries = 10

type Controller struct {
	client                  clientset.Interface
	portSynced              cache.InformerSynced
	portLister              listers.PortLister
	logicalSwitchPortLister listers.LogicalSwitchPortLister
	logicalSwitchPortSynced cache.InformerSynced
	udnLister               listers.UserDefinedNetworkLister
	udnSynced               cache.InformerSynced
	workqueue               workqueue.TypedRateLimitingInterface[cache.ObjectName]
}

func NewController(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory) *Controller {

	portInformer := informerFactory.Multiovn().V1().Ports()
	logicalSwitchPortInformer := informerFactory.Multiovn().V1().LogicalSwitchPorts()
	udnInformer := informerFactory.Multiovn().V1().UserDefinedNetworks()
	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](time.Second, 60*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)})

	controller := &Controller{
		client:                  client,
		portLister:              portInformer.Lister(),
		portSynced:              portInformer.Informer().HasSynced,
		logicalSwitchPortLister: logicalSwitchPortInformer.Lister(),
		logicalSwitchPortSynced: logicalSwitchPortInformer.Informer().HasSynced,
		udnLister:               udnInformer.Lister(),
		udnSynced:               udnInformer.Informer().HasSynced,
		workqueue:               workqueue.NewTypedRateLimitingQueue(ratelimiter),
	}

	portInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueuePort,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueuePort(new)
		},
		DeleteFunc: controller.enqueuePort,
	})

	return controller
}

func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting port controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.portSynced, c.logicalSwitchPortSynced, c.udnSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch workers to process Port resources
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
	logger.Info("handleSync port")
	port, err := c.portLister.Ports(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return c.handleDelete(objectRef.Namespace, objectRef.Name)
		}
		return err
	}

	err = c.handleSync(port)
	if err != nil {
		logger.Error(err, "Failed to sync port")
		return err
	}
	return nil
}

func (c *Controller) handleSync(port *multiovnv1.Port) error {
	if port.DeletionTimestamp != nil && controllerutil.ContainsFinalizer(port, ovn.ResourceFinalizer) {
		return c.handleDeletePort(port)
	} else if port.DeletionTimestamp == nil && !controllerutil.ContainsFinalizer(port, ovn.ResourceFinalizer) {
		return c.handleAddFinalizerToPort(port)
	}

	udn, err := c.udnLister.UserDefinedNetworks(port.Spec.UserDefinedNetworkNamespace).Get(port.Spec.UserDefinedNetworkName)
	if err != nil {
		return fmt.Errorf("failed to get user defined network: %v", err)
	}

	// Get the CIDR from the user defined network
	maskSize, err := getCIDRmask(udn)
	if err != nil {
		return err
	}

	subnetName := udncontroller.IPAMSubnet(port.Spec.UserDefinedNetworkNamespace, port.Spec.UserDefinedNetworkName)

	if port.Status.MacAddress == "" {
		portName := fmt.Sprintf("%s-%s", port.Namespace, port.Name)
		if port.Spec.MacAddress != "" && len(port.Spec.IPAddresses) > 0 {
			_, _, _, err := udncontroller.IPAM.GetStaticAddress(
				portName,
				portName,
				port.Spec.IPAddresses[0],
				&port.Spec.MacAddress,
				subnetName,
				true)
			if err != nil {
				return fmt.Errorf("failed to assign address %v", err)
			}
			ipAddress := port.Spec.IPAddresses[0]
			port.Status.MacAddress = port.Spec.MacAddress
			port.Status.IPAddresses = []string{ipAddress + "/" + strconv.Itoa(maskSize)}
			port.Status.Routes = routerInfo(udn.Spec.Gateway, port.Spec.Routes)
		} else {
			ipAddress, _, macAddress, err := udncontroller.IPAM.GetRandomAddress(
				portName,
				portName,
				nil,
				subnetName,
				"",
				[]string{}, true)
			if err != nil {
				return fmt.Errorf("failed to assign address %v", err)
			}
			port.Status.MacAddress = macAddress
			port.Status.IPAddresses = []string{ipAddress + "/" + strconv.Itoa(maskSize)}

			port.Status.Routes = routerInfo(udn.Spec.Gateway, port.Spec.Routes)
		}

		port, err = c.client.MultiovnV1().Ports(port.Namespace).UpdateStatus(context.Background(), port, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update port status: %v", err)
		}
	}

	lsp, err := c.logicalSwitchPortLister.LogicalSwitchPorts(port.Namespace).Get(port.Name)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to get logical switch port: %v", err)
	}

	if lsp == nil {
		lspAddress := fmt.Sprintf("%s %s", port.Status.MacAddress, port.Status.IPAddresses[0])
		lsp = &multiovnv1.LogicalSwitchPort{
			ObjectMeta: metav1.ObjectMeta{
				Name:      port.Name,
				Namespace: port.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         "multiovn.io/v1",
						Kind:               "Port",
						Name:               port.Name,
						UID:                port.UID,
						Controller:         func() *bool { b := true; return &b }(),
						BlockOwnerDeletion: func() *bool { b := true; return &b }(),
					},
				},
			},
			Spec: multiovnv1.LogicalSwitchPortSpec{
				LogicalSwitchName:      udn.Spec.LogicalSwitchName,
				LogicalSwitchNamespace: udn.Spec.LogicalSwitchNamespace,
				Addresses:              []string{lspAddress},
			},
		}
		_, err = c.client.MultiovnV1().LogicalSwitchPorts(port.Namespace).Create(context.Background(), lsp, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create logical switch port: %v", err)
		}

	}

	return nil
}

func routerInfo(ip string, routes []string) []multiovnv1.Route {
	routesResult := []multiovnv1.Route{}
	for _, route := range routes {
		routesResult = append(routesResult, multiovnv1.Route{Destination: route, NextHop: ip})
	}
	return routesResult
}

func getCIDRmask(udn *multiovnv1.UserDefinedNetwork) (int, error) {
	cidr := udn.Spec.CIDR
	// Extract the mask from CIDR
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse CIDR %s: %v", cidr, err)
	}
	// Get the mask length in bits
	maskSize, _ := ipNet.Mask.Size()
	return maskSize, nil
}

func (c *Controller) handleDeletePort(port *multiovnv1.Port) error {
	subnetName := udncontroller.IPAMSubnet(port.Spec.UserDefinedNetworkNamespace, port.Spec.UserDefinedNetworkName)
	portName := fmt.Sprintf("%s-%s", port.Namespace, port.Name)
	udncontroller.IPAM.ReleaseAddressByPod(portName, subnetName)

	// Remove finalizer
	controllerutil.RemoveFinalizer(port, ovn.ResourceFinalizer)
	_, err := c.client.MultiovnV1().Ports(port.Namespace).Update(context.Background(), port, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from port: %v", err)
	}

	return nil
}

func (c *Controller) handleAddFinalizerToPort(port *multiovnv1.Port) error {
	controllerutil.AddFinalizer(port, ovn.ResourceFinalizer)
	_, err := c.client.MultiovnV1().Ports(port.Namespace).Update(context.Background(), port, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to add finalizer to port: %v", err)
	}
	return nil
}

func (c *Controller) handleDelete(namespace, name string) error {
	// do nothing
	return nil
}

func (c *Controller) enqueuePort(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
		return
	} else {
		c.workqueue.AddRateLimited(objectRef)
	}
}

func (c *Controller) Init() error {
	logger := klog.FromContext(context.Background())
	logger.Info("Initializing port controller")
	ports, err := c.client.MultiovnV1().Ports("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list ports: %v", err)
	}

	logger.Info("Found ports:" + strconv.Itoa(len(ports.Items)))

	for _, port := range ports.Items {
		if port.Status.MacAddress != "" && len(port.Status.IPAddresses) > 0 {
			ip, _, err := net.ParseCIDR(port.Status.IPAddresses[0])
			if err != nil {
				return fmt.Errorf("failed to parse CIDR %s: %v", port.Status.IPAddresses[0], err)
			}
			subnetName := udncontroller.IPAMSubnet(port.Spec.UserDefinedNetworkNamespace, port.Spec.UserDefinedNetworkName)
			_, _, _, err = udncontroller.IPAM.GetStaticAddress(
				port.Name,
				port.Name,
				ip.String(),
				&port.Status.MacAddress,
				subnetName,
				true)
			if err != nil {
				return fmt.Errorf("failed to get static address: %v", err)
			}
		}
	}
	return nil
}
