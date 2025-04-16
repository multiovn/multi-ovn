package pod

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"golang.org/x/time/rate"

	nadinformers "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/informers/externalversions"
	multiovnv1 "github.com/multiovn/multi-ovn/pkg/apis/multiovn/v1"
	multiovnclient "github.com/multiovn/multi-ovn/pkg/client/clientset/versioned"
	multiovninformers "github.com/multiovn/multi-ovn/pkg/client/informers/externalversions"
	multiovnlisters "github.com/multiovn/multi-ovn/pkg/client/listers/multiovn/v1"
	"github.com/multiovn/multi-ovn/pkg/daemon"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	k8sinformers "k8s.io/client-go/informers"
	k8sclientset "k8s.io/client-go/kubernetes"
	corelisterv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const maxRetries = 10

const (
	MultusDefaultNetwork = "v1.multus-cni.io/default-network"
	MultusAttachNetwork  = "k8s.v1.cni.cncf.io/networks"
)

type MultusNetwork struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type Controller struct {
	k8sclient k8sclientset.Interface
	podSynced cache.InformerSynced
	podLister corelisterv1.PodLister

	multiovnClient multiovnclient.Interface
	udnLister      multiovnlisters.UserDefinedNetworkLister
	udnSynced      cache.InformerSynced
	portLister     multiovnlisters.PortLister
	portSynced     cache.InformerSynced
	workqueue      workqueue.TypedRateLimitingInterface[cache.ObjectName]
}

func NewController(
	k8sclient k8sclientset.Interface,
	multiovnClient multiovnclient.Interface,
	informerFactory k8sinformers.SharedInformerFactory,
	nadInformerFactory nadinformers.SharedInformerFactory,
	multiovnInformerFactory multiovninformers.SharedInformerFactory,
) *Controller {

	podInformer := informerFactory.Core().V1().Pods()

	portInformer := multiovnInformerFactory.Multiovn().V1().Ports()
	udnInformer := multiovnInformerFactory.Multiovn().V1().UserDefinedNetworks()
	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](time.Second, 60*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)})

	controller := &Controller{
		k8sclient: k8sclient,
		podLister: podInformer.Lister(),
		podSynced: podInformer.Informer().HasSynced,

		multiovnClient: multiovnClient,
		portLister:     portInformer.Lister(),
		portSynced:     portInformer.Informer().HasSynced,
		udnLister:      udnInformer.Lister(),
		udnSynced:      udnInformer.Informer().HasSynced,

		workqueue: workqueue.NewTypedRateLimitingQueue(ratelimiter),
	}

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueuePod,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueuePod(new)
		},
		DeleteFunc: controller.enqueuePod,
	})

	return controller
}

func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting pod controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(ctx.Done(), c.podSynced, c.portSynced, c.udnSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting pod controller workers", "count", workers)
	// Launch workers to process Pod resources
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

func convertMultusNetwork(s string) ([]MultusNetwork, error) {
	networks := []MultusNetwork{}
	if s == "" {
		return networks, nil
	}

	err := json.Unmarshal([]byte(s), &networks)
	if err != nil {
		return nil, fmt.Errorf("failed to parse network JSON array: %v", err)
	}

	return networks, nil
}

func (c *Controller) syncHandler(ctx context.Context, objectRef cache.ObjectName) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "objectRef", objectRef)
	logger.Info("handleSync pod")

	pod, err := c.podLister.Pods(objectRef.Namespace).Get(objectRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return c.handleDelete(objectRef.Namespace, objectRef.Name)
		}
		return err
	}

	err = c.handleSync(pod)
	if err != nil {
		logger.Error(err, "Failed to sync pod")
		return err
	}
	return nil
}

func (c *Controller) handleSync(pod *corev1.Pod) error {

	multusDefaultNetwork, defaultNetworkOK := pod.Annotations[MultusDefaultNetwork]
	multusAttachNetwork, attachNetworkOK := pod.Annotations[MultusAttachNetwork]
	if !defaultNetworkOK && !attachNetworkOK {
		return nil
	}

	defaultNetworkInfo := MultusNetwork{
		Name:      "",
		Namespace: "",
	}
	multusNetworks := []MultusNetwork{}
	defaultNetwork, err := convertMultusNetwork(multusDefaultNetwork)
	if err != nil {
		return err
	}
	defaultNetworkInfo = defaultNetwork[0]
	multusNetworks = append(multusNetworks, defaultNetwork...)
	attachNetwork, err := convertMultusNetwork(multusAttachNetwork)
	if err != nil {
		return err
	}
	multusNetworks = append(multusNetworks, attachNetwork...)

	multusOvnNetwork := []MultusNetwork{}
	for _, network := range multusNetworks {
		udn, err := c.udnLister.UserDefinedNetworks(network.Namespace).Get(network.Name)
		if err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("failed to get user defined network: %v", err)
		}

		if udn != nil {
			multusOvnNetwork = append(multusOvnNetwork, network)
		}

	}

	multiovnPodNetworkStr := pod.Annotations[daemon.MultiOVNNetworkAnnotation]
	multiovnPodNetworkList, err := convertMultiOVNPodNetwork(multiovnPodNetworkStr)
	if err != nil {
		return err
	}

	// Check if multiovnPodNetworkList contains networks that don't exist in multusOvnNetwork
	for _, multiovnNet := range multiovnPodNetworkList {
		found := false
		for _, multusNet := range multusOvnNetwork {
			if multiovnNet.Name == multusNet.Name && multiovnNet.Namespace == multusNet.Namespace {
				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("network %s in namespace %s found in multiovn pod network annotation but not in Multus networks",
				multiovnNet.Name, multiovnNet.Namespace)
		}
	}

	// Check each multus network and add to multiovnPodNetworkList if not already present
	for _, multusNet := range multusOvnNetwork {
		found := false
		for _, multiovnNet := range multiovnPodNetworkList {
			if multusNet.Name == multiovnNet.Name && multusNet.Namespace == multiovnNet.Namespace {
				found = true
				break
			}
		}

		if !found {
			// Create a new network entry and add it to multiovnPodNetworkList
			newNetwork := daemon.Network{
				Name:      multusNet.Name,
				Namespace: multusNet.Namespace,
			}

			multiovnPodNetworkList = append(multiovnPodNetworkList, newNetwork)
		}
	}

	err = c.ensurePodNetworkItem(pod, multiovnPodNetworkList, defaultNetworkInfo)
	if err != nil {
		return err
	}

	newMultiovnPodNetworkStr, err := json.Marshal(multiovnPodNetworkList)
	if err != nil {
		return err
	}
	pod.Annotations[daemon.MultiOVNNetworkAnnotation] = string(newMultiovnPodNetworkStr)
	_, err = c.k8sclient.CoreV1().Pods(pod.Namespace).Update(context.Background(), pod, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update pod %s in namespace %s: %v", pod.Name, pod.Namespace, err)
	}

	return nil
}

func (c *Controller) ensurePodNetworkItem(pod *corev1.Pod, multiovnPodNetworkList []daemon.Network, defaultNetworkInfo MultusNetwork) error {

	for i := range multiovnPodNetworkList {
		multiovnNet := multiovnPodNetworkList[i]
		if multiovnNet.Port == "" {
			portName := fmt.Sprintf("%s-%s", pod.Name, multiovnNet.Name)
			multiovnPodNetworkList[i].Port = portName
			multiovnNet.Port = portName

			udn, err := c.udnLister.UserDefinedNetworks(multiovnNet.Namespace).Get(multiovnNet.Name)
			if err != nil {
				return fmt.Errorf("failed to get user defined network: %v", err)
			}

			_, err = c.portLister.Ports(pod.Namespace).Get(multiovnNet.Port)
			if err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("failed to get port %s in namespace %s: %v", multiovnNet.Port, pod.Namespace, err)
			}

			if errors.IsNotFound(err) {
				port := &multiovnv1.Port{
					ObjectMeta: metav1.ObjectMeta{
						Name:      multiovnNet.Port,
						Namespace: pod.Namespace,
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion:         "v1",
								Kind:               "Pod",
								Name:               pod.Name,
								UID:                pod.UID,
								Controller:         func() *bool { b := true; return &b }(),
								BlockOwnerDeletion: func() *bool { b := true; return &b }(),
							},
						},
					},
					Spec: multiovnv1.PortSpec{
						UserDefinedNetworkName:      udn.Name,
						UserDefinedNetworkNamespace: udn.Namespace,
					},
				}

				if defaultNetworkInfo.Name == multiovnNet.Name && defaultNetworkInfo.Namespace == multiovnNet.Namespace {
					port.Spec.Routes = []string{"0.0.0.0/0"}
				}

				_, err = c.multiovnClient.MultiovnV1().Ports(pod.Namespace).Create(context.Background(), port, metav1.CreateOptions{})

				if err != nil {
					return fmt.Errorf("failed to create port %s in namespace %s: %v", multiovnNet.Port, pod.Namespace, err)
				}
			}
		}
	}

	return nil
}

func convertMultiOVNPodNetwork(s string) ([]daemon.Network, error) {
	if s == "" {
		return []daemon.Network{}, nil
	}

	var networks []daemon.Network
	err := json.Unmarshal([]byte(s), &networks)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal MultiOVN pod network annotation: %v", err)
	}

	return networks, nil
}

func (c *Controller) handleDelete(namespace, name string) error {

	//do nothing

	return nil
}

func (c *Controller) enqueuePod(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
		return
	} else {
		c.workqueue.AddRateLimited(objectRef)
	}
}
