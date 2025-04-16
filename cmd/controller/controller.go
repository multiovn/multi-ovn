package main

import (
	"context"

	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	networkv1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned"
	nadinformers "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/informers/externalversions"
	clientset "github.com/multiovn/multi-ovn/pkg/client/clientset/versioned"
	informers "github.com/multiovn/multi-ovn/pkg/client/informers/externalversions"
	"github.com/multiovn/multi-ovn/pkg/controller/acl"
	"github.com/multiovn/multi-ovn/pkg/controller/chassis"
	"github.com/multiovn/multi-ovn/pkg/controller/hachassis"
	"github.com/multiovn/multi-ovn/pkg/controller/hachassissgroup"
	"github.com/multiovn/multi-ovn/pkg/controller/logicalrouter"
	"github.com/multiovn/multi-ovn/pkg/controller/logicalrouterport"
	"github.com/multiovn/multi-ovn/pkg/controller/logicalswitch"
	"github.com/multiovn/multi-ovn/pkg/controller/logicalswitchport"
	"github.com/multiovn/multi-ovn/pkg/controller/nat"
	"github.com/multiovn/multi-ovn/pkg/controller/pod"
	"github.com/multiovn/multi-ovn/pkg/controller/port"
	"github.com/multiovn/multi-ovn/pkg/controller/portgroup"
	"github.com/multiovn/multi-ovn/pkg/controller/portgroupport"
	"github.com/multiovn/multi-ovn/pkg/controller/userdefinednetwork"
	"github.com/multiovn/multi-ovn/pkg/ovn"
	ovsclient "github.com/multiovn/multi-ovn/pkg/ovsdb/client"
	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnnb"
	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnsb"
	"github.com/multiovn/multi-ovn/signals"
	"github.com/ovn-org/libovsdb/client"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

func main() {

	// set up signals so we handle the shutdown signal gracefully
	ctx := signals.SetupSignalHandler()

	ovndbClient, err := NewOVNNBClient("tcp:10.233.59.109:6641")
	ovnnbClient := &ovn.OVNNBClient{
		Client: ovndbClient,
	}
	if err != nil {
		klog.Errorf("failed to create OVN NB client: %v", err)
		return
	}

	ovnsbClient, err := NewOVNSBClient("tcp:10.233.33.164:6642")
	if err != nil {
		klog.Errorf("failed to create OVN SB client: %v", err)
		return
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", "/root/.kube/config")
	if err != nil {
		klog.Errorf("Error building kubeconfig: %v", err)
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	ovnClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		klog.Errorf("Error building ovnclient clientset: %v", err)
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
	k8sClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Errorf("Error building k8sclient clientset: %v", err)
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	k8sInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(k8sClient, 0, kubeinformers.WithTweakListOptions(func(listOption *metav1.ListOptions) {
		listOption.AllowWatchBookmarks = true
	}))

	networkClient, err := networkv1.NewForConfig(cfg)
	if err != nil {
		klog.Errorf("Error building networkclient clientset: %v", err)
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	nadInformerFactory := nadinformers.NewSharedInformerFactoryWithOptions(networkClient, 0, nadinformers.WithTweakListOptions(func(listOption *metav1.ListOptions) {
		listOption.AllowWatchBookmarks = true
	}))

	// 创建informer factory
	informerFactory := informers.NewSharedInformerFactoryWithOptions(ovnClient, 0, informers.WithTweakListOptions(func(listOption *metav1.ListOptions) {
		listOption.AllowWatchBookmarks = true
	}))

	// 创建controller
	podController := pod.NewController(
		k8sClient,
		ovnClient,
		k8sInformerFactory,
		nadInformerFactory,
		informerFactory,
	)

	portController := port.NewController(
		ovnClient,
		informerFactory,
	)

	udnController := userdefinednetwork.NewController(
		ovnClient,
		informerFactory,
		networkClient,
		nadInformerFactory,
	)

	udnController.Init()
	portController.Init()

	lsController := logicalswitch.NewController(
		ovnClient,
		informerFactory,
		ovnnbClient,
	)

	lspController := logicalswitchport.NewController(
		ovnClient,
		informerFactory,
		ovnnbClient,
	)

	lrController := logicalrouter.NewController(
		ovnClient,
		informerFactory,
		ovnnbClient,
	)

	lrpController := logicalrouterport.NewController(
		ovnClient,
		informerFactory,
		ovnnbClient,
	)

	natController := nat.NewController(
		ovnClient,
		informerFactory,
		ovnnbClient,
	)

	pgController := portgroup.NewController(
		ovnClient,
		informerFactory,
		ovnnbClient,
	)

	pgpController := portgroupport.NewController(
		ovnClient,
		informerFactory,
		ovnnbClient,
	)

	aclController := acl.NewController(
		ovnClient,
		informerFactory,
		ovnnbClient,
	)

	hcgController := hachassissgroup.NewController(
		ovnClient,
		informerFactory,
		ovnnbClient,
	)

	hcController := hachassis.NewController(
		ovnClient,
		informerFactory,
		ovnnbClient,
	)

	chassisController := chassis.NewController(
		ovnClient,
		&ovn.OVNSBClient{
			Client: ovnsbClient,
		},
	)

	if err := chassisController.Run(context.Background()); err != nil {
		klog.Fatalf("Error running chassis controller: %s", err.Error())
	}

	// 启动informer
	informerFactory.Start(ctx.Done())
	nadInformerFactory.Start(ctx.Done())
	k8sInformerFactory.Start(ctx.Done())

	go func() {
		if err := lsController.Run(ctx, 1); err != nil {
			klog.Fatalf("Error running controller: %s", err.Error())
		}
	}()

	go func() {
		if err := lspController.Run(ctx, 1); err != nil {
			klog.Fatalf("Error running controller: %s", err.Error())
		}
	}()

	go func() {
		if err := lrController.Run(ctx, 1); err != nil {
			klog.Fatalf("Error running logical router controller: %s", err.Error())
		}
	}()

	go func() {
		if err := lrpController.Run(ctx, 1); err != nil {
			klog.Fatalf("Error running logical router port controller: %s", err.Error())
		}
	}()

	go func() {
		if err := natController.Run(ctx, 1); err != nil {
			klog.Fatalf("Error running nat controller: %s", err.Error())
		}
	}()

	go func() {
		if err := pgController.Run(ctx, 1); err != nil {
			klog.Fatalf("Error running port group controller: %s", err.Error())
		}
	}()

	go func() {
		if err := pgpController.Run(ctx, 1); err != nil {
			klog.Fatalf("Error running port group port controller: %s", err.Error())
		}
	}()

	go func() {
		if err := aclController.Run(ctx, 1); err != nil {
			klog.Fatalf("Error running ACL controller: %s", err.Error())
		}
	}()

	go func() {
		if err := hcgController.Run(ctx, 1); err != nil {
			klog.Fatalf("Error running HA chassis group controller: %s", err.Error())
		}
	}()

	go func() {
		if err := hcController.Run(ctx, 1); err != nil {
			klog.Fatalf("Error running HA chassis controller: %s", err.Error())
		}
	}()

	go func() {
		if err := udnController.Run(ctx, 1); err != nil {
			klog.Fatalf("Error running user defined network controller: %s", err.Error())
		}
	}()

	go func() {
		if err := portController.Run(ctx, 1); err != nil {
			klog.Fatalf("Error running port controller: %s", err.Error())
		}
	}()

	go func() {
		if err := podController.Run(ctx, 1); err != nil {
			klog.Fatalf("Error running pod controller: %s", err.Error())
		}
	}()

	<-ctx.Done()
}

func NewOVNSBClient(endpoint string) (client.Client, error) {
	dbModel, err := ovnsb.FullDatabaseModel()
	if err != nil {
		klog.Error(err)
		return nil, err
	}

	monitors := []client.MonitorOption{
		client.WithTable(&ovnsb.Chassis{}),
	}

	sbClient, err := ovsclient.NewOvsDbClient(ovsclient.SBDB, endpoint, dbModel, monitors)
	if err != nil {
		klog.Errorf("failed to create OVN SB client: %v", err)
		return nil, err
	}
	return sbClient, nil

}

func NewOVNNBClient(endpoint string) (client.Client, error) {
	dbModel, err := ovnnb.FullDatabaseModel()
	if err != nil {
		klog.Error(err)
		return nil, err
	}

	monitors := []client.MonitorOption{
		client.WithTable(&ovnnb.ACL{}),
		client.WithTable(&ovnnb.AddressSet{}),
		client.WithTable(&ovnnb.BFD{}),
		client.WithTable(&ovnnb.DHCPOptions{}),
		client.WithTable(&ovnnb.GatewayChassis{}),
		client.WithTable(&ovnnb.HAChassis{}),
		client.WithTable(&ovnnb.HAChassisGroup{}),
		client.WithTable(&ovnnb.LoadBalancer{}),
		client.WithTable(&ovnnb.LoadBalancerHealthCheck{}),
		client.WithTable(&ovnnb.LogicalRouterPolicy{}),
		client.WithTable(&ovnnb.LogicalRouterPort{}),
		client.WithTable(&ovnnb.LogicalRouterStaticRoute{}),
		client.WithTable(&ovnnb.LogicalRouter{}),
		client.WithTable(&ovnnb.LogicalSwitchPort{}),
		client.WithTable(&ovnnb.LogicalSwitch{}),
		client.WithTable(&ovnnb.NAT{}),
		client.WithTable(&ovnnb.NBGlobal{}),
		client.WithTable(&ovnnb.PortGroup{}),
	}
	nbClient, err := ovsclient.NewOvsDbClient(ovsclient.NBDB, endpoint, dbModel, monitors)
	if err != nil {
		klog.Errorf("failed to create OVN NB client: %v", err)
		return nil, err
	}
	return nbClient, nil

}
