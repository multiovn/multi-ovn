package main

// 创建OVSDB NB客户端的基本配置
import (
	"time"

	"github.com/google/uuid"
	apiv1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	v1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned/typed/k8s.cni.cncf.io/v1"
	ovsclient "github.com/multiovn/multi-ovn/pkg/ovsdb/client"
	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnnb"
	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnsb"
	"github.com/ovn-org/libovsdb/cache"
	"github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/model"
	"k8s.io/klog"
)

type NetworkAttachmentDefinitionClient struct {
	v1.NetworkAttachmentDefinitionInterface
}

func GenerateUUID() string {
	return uuid.New().String()
}

// NewOVNSBClient creates a new OVN Southbound database client
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
		client.WithTable(&ovnsb.Chassis{}),
	}
	nbClient, err := ovsclient.NewOvsDbClient(ovsclient.NBDB, endpoint, dbModel, monitors)
	if err != nil {
		klog.Errorf("failed to create OVN NB client: %v", err)
		return nil, err
	}
	return nbClient, nil
}

// MonitorChassis sets up monitoring for chassis changes in the OVN Southbound database
func MonitorChassis(sbClient client.Client) error {
	// Create an event handler for chassis changes
	chassisHandler := &cache.EventHandlerFuncs{
		AddFunc: func(table string, model model.Model) {
			if chassis, ok := model.(*ovnsb.Chassis); ok {
				klog.Infof("Chassis added: %s, hostname: %s", chassis.Name, chassis.Hostname)
				for k, v := range chassis.ExternalIDs {
					klog.Infof("  External ID: %s = %s", k, v)
				}
			}
		},
		UpdateFunc: func(table string, old model.Model, new model.Model) {
			if oldChassis, ok := old.(*ovnsb.Chassis); ok {
				if newChassis, ok := new.(*ovnsb.Chassis); ok {
					klog.Infof("Chassis updated: %s, hostname: %s", newChassis.Name, newChassis.Hostname)
					klog.Infof("  Old NB config: %d, New NB config: %d", oldChassis.NbCfg, newChassis.NbCfg)
				}
			}
		},
		DeleteFunc: func(table string, model model.Model) {
			if chassis, ok := model.(*ovnsb.Chassis); ok {
				klog.Infof("Chassis deleted: %s, hostname: %s", chassis.Name, chassis.Hostname)
			}
		},
	}

	// Add the event handler to the client's cache
	sbClient.Cache().AddEventHandler(chassisHandler)

	klog.Info("Chassis monitoring started")
	return nil
}

func main() {
	// Create OVN SB client for chassis monitoring
	sbClient, err := NewOVNSBClient("tcp:10.233.33.164:6642")
	if err != nil {
		klog.Errorf("failed to create OVN SB client: %v", err)
		return
	}

	// Set up chassis monitoring
	err = MonitorChassis(sbClient)
	if err != nil {
		klog.Errorf("failed to set up chassis monitoring: %v", err)
		return
	}

	// Keep the program running to receive chassis events
	klog.Info("Waiting for chassis events...")
	for {
		time.Sleep(30 * time.Second)
		klog.Info("Still monitoring chassis...")
	}
}

func doTest() {
	NetworkAttachmentDefinition := &apiv1.NetworkAttachmentDefinition{}
	NetworkAttachmentDefinition.Name = "test"
}
