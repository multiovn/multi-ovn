package chassis

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"

	multiovnv1 "github.com/multiovn/multi-ovn/pkg/apis/multiovn/v1"
	clientset "github.com/multiovn/multi-ovn/pkg/client/clientset/versioned"
	"github.com/multiovn/multi-ovn/pkg/ovn"
	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnsb"
	"github.com/ovn-org/libovsdb/cache"
	"github.com/ovn-org/libovsdb/model"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Controller struct {
	ovnClient clientset.Interface

	ovnsbClient *ovn.OVNSBClient
}

func NewController(ovnClient clientset.Interface, ovnsbClient *ovn.OVNSBClient) *Controller {
	controller := &Controller{
		ovnClient:   ovnClient,
		ovnsbClient: ovnsbClient,
	}

	return controller
}

func (c *Controller) Run(ctx context.Context) error {
	klog.Info("start chassis controller")
	chassisList, err := c.ovnClient.MultiovnV1().Chassises().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, chassis := range chassisList.Items {
		err := c.ovnClient.MultiovnV1().Chassises().Delete(context.Background(), chassis.Name, metav1.DeleteOptions{})
		if err != nil {
			return err
		}
	}

	// Create a slice to store the chassis objects from OVN SB
	ovnChassisList := make([]*ovnsb.Chassis, 0)

	err = c.ovnsbClient.List(ctx, &ovnChassisList)
	if err != nil {
		return err
	}

	for _, chassis := range ovnChassisList {
		err := c.ensureChassis(chassis)
		if err != nil {
			return err
		}
	}
	// Create an event handler for chassis changes
	chassisHandler := &cache.EventHandlerFuncs{
		AddFunc: func(table string, model model.Model) {
			if chassis, ok := model.(*ovnsb.Chassis); ok {
				err := c.ensureChassis(chassis)
				if err != nil {
					klog.Errorf("failed to ensure chassis %s: %v", chassis.Name, err)
				}
			}
		},
		DeleteFunc: func(table string, model model.Model) {
			if chassis, ok := model.(*ovnsb.Chassis); ok {
				err := c.deleteChassis(chassis)
				if err != nil {
					klog.Errorf("failed to delete chassis %s: %v", chassis.Name, err)
				}
			}
		},

		UpdateFunc: func(table string, old model.Model, new model.Model) {
			if newChassis, ok := new.(*ovnsb.Chassis); ok {
				err := c.ensureChassis(newChassis)
				if err != nil {
					klog.Errorf("failed to ensure chassis %s: %v", newChassis.Name, err)
				}
			}

		},
	}

	// Add the event handler to the client's cache
	c.ovnsbClient.Client.Cache().AddEventHandler(chassisHandler)

	return nil
}

func (c *Controller) ensureChassis(chassis *ovnsb.Chassis) error {
	klog.Infof("ensureChassis: %s", chassis.Name)
	_, err := c.ovnClient.MultiovnV1().Chassises().Get(context.Background(), chassis.Name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
	}

	if apierrors.IsNotFound(err) {
		chassisObj := &multiovnv1.Chassis{
			ObjectMeta: metav1.ObjectMeta{
				Name: chassis.Name,
			},
		}

		chassisObj, err = c.ovnClient.MultiovnV1().Chassises().Create(context.Background(), chassisObj, metav1.CreateOptions{})
		if err != nil {
			return err
		}

		chassisObj.Status = multiovnv1.ChassisStatus{
			UUID:     chassis.UUID,
			HostName: chassis.Hostname,
		}

		_, err = c.ovnClient.MultiovnV1().Chassises().UpdateStatus(context.Background(), chassisObj, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
	}

	klog.Infof("ensureChassis: %s success", chassis.Name)
	return nil

}

func (c *Controller) deleteChassis(chassis *ovnsb.Chassis) error {
	klog.Infof("deleteChassis: %s", chassis.Name)
	existChassisObj, err := c.ovnClient.MultiovnV1().Chassises().Get(context.Background(), chassis.Name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
	}

	if existChassisObj != nil {
		return nil
	}

	err = c.ovnClient.MultiovnV1().Chassises().Delete(context.Background(), chassis.Name, metav1.DeleteOptions{})
	if err != nil {
		return err
	}

	return nil
}
