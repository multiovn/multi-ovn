package ovn

import (
	"context"
	"fmt"

	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnnb"
	"k8s.io/klog"
)

func (c *OVNNBClient) CreateLogicalRouter(lr *ovnnb.LogicalRouter) (string, error) {
	// Create LogicalRouter object
	uuid := GenerateUUID()
	lr.UUID = uuid

	// Use Model API to create
	result, err := c.Create(lr)
	if err != nil {
		return "", fmt.Errorf("failed to create logical router: %v", err)
	}

	// Execute transaction
	err = c.TransactWithCheck(context.Background(), result)
	if err != nil {
		return "", fmt.Errorf("failed to execute logical router creation transaction: %v", err)
	}

	return uuid, nil
}

func (c *OVNNBClient) GetLogicalRouter(name string, ignoreNotFound bool) (*ovnnb.LogicalRouter, error) {
	// Implement logic to get logical router from OVN database
	lrList := make([]ovnnb.LogicalRouter, 0)
	if err := c.WhereCache(func(lr *ovnnb.LogicalRouter) bool {
		return lr.Name == name
	}).List(context.Background(), &lrList); err != nil {
		return nil, fmt.Errorf("list logical router %q: %v", name, err)
	}

	// not found
	if len(lrList) == 0 {
		if ignoreNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("not found logical router %q", name)
	}

	if len(lrList) > 1 {
		return nil, fmt.Errorf("more than one logical router with same name %q", name)
	}

	// #nosec G602
	return &lrList[0], nil
}

func (c *OVNNBClient) UpdateLogicalRouter(lr *ovnnb.LogicalRouter) error {
	// Implement logic to update logical router
	existingLR, err := c.GetLogicalRouter(lr.Name, false)
	if err != nil {
		return fmt.Errorf("failed to get logical router: %v", err)
	}

	// Update Enabled, Options, and ExternalIDs fields
	updateOp, err := c.Where(existingLR).
		Update(&ovnnb.LogicalRouter{
			Enabled:     lr.Enabled,
			Options:     lr.Options,
			ExternalIDs: lr.ExternalIDs,
		})
	if err != nil {
		return fmt.Errorf("failed to generate update logical router operation: %v", err)
	}

	// Execute transaction
	err = c.TransactWithCheck(context.Background(), updateOp)
	if err != nil {
		return fmt.Errorf("failed to execute logical router update transaction: %v", err)
	}

	return nil
}

func (c *OVNNBClient) DeleteLogicalRouter(name string) error {
	lr, err := c.GetLogicalRouter(name, true)
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("get logical router %s: %v", name, err)
	}

	// not found, skip
	if lr == nil {
		return nil
	}

	op, err := c.Where(lr).Delete()
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("generate operations for deleting logical router %s: %v", name, err)
	}

	err = c.TransactWithCheck(context.Background(), op)
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("transact for deleting logical router %s: %v", name, err)
	}

	return nil
}
