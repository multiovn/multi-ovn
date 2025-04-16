package ovn

import (
	"context"
	"fmt"

	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnnb"
	"k8s.io/klog/v2"
)

// CreateHAChassisGroup creates a new HAChassisGroup in the OVN database
func (c *OVNNBClient) CreateHAChassisGroup(hcg *ovnnb.HAChassisGroup) (string, error) {
	// Create HAChassisGroup object
	uuid := GenerateUUID()
	hcg.UUID = uuid

	// Use Model API to create
	result, err := c.Create(hcg)
	if err != nil {
		return "", fmt.Errorf("failed to create HA chassis group: %v", err)
	}

	// Execute transaction
	err = c.TransactWithCheck(context.Background(), result)
	if err != nil {
		return "", fmt.Errorf("failed to execute HA chassis group creation transaction: %v", err)
	}

	return uuid, nil
}

// GetHAChassisGroup retrieves a HAChassisGroup from the OVN database by name
func (c *OVNNBClient) GetHAChassisGroup(name string, ignoreNotFound bool) (*ovnnb.HAChassisGroup, error) {
	// Implement logic to get HA chassis group from OVN database
	hcgList := make([]ovnnb.HAChassisGroup, 0)
	if err := c.WhereCache(func(hcg *ovnnb.HAChassisGroup) bool {
		return hcg.Name == name
	}).List(context.Background(), &hcgList); err != nil {
		return nil, fmt.Errorf("list HA chassis group %q: %v", name, err)
	}

	// not found
	if len(hcgList) == 0 {
		if ignoreNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("not found HA chassis group %q", name)
	}

	if len(hcgList) > 1 {
		return nil, fmt.Errorf("more than one HA chassis group with same name %q", name)
	}

	// #nosec G602
	return &hcgList[0], nil
}

// UpdateHAChassisGroup updates an existing HAChassisGroup in the OVN database
func (c *OVNNBClient) UpdateHAChassisGroup(hcg *ovnnb.HAChassisGroup) error {
	// Implement logic to update HA chassis group
	existingHCG, err := c.GetHAChassisGroup(hcg.Name, false)
	if err != nil {
		return fmt.Errorf("failed to get HA chassis group: %v", err)
	}

	// Update ExternalIDs fields
	updateOp, err := c.Where(existingHCG).
		Update(&ovnnb.HAChassisGroup{
			ExternalIDs: hcg.ExternalIDs,
		})
	if err != nil {
		return fmt.Errorf("failed to generate update HA chassis group operation: %v", err)
	}

	// Execute transaction
	err = c.TransactWithCheck(context.Background(), updateOp)
	if err != nil {
		return fmt.Errorf("failed to execute HA chassis group update transaction: %v", err)
	}

	return nil
}

// DeleteHAChassisGroup deletes a HAChassisGroup from the OVN database by name
func (c *OVNNBClient) DeleteHAChassisGroup(name string) error {
	hcg, err := c.GetHAChassisGroup(name, true)
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("get HA chassis group %s: %v", name, err)
	}

	// not found, skip
	if hcg == nil {
		return nil
	}

	op, err := c.Where(hcg).Delete()
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("generate operations for deleting HA chassis group %s: %v", name, err)
	}

	err = c.TransactWithCheck(context.Background(), op)
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("transact for deleting HA chassis group %s: %v", name, err)
	}

	return nil
}
