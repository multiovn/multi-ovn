package ovn

import (
	"context"
	"fmt"

	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnnb"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"k8s.io/klog/v2"
)

// CreateHAChassis creates a new HAChassis in the OVN database
func (c *OVNNBClient) CreateHAChassis(hc *ovnnb.HAChassis, hcg *ovnnb.HAChassisGroup) (string, error) {
	// Create HAChassis object
	uuid := GenerateUUID()
	hc.UUID = uuid

	// Use Model API to create
	result, err := c.Create(hc)
	if err != nil {
		return "", fmt.Errorf("failed to create HA chassis: %v", err)
	}

	mutationFunc := func(hcg *ovnnb.HAChassisGroup) *model.Mutation {
		mutation := &model.Mutation{
			Field:   &hcg.HaChassis,
			Value:   []string{uuid},
			Mutator: ovsdb.MutateOperationInsert,
		}
		return mutation
	}

	op, err := c.Where(hcg).Mutate(hcg, *mutationFunc(hcg))
	if err != nil {
		return "", fmt.Errorf("generate update HA chassis group operations failed: %v", err)
	}
	result = append(result, op...)

	// Execute transaction
	err = c.TransactWithCheck(context.Background(), result)
	if err != nil {
		return "", fmt.Errorf("failed to execute HA chassis creation transaction: %v", err)
	}

	return uuid, nil
}

// GetHAChassis retrieves a HAChassis from the OVN database by
func (c *OVNNBClient) GetHAChassis(uuid string, ignoreNotFound bool) (*ovnnb.HAChassis, error) {
	// Implement logic to get HA chassis from OVN database by UUID
	hcList := make([]ovnnb.HAChassis, 0)
	if err := c.WhereCache(func(hc *ovnnb.HAChassis) bool {
		return hc.UUID == uuid
	}).List(context.Background(), &hcList); err != nil {
		return nil, fmt.Errorf("list HA chassis with UUID %q: %v", uuid, err)
	}

	// not found
	if len(hcList) == 0 {
		if ignoreNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("not found HA chassis with UUID %q", uuid)
	}

	if len(hcList) > 1 {
		return nil, fmt.Errorf("more than one HA chassis with same UUID %q", uuid)
	}

	// #nosec G602
	return &hcList[0], nil
}

// UpdateHAChassis updates an existing HAChassis in the OVN database
func (c *OVNNBClient) UpdateHAChassis(hc *ovnnb.HAChassis) error {
	// Get the existing HA chassis
	existingHC, err := c.GetHAChassis(hc.UUID, false)
	if err != nil {
		return fmt.Errorf("failed to get HA chassis: %v", err)
	}

	// Update fields
	updateOp, err := c.Where(existingHC).
		Update(&ovnnb.HAChassis{
			Priority:    hc.Priority,
			ExternalIDs: hc.ExternalIDs,
		})
	if err != nil {
		return fmt.Errorf("failed to generate update HA chassis operation: %v", err)
	}

	// Execute transaction
	err = c.TransactWithCheck(context.Background(), updateOp)
	if err != nil {
		return fmt.Errorf("failed to execute HA chassis update transaction: %v", err)
	}

	return nil
}

// DeleteHAChassis deletes a HAChassis from the OVN database by UUID
func (c *OVNNBClient) DeleteHAChassis(hc *ovnnb.HAChassis, hcg *ovnnb.HAChassisGroup) error {
	ops := []ovsdb.Operation{}

	if hcg != nil {
		mutationFunc := func(hcg *ovnnb.HAChassisGroup) *model.Mutation {
			mutation := &model.Mutation{
				Field:   &hcg.HaChassis,
				Value:   []string{hc.UUID},
				Mutator: ovsdb.MutateOperationDelete,
			}
			return mutation
		}

		op, err := c.Where(hcg).Mutate(hcg, *mutationFunc(hcg))
		if err != nil {
			return fmt.Errorf("generate update hachassis group operations failed: %v", err)
		}
		ops = append(ops, op...)
	}

	op, err := c.Where(hc).Delete()
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("generate operations for deleting HA chassis: %v", err)
	}
	ops = append(ops, op...)

	err = c.TransactWithCheck(context.Background(), ops)
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("transact for deleting HA chassis: %v", err)
	}

	return nil
}
