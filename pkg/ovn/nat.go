package ovn

import (
	"context"
	"fmt"

	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnnb"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"k8s.io/klog"
)

func (c *OVNNBClient) CreateNat(nat *ovnnb.NAT, lr *ovnnb.LogicalRouter) (string, error) {
	// Create NAT object
	uuid := GenerateUUID()
	nat.UUID = uuid

	// Use Model API to create
	result, err := c.Create(nat)
	if err != nil {
		return "", fmt.Errorf("failed to create nat: %v", err)
	}

	mutationFunc := func(lr *ovnnb.LogicalRouter) *model.Mutation {
		mutation := &model.Mutation{
			Field:   &lr.Nat,
			Value:   []string{uuid},
			Mutator: ovsdb.MutateOperationInsert,
		}
		return mutation
	}

	op, err := c.Where(lr).Mutate(lr, *mutationFunc(lr))
	if err != nil {
		return "", fmt.Errorf("generate update logical router operations failed: %v", err)
	}
	result = append(result, op...)

	// Execute transaction
	err = c.TransactWithCheck(context.Background(), result)
	if err != nil {
		return "", fmt.Errorf("failed to execute nat creation transaction: %v", err)
	}

	return uuid, nil
}

func (c *OVNNBClient) GetNat(uuid string, ignoreNotFound bool) (*ovnnb.NAT, error) {
	// For NAT, we need to find it by external_ids
	natList := make([]ovnnb.NAT, 0)

	// Find NAT by UUID
	if err := c.WhereCache(func(nat *ovnnb.NAT) bool {
		return nat.UUID == uuid
	}).List(context.Background(), &natList); err != nil {
		return nil, fmt.Errorf("list nat %q: %v", uuid, err)
	}
	// not found
	if len(natList) == 0 {
		if ignoreNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("not found nat %q", uuid)
	}

	if len(natList) > 1 {
		return nil, fmt.Errorf("more than one nat with same name %q", uuid)
	}

	// #nosec G602
	return &natList[0], nil
}

func (c *OVNNBClient) UpdateNat(nat *ovnnb.NAT) error {
	// Implement logic to update NAT
	existingNAT := &ovnnb.NAT{UUID: nat.UUID}
	if err := c.Get(context.Background(), existingNAT); err != nil {
		return fmt.Errorf("failed to get nat: %v", err)
	}

	// Update fields
	updateOp, err := c.Where(existingNAT).
		Update(&ovnnb.NAT{
			AllowedExtIPs:     nat.AllowedExtIPs,
			ExemptedExtIPs:    nat.ExemptedExtIPs,
			ExternalIDs:       nat.ExternalIDs,
			ExternalIP:        nat.ExternalIP,
			ExternalMAC:       nat.ExternalMAC,
			ExternalPortRange: nat.ExternalPortRange,
			GatewayPort:       nat.GatewayPort,
			LogicalIP:         nat.LogicalIP,
			LogicalPort:       nat.LogicalPort,
			Options:           nat.Options,
			Type:              nat.Type,
		})
	if err != nil {
		return fmt.Errorf("failed to generate update nat operation: %v", err)
	}

	// Execute transaction
	err = c.TransactWithCheck(context.Background(), updateOp)
	if err != nil {
		return fmt.Errorf("failed to execute nat update transaction: %v", err)
	}

	return nil
}

func (c *OVNNBClient) RemoveNatFromLogicalRouter(nat *ovnnb.NAT, lr *ovnnb.LogicalRouter) error {
	ops := []ovsdb.Operation{}
	if lr != nil {
		mutationFunc := func(lr *ovnnb.LogicalRouter) *model.Mutation {
			mutation := &model.Mutation{
				Field:   &lr.Nat,
				Value:   []string{nat.UUID},
				Mutator: ovsdb.MutateOperationDelete,
			}
			return mutation
		}

		op, err := c.Where(lr).Mutate(lr, *mutationFunc(lr))
		if err != nil {
			return fmt.Errorf("generate update logical router nat failed: %v", err)
		}
		ops = append(ops, op...)
	}
	// Delete the NAT from logical router
	op, err := c.Where(nat).Delete()
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("generate operations for deleting nat %s: %v", nat.UUID, err)
	}
	ops = append(ops, op...)
	err = c.TransactWithCheck(context.Background(), ops)
	if err != nil {
		return fmt.Errorf("execute logical router nat update transaction failed: %v", err)
	}

	return nil
}
