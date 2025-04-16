package ovn

import (
	"context"
	"fmt"

	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnnb"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"k8s.io/klog"
)

func (c *OVNNBClient) CreateLogicalRouterPort(lrp *ovnnb.LogicalRouterPort, lr *ovnnb.LogicalRouter) (string, error) {
	// Create LogicalRouterPort object
	uuid := GenerateUUID()
	lrp.UUID = uuid

	// Use Model API to create
	result, err := c.Create(lrp)
	if err != nil {
		return "", fmt.Errorf("failed to create logical router port: %v", err)
	}

	mutationFunc := func(lr *ovnnb.LogicalRouter) *model.Mutation {
		mutation := &model.Mutation{
			Field:   &lr.Ports,
			Value:   []string{uuid},
			Mutator: ovsdb.MutateOperationInsert,
		}
		return mutation
	}

	op, err := c.Where(lr).Mutate(lr, *mutationFunc(lr))
	if err != nil {
		return "", fmt.Errorf("generate update logical switch operations failed: %v", err)
	}
	result = append(result, op...)

	// Execute transaction
	err = c.TransactWithCheck(context.Background(), result)
	if err != nil {
		return "", fmt.Errorf("failed to execute logical router port creation transaction: %v", err)
	}

	return uuid, nil
}

func (c *OVNNBClient) GetLogicalRouterPort(name string, ignoreNotFound bool) (*ovnnb.LogicalRouterPort, error) {
	// Implement logic to get logical router port from OVN database
	lrpList := make([]ovnnb.LogicalRouterPort, 0)
	if err := c.WhereCache(func(lrp *ovnnb.LogicalRouterPort) bool {
		return lrp.Name == name
	}).List(context.Background(), &lrpList); err != nil {
		return nil, fmt.Errorf("list logical router port %q: %v", name, err)
	}

	// not found
	if len(lrpList) == 0 {
		if ignoreNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("not found logical router port %q", name)
	}

	if len(lrpList) > 1 {
		return nil, fmt.Errorf("more than one logical router port with same name %q", name)
	}

	// #nosec G602
	return &lrpList[0], nil
}

func (c *OVNNBClient) UpdateLogicalRouterPort(lrp *ovnnb.LogicalRouterPort) error {
	// Implement logic to update logical router port
	existingLRP, err := c.GetLogicalRouterPort(lrp.Name, false)
	if err != nil {
		return fmt.Errorf("failed to get logical router port: %v", err)
	}

	// Update fields
	updateOp, err := c.Where(existingLRP).
		Update(&ovnnb.LogicalRouterPort{
			Enabled:        lrp.Enabled,
			ExternalIDs:    lrp.ExternalIDs,
			Options:        lrp.Options,
			HaChassisGroup: lrp.HaChassisGroup,
			Ipv6Prefix:     lrp.Ipv6Prefix,
			Ipv6RaConfigs:  lrp.Ipv6RaConfigs,
			MAC:            lrp.MAC,
			Networks:       lrp.Networks,
			Peer:           lrp.Peer,
		})
	if err != nil {
		return fmt.Errorf("failed to generate update logical router port operation: %v", err)
	}

	// Execute transaction
	err = c.TransactWithCheck(context.Background(), updateOp)
	if err != nil {
		return fmt.Errorf("failed to execute logical router port update transaction: %v", err)
	}

	return nil
}

func (c *OVNNBClient) DeleteLogicalRouterPort(lrp *ovnnb.LogicalRouterPort, lr *ovnnb.LogicalRouter) error {
	ops := []ovsdb.Operation{}
	if lr != nil {
		// Delete the logical router port
		mutationFunc := func(lr *ovnnb.LogicalRouter) *model.Mutation {
			mutation := &model.Mutation{
				Field:   &lr.Ports,
				Value:   []string{lrp.UUID},
				Mutator: ovsdb.MutateOperationDelete,
			}
			return mutation
		}

		op, err := c.Where(lr).Mutate(lr, *mutationFunc(lr))
		if err != nil {
			return fmt.Errorf("generate update logical router  port failed: %v", err)
		}

		ops = append(ops, op...)
	}

	op, err := c.Where(lrp).Delete()
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("generate operations for deleting logical router port %s: %v", lrp.Name, err)
	}
	ops = append(ops, op...)

	err = c.TransactWithCheck(context.Background(), op)
	if err != nil {
		return fmt.Errorf("execute logical router port update transaction failed: %v", err)
	}

	return nil
}
