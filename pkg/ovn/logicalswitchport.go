package ovn

import (
	"context"
	"fmt"

	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnnb"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"k8s.io/klog"
)

func (c *OVNNBClient) CreateLogicalSwitchPort(lsp *ovnnb.LogicalSwitchPort, logicalSwitch *ovnnb.LogicalSwitch) (string, error) {

	uuid := GenerateUUID()
	lsp.UUID = uuid

	result, err := c.Create(lsp)
	if err != nil {
		return "", fmt.Errorf("create logical switch port failed: %v", err)
	}

	mutationFunc := func(ls *ovnnb.LogicalSwitch) *model.Mutation {
		mutation := &model.Mutation{
			Field:   &ls.Ports,
			Value:   []string{uuid},
			Mutator: ovsdb.MutateOperationInsert,
		}
		return mutation
	}

	op, err := c.Where(logicalSwitch).Mutate(logicalSwitch, *mutationFunc(logicalSwitch))
	if err != nil {
		return "", fmt.Errorf("generate update logical switch operations failed: %v", err)
	}
	result = append(result, op...)

	err = c.TransactWithCheck(context.Background(), result)
	if err != nil {
		return "", fmt.Errorf("execute logical switch port creation transaction failed: %v", err)
	}

	return uuid, nil
}

func (c *OVNNBClient) GetLogicalSwitchPort(name string, ignoreNotFound bool) (*ovnnb.LogicalSwitchPort, error) {
	lspList := make([]ovnnb.LogicalSwitchPort, 0)
	if err := c.WhereCache(func(lsp *ovnnb.LogicalSwitchPort) bool {
		return lsp.Name == name
	}).List(context.Background(), &lspList); err != nil {
		return nil, fmt.Errorf("list logical switch port %q: %v", name, err)
	}

	// not found
	if len(lspList) == 0 {
		if ignoreNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("not found logical switch port %q", name)
	}

	if len(lspList) > 1 {
		return nil, fmt.Errorf("more than one logical switch port with same name %q", name)
	}

	// #nosec G602
	return &lspList[0], nil
}

func (c *OVNNBClient) UpdateLogicalSwitchPort(lsp *ovnnb.LogicalSwitchPort) error {

	existingLSP, err := c.GetLogicalSwitchPort(lsp.Name, false)
	if err != nil {
		return fmt.Errorf("get logical switch port failed: %v", err)
	}

	updateOp, err := c.Where(existingLSP).
		Update(&ovnnb.LogicalSwitchPort{
			Addresses:    lsp.Addresses,
			Enabled:      lsp.Enabled,
			ExternalIDs:  lsp.ExternalIDs,
			Options:      lsp.Options,
			ParentName:   lsp.ParentName,
			PortSecurity: lsp.PortSecurity,
			TagRequest:   lsp.TagRequest,
			Type:         lsp.Type,
		})
	if err != nil {
		return fmt.Errorf("generate update logical switch port operations failed: %v", err)
	}

	err = c.TransactWithCheck(context.Background(), updateOp)
	if err != nil {
		return fmt.Errorf("execute logical switch port update transaction failed: %v", err)
	}

	return nil
}

func (c *OVNNBClient) DeleteLogicalSwitchPort(lsp *ovnnb.LogicalSwitchPort, logicalSwitch *ovnnb.LogicalSwitch) error {
	ops := []ovsdb.Operation{}

	if logicalSwitch != nil {
		mutationFunc := func(ls *ovnnb.LogicalSwitch) *model.Mutation {
			mutation := &model.Mutation{
				Field:   &ls.Ports,
				Value:   []string{lsp.UUID},
				Mutator: ovsdb.MutateOperationDelete,
			}
			return mutation
		}

		op, err := c.Where(logicalSwitch).Mutate(logicalSwitch, *mutationFunc(logicalSwitch))
		if err != nil {
			return fmt.Errorf("generate update logical switch operations failed: %v", err)
		}

		ops = append(ops, op...)
	}

	op, err := c.Where(lsp).Delete()

	if err != nil {
		klog.Error(err)
		return fmt.Errorf("generate operations for deleting logical switch port %s: %v", lsp.Name, err)
	}
	ops = append(ops, op...)

	err = c.TransactWithCheck(context.Background(), ops)
	if err != nil {
		return fmt.Errorf("execute logical switch port update transaction failed: %v", err)
	}

	return nil
}
