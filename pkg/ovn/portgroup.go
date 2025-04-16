package ovn

import (
	"context"
	"fmt"

	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnnb"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"
	"k8s.io/klog"
)

func (c *OVNNBClient) CreatePortGroup(pg *ovnnb.PortGroup) (string, error) {
	// Create PortGroup object
	uuid := GenerateUUID()
	pg.UUID = uuid

	// Use Model API to create
	result, err := c.Create(pg)
	if err != nil {
		return "", fmt.Errorf("failed to create port group: %v", err)
	}

	// Execute transaction
	err = c.TransactWithCheck(context.Background(), result)
	if err != nil {
		return "", fmt.Errorf("failed to execute port group creation transaction: %v", err)
	}

	return uuid, nil
}

func (c *OVNNBClient) GetPortGroup(name string, ignoreNotFound bool) (*ovnnb.PortGroup, error) {
	// Implementation to get port group from OVN database
	pgList := make([]ovnnb.PortGroup, 0)
	if err := c.WhereCache(func(pg *ovnnb.PortGroup) bool {
		return pg.Name == name
	}).List(context.Background(), &pgList); err != nil {
		return nil, fmt.Errorf("list port group %q: %v", name, err)
	}

	// not found
	if len(pgList) == 0 {
		if ignoreNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("not found port group %q", name)
	}

	if len(pgList) > 1 {
		return nil, fmt.Errorf("more than one port group with same name %q", name)
	}

	// #nosec G602
	return &pgList[0], nil
}

func (c *OVNNBClient) UpdatePortGroup(pg *ovnnb.PortGroup) error {
	existingPG, err := c.GetPortGroup(pg.Name, false)
	if err != nil {
		return fmt.Errorf("failed to get port group %q: %v", pg.Name, err)
	}

	op, err := c.Where(existingPG).Update(&ovnnb.PortGroup{
		ExternalIDs: pg.ExternalIDs,
	})

	if err != nil {
		return fmt.Errorf("generate update port group operations failed: %v", err)
	}
	return c.TransactWithCheck(context.Background(), op)
}

func (c *OVNNBClient) AddPortToPortGroup(pg *ovnnb.PortGroup, portUUID string) error {
	mutationFunc := func(pg *ovnnb.PortGroup) *model.Mutation {
		mutation := &model.Mutation{
			Field:   &pg.Ports,
			Value:   []string{portUUID},
			Mutator: ovsdb.MutateOperationInsert,
		}
		return mutation
	}

	op, err := c.Where(pg).Mutate(pg, *mutationFunc(pg))
	if err != nil {
		return fmt.Errorf("generate add port to port group operations failed: %v", err)
	}

	// Execute transaction
	err = c.TransactWithCheck(context.Background(), op)
	if err != nil {
		return fmt.Errorf("failed to execute add port to port group transaction: %v", err)
	}

	return nil
}

func (c *OVNNBClient) DeletePortToPortGroup(pg *ovnnb.PortGroup, portUUID string) error {
	mutationFunc := func(pg *ovnnb.PortGroup) *model.Mutation {
		mutation := &model.Mutation{
			Field:   &pg.Ports,
			Value:   []string{portUUID},
			Mutator: ovsdb.MutateOperationDelete,
		}
		return mutation
	}

	op, err := c.Where(pg).Mutate(pg, *mutationFunc(pg))
	if err != nil {
		return fmt.Errorf("generate delete port from port group operations failed: %v", err)
	}

	// Execute transaction
	err = c.TransactWithCheck(context.Background(), op)
	if err != nil {
		return fmt.Errorf("failed to execute port group delete port transaction: %v", err)
	}

	return nil
}

func (c *OVNNBClient) DeletePortGroup(name string) error {
	pg, err := c.GetPortGroup(name, true)
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("get port group %s: %v", name, err)
	}

	// not found, skip
	if pg == nil {
		return nil
	}

	op, err := c.Where(pg).Delete()
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("generate operations for deleting port group %s: %v", name, err)
	}

	err = c.TransactWithCheck(context.Background(), op)
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("transact for deleting port group %s: %v", name, err)
	}

	return nil
}
