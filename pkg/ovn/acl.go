package ovn

import (
	"context"
	"fmt"

	"github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/model"
	"github.com/ovn-org/libovsdb/ovsdb"

	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnnb"
)

// GetACL retrieves an ACL from OVN by UUID
func (c *OVNNBClient) GetACL(uuid string, ignoreNotFound bool) (*ovnnb.ACL, error) {
	ctx := context.Background()
	acl := &ovnnb.ACL{UUID: uuid}

	err := c.Get(ctx, acl)
	if err != nil {
		if err == client.ErrNotFound && ignoreNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get ACL: %v", err)
	}

	return acl, nil
}

// CreateACL creates a new ACL in OVN
func (c *OVNNBClient) CreateACL(acl *ovnnb.ACL, ls *ovnnb.LogicalSwitch, pg *ovnnb.PortGroup) (string, error) {
	ctx := context.Background()

	// Generate a UUID for the ACL
	uuid := GenerateUUID()
	acl.UUID = uuid

	// Create the ACL
	ops, err := c.Create(acl)
	if err != nil {
		return "", fmt.Errorf("failed to create ACL operations: %v", err)
	}

	if ls != nil {
		mutationFunc := func(ls *ovnnb.LogicalSwitch) *model.Mutation {
			mutation := &model.Mutation{
				Field:   &ls.ACLs,
				Value:   []string{uuid},
				Mutator: ovsdb.MutateOperationInsert,
			}
			return mutation
		}

		op, err := c.Where(ls).Mutate(ls, *mutationFunc(ls))
		if err != nil {
			return "", fmt.Errorf("generate update logical router operations failed: %v", err)
		}
		ops = append(ops, op...)
	} else {
		mutationFunc := func(pg *ovnnb.PortGroup) *model.Mutation {
			mutation := &model.Mutation{
				Field:   &pg.ACLs,
				Value:   []string{uuid},
				Mutator: ovsdb.MutateOperationInsert,
			}
			return mutation
		}

		op, err := c.Where(pg).Mutate(pg, *mutationFunc(pg))
		if err != nil {
			return "", fmt.Errorf("generate update port group operations failed: %v", err)
		}
		ops = append(ops, op...)
	}
	// Execute the operations
	err = c.TransactWithCheck(ctx, ops)
	if err != nil {
		return "", fmt.Errorf("failed to create ACL: %v", err)
	}

	return uuid, nil
}

// UpdateACL updates an existing ACL in OVN
func (c *OVNNBClient) UpdateACL(acl *ovnnb.ACL) error {
	ctx := context.Background()

	// Update the ACL
	ops, err := c.Where(acl).Update(acl)
	if err != nil {
		return fmt.Errorf("failed to create update ACL operations: %v", err)
	}

	// Execute the operations
	err = c.TransactWithCheck(ctx, ops)
	if err != nil {
		return fmt.Errorf("failed to update ACL: %v", err)
	}

	return nil
}

// DeleteACL deletes an ACL from OVN
func (c *OVNNBClient) DeleteACL(acl *ovnnb.ACL, ls *ovnnb.LogicalSwitch, pg *ovnnb.PortGroup) error {

	ctx := context.Background()
	ops := []ovsdb.Operation{}

	if ls != nil {
		mutationFunc := func(ls *ovnnb.LogicalSwitch) *model.Mutation {
			mutation := &model.Mutation{
				Field:   &ls.ACLs,
				Value:   []string{acl.UUID},
				Mutator: ovsdb.MutateOperationDelete,
			}
			return mutation
		}

		op, err := c.Where(ls).Mutate(ls, *mutationFunc(ls))
		if err != nil {
			return fmt.Errorf("generate update logical router operations failed: %v", err)
		}
		ops = append(ops, op...)
	}

	if pg != nil {
		mutationFunc := func(pg *ovnnb.PortGroup) *model.Mutation {
			mutation := &model.Mutation{
				Field:   &pg.ACLs,
				Value:   []string{acl.UUID},
				Mutator: ovsdb.MutateOperationDelete,
			}
			return mutation
		}

		op, err := c.Where(pg).Mutate(pg, *mutationFunc(pg))
		if err != nil {
			return fmt.Errorf("generate update port group operations failed: %v", err)
		}
		ops = append(ops, op...)
	}
	// Delete the ACL
	op, err := c.Where(acl).Delete()
	if err != nil {
		return fmt.Errorf("failed to create delete ACL operations: %v", err)
	}
	ops = append(ops, op...)
	// Execute the operations
	err = c.TransactWithCheck(ctx, ops)
	if err != nil {
		return fmt.Errorf("failed to delete ACL: %v", err)
	}

	return nil
}
