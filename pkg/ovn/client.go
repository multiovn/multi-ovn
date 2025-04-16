package ovn

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/ovsdb"
	"k8s.io/klog/v2"
)

var ResourceFinalizer = "multiovn.ovn.io/resourceFinalizer"

type OVNNBClient struct {
	client.Client
}

func GenerateUUID() string {
	return uuid.New().String()
}

func GetEntityName(namespace, name string) string {
	return fmt.Sprintf("%s-%s", namespace, name)
}

func (c *OVNNBClient) TransactWithCheck(ctx context.Context, operations []ovsdb.Operation) error {
	results, err := c.Transact(ctx, operations...)
	if err != nil {
		return err
	}

	errors, err := ovsdb.CheckOperationResults(results, operations)
	if err != nil {
		klog.Errorf("error occurred in transact with operations %+v with operation errors %+v: %v", operations, errors, err)
		return err
	}

	return nil
}

type OVNSBClient struct {
	client.Client
}
