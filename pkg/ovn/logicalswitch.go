package ovn

import (
	"context"
	"fmt"

	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnnb"
	"k8s.io/klog"
)

func (c *OVNNBClient) CreateLogicalSwitch(ls *ovnnb.LogicalSwitch) (string, error) {
	// 创建LogicalSwitch对象
	uuid := GenerateUUID()
	ls.UUID = uuid

	// 使用Model API创建
	result, err := c.Create(ls)
	if err != nil {
		return "", fmt.Errorf("创建logical switch失败: %v", err)
	}

	// 执行事务
	err = c.TransactWithCheck(context.Background(), result)
	if err != nil {
		return "", fmt.Errorf("执行logical switch创建事务失败: %v", err)
	}

	return uuid, nil
}

func (c *OVNNBClient) GetLogicalSwitch(name string, ignoreNotFound bool) (*ovnnb.LogicalSwitch, error) {
	// 实现从OVN数据库获取logical switch的逻辑
	// 这里需要使用libovsdb来查询数据库

	lsList := make([]ovnnb.LogicalSwitch, 0)
	if err := c.WhereCache(func(ls *ovnnb.LogicalSwitch) bool {
		return ls.Name == name
	}).List(context.Background(), &lsList); err != nil {
		return nil, fmt.Errorf("list switch switch %q: %v", name, err)
	}

	// not found
	if len(lsList) == 0 {
		if ignoreNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("not found logical switch %q", name)
	}

	if len(lsList) > 1 {
		return nil, fmt.Errorf("more than one logical switch with same name %q", name)
	}

	// #nosec G602
	return &lsList[0], nil
}

func (c *OVNNBClient) UpdateLogicalSwitch(ls *ovnnb.LogicalSwitch) error {
	// 实现创建logical switch的逻辑
	// 这里需要使用libovsdb来创建
	// 首先获取现有的LogicalSwitch
	existingLS, err := c.GetLogicalSwitch(ls.Name, false)
	if err != nil {
		return fmt.Errorf("获取logical switch失败: %v", err)
	}

	// 更新ExternalIDs和OtherConfig字段
	updateOp, err := c.Where(existingLS).
		Update(&ovnnb.LogicalSwitch{
			ExternalIDs: ls.ExternalIDs,
			OtherConfig: ls.OtherConfig,
		})
	if err != nil {
		return fmt.Errorf("生成更新logical switch操作失败: %v", err)
	}

	// 执行事务
	err = c.TransactWithCheck(context.Background(), updateOp)
	if err != nil {
		return fmt.Errorf("执行logical switch更新事务失败: %v", err)
	}

	return nil

}

func (c *OVNNBClient) DeleteLogicalSwitch(name string) error {
	ls, err := c.GetLogicalSwitch(name, true)
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("get logical switch %s: %v", name, err)
	}

	// not found, skip
	if ls == nil {
		return nil
	}

	op, err := c.Where(ls).Delete()
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("generate operations for deleting logical switch %s: %v", name, err)
	}

	err = c.TransactWithCheck(context.Background(), op)
	if err != nil {
		klog.Error(err)
		return fmt.Errorf("transact for deleting logical switch %s: %v", name, err)
	}

	return nil
}
