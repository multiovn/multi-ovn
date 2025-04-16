package ovn

import (
	"github.com/multiovn/multi-ovn/pkg/ovsdb/ovnnb"
	"github.com/ovn-org/libovsdb/ovsdb"
)

type Common interface {
	Transact(method string, operations []ovsdb.Operation) error
}

type ILogicalSwitch interface {
	CreateLogicalSwitch(ls *ovnnb.LogicalSwitch) (string, error)
	GetLogicalSwitch(name string) (*ovnnb.LogicalSwitch, error)
	UpdateLogicalSwitch(ls *ovnnb.LogicalSwitch) error
	DeleteLogicalSwitch(name string) error
}

type ILogicalSwitchPort interface {
	CreateLogicalSwitchPort(lsp *ovnnb.LogicalSwitchPort, logicalSwitch string) (string, error)
	GetLogicalSwitchPort(name string, ignoreNotFound bool) (*ovnnb.LogicalSwitchPort, error)
	UpdateLogicalSwitchPort(lsp *ovnnb.LogicalSwitchPort) error
	DeleteLogicalSwitchPort(name string) error
}

type IHAChassisGroup interface {
	CreateHAChassisGroup(hcg *ovnnb.HAChassisGroup) (string, error)
	GetHAChassisGroup(name string, ignoreNotFound bool) (*ovnnb.HAChassisGroup, error)
	UpdateHAChassisGroup(hcg *ovnnb.HAChassisGroup) error
	DeleteHAChassisGroup(name string) error
}

type NBClient interface {
	Common
	ILogicalSwitch
	ILogicalSwitchPort
	IHAChassisGroup
}
