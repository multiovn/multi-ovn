package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ProtocolIPv4 = "IPv4"
	ProtocolIPv6 = "IPv6"
	ProtocolDual = "Dual"

	GWDistributedType = "distributed"
	GWCentralizedType = "centralized"
)

// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LogicalSwitchPort is a specification for a LogicalSwitchPort resource
type LogicalSwitchPort struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LogicalSwitchPortSpec   `json:"spec"`
	Status LogicalSwitchPortStatus `json:"status,omitempty"`
}

type LogicalSwitchPortSpec struct {
	LogicalSwitchName          string            `json:"logicalSwitchName"`
	LogicalSwitchNamespace     string            `json:"logicalSwitchNamespace"`
	LogicalRouterPortName      string            `json:"logicalRouterPortName,omitempty"`
	LogicalRouterPortNamespace string            `json:"logicalRouterPortNamespace,omitempty"`
	Addresses                  []string          `json:"addresses,omitempty"`
	Enabled                    *bool             `json:"enabled,omitempty"`
	ExternalIDs                map[string]string `json:"externalIDs,omitempty"`
	Options                    map[string]string `json:"options,omitempty"`
	ParentName                 *string           `json:"parentName,omitempty"`
	PortSecurity               []string          `json:"portSecurity,omitempty"`
	TagRequest                 *int              `json:"tagRequest,omitempty"`
	Type                       string            `json:"type,omitempty"`
}

type LogicalSwitchPortStatus struct {
	UUID                  string `json:"uuid,omitempty"`
	LogicalSwitchUUID     string `json:"logicalSwitchUUID"`
	LogicalRouterPortUUID string `json:"logicalRouterPortUUID,omitempty"`
}

type Route struct {
	Destination string `json:"destination,omitempty"`
	NextHop     string `json:"nextHop,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LogicalSwitchPortList contains a list of LogicalSwitchPort
type LogicalSwitchPortList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LogicalSwitchPort `json:"items"`
}

// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Port struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PortSpec   `json:"spec"`
	Status PortStatus `json:"status,omitempty"`
}

type PortSpec struct {
	MacAddress                  string   `json:"macAddress,omitempty"`
	IPAddresses                 []string `json:"ipAddresses,omitempty"`
	Routes                      []string `json:"routes,omitempty"`
	UserDefinedNetworkName      string   `json:"userDefinedNetworkName"`
	UserDefinedNetworkNamespace string   `json:"userDefinedNetworkNamespace"`
}

type PortStatus struct {
	MacAddress  string   `json:"macAddress"`
	IPAddresses []string `json:"ipAddresses"`
	Routes      []Route  `json:"routes,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type PortList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Port `json:"items"`
}

// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="UUID",type=string,JSONPath=`.status.uuid`
// +kubebuilder:resource:scope=Namespaced
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LogicalSwitch struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LogicalSwitchSpec   `json:"spec"`
	Status LogicalSwitchStatus `json:"status,omitempty"`
}

type LogicalSwitchStatus struct {
	UUID string `json:"uuid,omitempty"`
}

type LogicalSwitchSpec struct {
	OtherConfig map[string]string `json:"otherConfig,omitempty"`
	ExternalIDs map[string]string `json:"externalIDs,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LogicalSwitchList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LogicalSwitch `json:"items"`
}

// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="UUID",type=string,JSONPath=`.status.uuid`
// +kubebuilder:resource:scope=Namespaced
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LogicalRouter struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LogicalRouterSpec   `json:"spec"`
	Status LogicalRouterStatus `json:"status,omitempty"`
}

type LogicalRouterStatus struct {
	UUID string `json:"uuid,omitempty"`
}

type LogicalRouterSpec struct {
	Enabled     *bool             `json:"enabled,omitempty"`
	Options     map[string]string `json:"options,omitempty"`
	ExternalIDs map[string]string `json:"externalIDs,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LogicalRouterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LogicalRouter `json:"items"`
}

// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="UUID",type=string,JSONPath=`.status.uuid`
// +kubebuilder:resource:scope=Namespaced
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LogicalRouterPort struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LogicalRouterPortSpec   `json:"spec"`
	Status LogicalRouterPortStatus `json:"status,omitempty"`
}

type LogicalRouterPortStatus struct {
	UUID              string `json:"uuid,omitempty"`
	LogicalRouterUUID string `json:"logicalRouterUUID,omitempty"`
}

type LogicalRouterPortSpec struct {
	Enabled                *bool             `json:"enabled,omitempty"`
	ExternalIDs            map[string]string `json:"externalIDs,omitempty"`
	Options                map[string]string `json:"options,omitempty"`
	HaChassisGroup         *string           `json:"haChassisGroup,omitempty"`
	Ipv6Prefix             []string          `json:"ipv6Prefix,omitempty"`
	Ipv6RaConfigs          map[string]string `json:"ipv6RaConfigs,omitempty"`
	MAC                    string            `json:"mac,omitempty"`
	Networks               []string          `json:"networks,omitempty"`
	Peer                   *string           `json:"peer,omitempty"`
	LogicalRouterName      string            `json:"logicalRouterName,omitempty"`
	LogicalRouterNamespace string            `json:"logicalRouterNamespace,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type LogicalRouterPortList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LogicalRouterPort `json:"items"`
}

// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="UUID",type=string,JSONPath=`.status.uuid`
// +kubebuilder:resource:scope=Namespaced
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Nat struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NatSpec   `json:"spec"`
	Status NatStatus `json:"status,omitempty"`
}

type NatSpec struct {
	LogicalRouterName      string            `json:"logicalRouterName"`
	LogicalRouterNamespace string            `json:"logicalRouterNamespace"`
	AllowedExtIPs          *string           `json:"allowedExtIps,omitempty"`
	ExemptedExtIPs         *string           `json:"exemptedExtIps,omitempty"`
	ExternalIDs            map[string]string `json:"externalIDs,omitempty"`
	ExternalIP             string            `json:"externalIP"`
	ExternalMAC            *string           `json:"externalMAC,omitempty"`
	ExternalPortRange      string            `json:"externalPortRange,omitempty"`
	GatewayPort            *string           `json:"gatewayPort,omitempty"`
	LogicalIP              string            `json:"logicalIP"`
	LogicalPort            *string           `json:"logicalPort,omitempty"`
	Options                map[string]string `json:"options,omitempty"`
	Type                   string            `json:"type"`
}

type NatStatus struct {
	UUID              string `json:"uuid,omitempty"`
	LogicalRouterUUID string `json:"logicalRouterUUID,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type NatList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Nat `json:"items"`
}

// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="UUID",type=string,JSONPath=`.status.uuid`
// +kubebuilder:resource:scope=Namespaced
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type PortGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PortGroupSpec   `json:"spec"`
	Status PortGroupStatus `json:"status,omitempty"`
}

type PortGroupSpec struct {
	ExternalIDs map[string]string `json:"externalIDs,omitempty"`
}

type PortGroupStatus struct {
	UUID string `json:"uuid,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type PortGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PortGroup `json:"items"`
}

// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="PortGroupUUID",type=string,JSONPath=`.status.portGroupUUID`
// +kubebuilder:printcolumn:name="LogicalSwitchPortUUID",type=string,JSONPath=`.status.logicalSwitchPortUUID`
// +kubebuilder:resource:scope=Namespaced
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type PortGroupPort struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PortGroupPortSpec   `json:"spec"`
	Status            PortGroupPortStatus `json:"status,omitempty"`
}

type PortGroupPortSpec struct {
	PortGroupName              string `json:"portGroupName"`
	PortGroupNamespace         string `json:"portGroupNamespace"`
	LogicalSwitchPortNamespace string `json:"logicalSwitchPortNamespace"`
	LogicalSwitchPortName      string `json:"logicalSwitchPortName"`
}

type PortGroupPortStatus struct {
	PortGroupUUID         string `json:"portGroupUUID,omitempty"`
	LogicalSwitchPortUUID string `json:"logicalSwitchPortUUID,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type PortGroupPortList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PortGroupPort `json:"items"`
}

// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="UUID",type=string,JSONPath=`.status.uuid`
// +kubebuilder:resource:scope=Namespaced
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ACL struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ACLSpec   `json:"spec"`
	Status ACLStatus `json:"status,omitempty"`
}

type ACLSpec struct {
	ParentType      string            `json:"parentType"`
	ParentNamespace string            `json:"parentNamespace"`
	ParentName      string            `json:"parentName"`
	Priority        int               `json:"priority"`
	Match           string            `json:"match"`
	Action          string            `json:"action"`
	Direction       string            `json:"direction"`
	ExternalIDs     map[string]string `json:"externalIDs,omitempty"`
	Label           int               `json:"label,omitempty"`
	Log             bool              `json:"log,omitempty"`
	Meter           *string           `json:"meter,omitempty"`
	Name            *string           `json:"name,omitempty"`
	Options         map[string]string `json:"options,omitempty"`
	Severity        *string           `json:"severity,omitempty"`
}

type ACLStatus struct {
	UUID       string `json:"uuid,omitempty"`
	ParentUUID string `json:"parentUUID,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ACLList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ACL `json:"items"`
}

// +genclient
// +genclient:nonNamespaced
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="UUID",type=string,JSONPath=`.status.uuid`
// +kubebuilder:resource:scope=Cluster,path=chassises
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Chassis struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Status ChassisStatus `json:"status,omitempty"`
}

type ChassisStatus struct {
	UUID     string `json:"uuid,omitempty"`
	HostName string `json:"hostName,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ChassisList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Chassis `json:"items"`
}

// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="UUID",type=string,JSONPath=`.status.uuid`
// +kubebuilder:resource:scope=Namespaced,path=hachassises
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type HAChassis struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HAChassisSpec   `json:"spec"`
	Status HAChassisStatus `json:"status,omitempty"`
}

type HAChassisSpec struct {
	ChassisName             string            `json:"chassisName"`
	Priority                int               `json:"priority"`
	ExternalIDs             map[string]string `json:"externalIDs,omitempty"`
	HAChassisGroupNamespace string            `json:"haChassisGroupNamespace"`
	HAChassisGroupName      string            `json:"haChassisGroupName"`
}

type HAChassisStatus struct {
	UUID               string `json:"uuid,omitempty"`
	HAChassisGroupUUID string `json:"haChassisGroupUUID,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type HAChassisList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HAChassis `json:"items"`
}

// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="UUID",type=string,JSONPath=`.status.uuid`
// +kubebuilder:resource:scope=Namespaced
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type HAChassisGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HAChassisGroupSpec   `json:"spec"`
	Status HAChassisGroupStatus `json:"status,omitempty"`
}

type HAChassisGroupSpec struct {
	ExternalIDs map[string]string `json:"externalIDs,omitempty"`
}

type HAChassisGroupStatus struct {
	UUID string `json:"uuid,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type HAChassisGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HAChassisGroup `json:"items"`
}

// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type UserDefinedNetwork struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   UserDefinedNetworkSpec   `json:"spec"`
	Status UserDefinedNetworkStatus `json:"status,omitempty"`
}

type UserDefinedNetworkSpec struct {
	NetworkType            string `json:"networkType"` //underlay or overlay
	CIDR                   string `json:"cidr"`
	Gateway                string `json:"gateway"`
	LogicalSwitchName      string `json:"logicalSwitchName"`
	LogicalSwitchNamespace string `json:"logicalSwitchNamespace"`
}

type UserDefinedNetworkStatus struct {
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type UserDefinedNetworkList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []UserDefinedNetwork `json:"items"`
}
