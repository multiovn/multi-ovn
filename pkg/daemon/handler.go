package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"strings"
	"time"

	multiovnv1 "github.com/multiovn/multi-ovn/pkg/apis/multiovn/v1"

	"github.com/multiovn/multi-ovn/pkg/request"

	clientset "github.com/multiovn/multi-ovn/pkg/client/clientset/versioned"

	"github.com/emicklei/go-restful"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"
)

const (
	MultiOVNNetworkAnnotation = "multiovn.io/pod-networks"
)

type CniServerHandler struct {
	Config       *Configuration
	KubeClient   kubernetes.Interface
	Crdclientset clientset.Interface
}

type Network struct {
	Name      string `json:"name"`
	Port      string `json:"port"`
	Namespace string `json:"namespace"`
}

func createCniServerHandler(config *Configuration) (*CniServerHandler, error) {
	csh := &CniServerHandler{KubeClient: config.KubeClient, Config: config, Crdclientset: config.Crdclientset}
	return csh, nil
}

func getPortInfoNameFromPodNetwork(pod *corev1.Pod, networkName string) (string, error) {
	if pod.Annotations == nil {
		return "", fmt.Errorf("pod annotations is empty")
	}

	networkStr, ok := pod.Annotations[MultiOVNNetworkAnnotation]
	if !ok {
		return "", fmt.Errorf("pod annotation %s not found", MultiOVNNetworkAnnotation)
	}

	var networks []Network
	if err := json.Unmarshal([]byte(networkStr), &networks); err != nil {
		return "", fmt.Errorf("unmarshal network annotation failed: %v", err)
	}

	for _, network := range networks {
		if network.Name == networkName {
			return network.Port, nil
		}
	}

	return "", fmt.Errorf("network %s not found", networkName)
}

func (csh CniServerHandler) handleAdd(req *restful.Request, resp *restful.Response) {
	podRequest := request.PodRequest{}
	err := req.ReadEntity(&podRequest)
	if err != nil {
		klog.Errorf("parse add request failed %v", err)
		resp.WriteHeaderAndEntity(http.StatusBadRequest, err)
		return
	}
	klog.Infof("add port request %v", podRequest)
	var macAddr, ipAddr, portName string
	found := false
	var routes []multiovnv1.Route
	for i := 0; i < 10; i++ {
		pod, err := csh.KubeClient.CoreV1().Pods(podRequest.PodNamespace).Get(context.Background(), podRequest.PodName, v1.GetOptions{})
		if err != nil {
			klog.Errorf("get pod %s/%s failed %v", podRequest.PodNamespace, podRequest.PodName, err)
			resp.WriteHeaderAndEntity(http.StatusInternalServerError, err)
			return
		}

		portInfoName, err := getPortInfoNameFromPodNetwork(pod, podRequest.NetworkName)
		if err != nil {
			klog.Errorf("get port %s failed %v", portInfoName, err)
			time.Sleep(2 * time.Second)
			continue
		}

		port, err := csh.Crdclientset.MultiovnV1().Ports(podRequest.PodNamespace).Get(context.Background(), portInfoName, v1.GetOptions{})
		if err != nil {
			klog.Errorf("get port %s failed %v", portInfoName, err)
			time.Sleep(2 * time.Second)
			continue
		}

		if port.Status.MacAddress == "" {
			klog.Errorf("port %s mac address is empty", portInfoName)
			time.Sleep(2 * time.Second)
			continue
		}

		logicalSwitchPort, err := csh.Crdclientset.MultiovnV1().LogicalSwitchPorts(podRequest.PodNamespace).Get(context.Background(), portInfoName, v1.GetOptions{})
		if err != nil || logicalSwitchPort.Status.UUID == "" {
			klog.Errorf("get logical switch port %s failed %v", portInfoName, err)
			time.Sleep(2 * time.Second)
			continue
		}

		if logicalSwitchPort.Spec.LogicalSwitchName != podRequest.LogicalSwitchName ||
			logicalSwitchPort.Spec.LogicalSwitchNamespace != podRequest.LogicalSwitchNamespace {
			klog.Errorf("logical switch port %s is not in the same logical switch %s/%s", portInfoName, podRequest.LogicalSwitchName, podRequest.LogicalSwitchNamespace)
			time.Sleep(2 * time.Second)
			continue
		}

		macAddr = port.Status.MacAddress
		ipAddr = port.Status.IPAddresses[0]
		routes = port.Status.Routes
		portName = fmt.Sprintf("%s-%s", port.Namespace, portInfoName)
		found = true
		break
	}

	if !found {
		klog.Errorf("failed to get port info %s", podRequest.NetworkName)
		resp.WriteHeaderAndEntity(http.StatusInternalServerError, fmt.Errorf("failed to get port %s", podRequest.NetworkName))
		return
	}

	klog.Infof("create container mac %s, ip %s", macAddr, ipAddr)
	err = csh.configureNic(podRequest.PodName, podRequest.PodNamespace, podRequest.NetNs, podRequest.ContainerID, macAddr, ipAddr, portName, podRequest.IfName, routes)
	if err != nil {
		klog.Errorf("configure nic failed %v", err)
		resp.WriteHeaderAndEntity(http.StatusInternalServerError, err)
		return
	}
	resp.WriteHeaderAndEntity(http.StatusOK, request.PodResponse{IpAddress: strings.Split(ipAddr, "/")[0], MacAddress: macAddr, CIDR: "0.0.0.0/0", Gateway: "0.0.0.0"})

}

func (csh CniServerHandler) handleDel(req *restful.Request, resp *restful.Response) {
	podRequest := request.PodRequest{}
	err := req.ReadEntity(&podRequest)
	if err != nil {
		klog.Errorf("parse del request failed %v", err)
		resp.WriteHeaderAndEntity(http.StatusBadRequest, err)
		return
	}
	klog.Infof("delete port request %v", podRequest)

	err = csh.deleteNic(podRequest.ContainerID, podRequest.IfName)
	if err != nil {
		klog.Errorf("del nic failed %v", err)
		resp.WriteHeaderAndEntity(http.StatusInternalServerError, err)
		return
	}
	resp.WriteHeader(http.StatusNoContent)
}
