package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"

	"github.com/containernetworking/cni/pkg/skel"
	"github.com/containernetworking/cni/pkg/types"
	current "github.com/containernetworking/cni/pkg/types/040"
	cniVersion "github.com/containernetworking/cni/pkg/version"
	"github.com/multiovn/multi-ovn/pkg/request"
)

func init() {
	// this ensures that main runs only on main thread (thread group leader).
	// since namespace ops (unshare, setns) are done for a single thread, we
	// must ensure that the goroutine does not jump from OS thread to thread
	runtime.LockOSThread()
}

type CNIStdinData struct {
	Name string `json:"name"`
}

func main() {

	skel.PluginMainFuncs(skel.CNIFuncs{
		Add: cmdAdd,
		Del: cmdDel,
	},
		cniVersion.PluginSupports("0.1.0", "0.2.0", "0.3.0", "0.3.1"),
		"NewOvn CNI plugin 0.0.1")
}

func cmdAdd(args *skel.CmdArgs) error {
	var err error
	str1 := fmt.Sprintf("cmdAdd args: %v\n", args.Args)
	str2 := fmt.Sprintf("cmdAdd args.StdinData: %v\n", string(args.StdinData))
	str3 := fmt.Sprintf("cmdAdd args.ContainerID: %v\n", args.ContainerID)
	str4 := fmt.Sprintf("cmdAdd args.Netns: %v\n", args.Netns)
	str5 := fmt.Sprintf("cmdAdd args.IfName: %v\n", args.IfName)

	os.WriteFile("/tmp/cni.log", []byte(str1+str2+str3+str4+str5), 0644)
	n, cniVersion, err := loadNetConf(args.StdinData)
	if err != nil {
		return err
	}

	podName, err := parseValueFromArgs("K8S_POD_NAME", args.Args)
	if err != nil {
		return err
	}
	podNamespace, err := parseValueFromArgs("K8S_POD_NAMESPACE", args.Args)
	if err != nil {
		return err
	}

	var cniStdinData CNIStdinData
	if err := json.Unmarshal(args.StdinData, &cniStdinData); err != nil {
		return fmt.Errorf("failed to unmarshal cni args: %v", err)
	}
	if cniStdinData.Name == "" {
		return fmt.Errorf("network name is required")
	}

	client := request.NewCniServerClient(n.ServerSocket)

	res, err := client.Add(request.PodRequest{
		PodName:                podName,
		PodNamespace:           podNamespace,
		ContainerID:            args.ContainerID,
		NetNs:                  args.Netns,
		IfName:                 args.IfName,
		NetworkName:            cniStdinData.Name,
		LogicalSwitchName:      n.LogicalSwitchName,
		LogicalSwitchNamespace: n.LogicalSwitchNamespace,
	})
	if err != nil {
		return err
	}
	result := generateCNIResult(cniVersion, res)
	return types.PrintResult(&result, cniVersion)
}

func generateCNIResult(cniVersion string, podResponse *request.PodResponse) current.Result {
	result := current.Result{CNIVersion: cniVersion}
	_, mask, _ := net.ParseCIDR(podResponse.CIDR)
	ip := current.IPConfig{
		Version: "4",
		Address: net.IPNet{IP: net.ParseIP(podResponse.IpAddress).To4(), Mask: mask.Mask},
		Gateway: net.ParseIP(podResponse.Gateway).To4(),
	}
	result.IPs = []*current.IPConfig{&ip}
	route := types.Route{}
	route.Dst = net.IPNet{IP: net.ParseIP("0.0.0.0").To4(), Mask: net.CIDRMask(0, 32)}
	route.GW = net.ParseIP(podResponse.Gateway).To4()
	result.Routes = []*types.Route{&route}
	return result
}

func cmdDel(args *skel.CmdArgs) error {
	n, _, err := loadNetConf(args.StdinData)
	if err != nil {
		return err
	}

	client := request.NewCniServerClient(n.ServerSocket)
	podName, err := parseValueFromArgs("K8S_POD_NAME", args.Args)
	if err != nil {
		return err
	}
	podNamespace, err := parseValueFromArgs("K8S_POD_NAMESPACE", args.Args)
	if err != nil {
		return err
	}

	return client.Del(request.PodRequest{
		PodName:      podName,
		PodNamespace: podNamespace,
		ContainerID:  args.ContainerID,
		NetNs:        args.Netns,
		IfName:       args.IfName})
}

type NetConf struct {
	types.NetConf
	ServerSocket           string `json:"serverSocket"`
	LogicalSwitchName      string `json:"logicalSwitchName"`
	LogicalSwitchNamespace string `json:"logicalSwitchNamespace"`
}

func loadNetConf(bytes []byte) (*NetConf, string, error) {
	n := &NetConf{}
	if err := json.Unmarshal(bytes, n); err != nil {
		return nil, "", fmt.Errorf("failed to load netconf: %v", err)
	}
	if n.ServerSocket == "" {
		return nil, "", fmt.Errorf("server_socket is required in cni.conf")
	}
	if n.LogicalSwitchName == "" {
		return nil, "", fmt.Errorf("logicalSwitchName is required in cni.conf")
	}
	if n.LogicalSwitchNamespace == "" {
		return nil, "", fmt.Errorf("logicalSwitchNamespace is required in cni.conf")
	}
	return n, n.CNIVersion, nil
}

func parseValueFromArgs(key, argString string) (string, error) {
	if argString == "" {
		return "", errors.New("CNI_ARGS is required")
	}
	args := strings.Split(argString, ";")
	for _, arg := range args {
		if strings.HasPrefix(arg, fmt.Sprintf("%s=", key)) {
			podName := strings.TrimPrefix(arg, fmt.Sprintf("%s=", key))
			if len(podName) > 0 {
				return podName, nil
			}
		}
	}
	return "", fmt.Errorf("%s is required in CNI_ARGS", key)
}
