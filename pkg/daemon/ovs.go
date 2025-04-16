package daemon

import (
	"fmt"
	"net"
	"os/exec"

	multiovnv1 "github.com/multiovn/multi-ovn/pkg/apis/multiovn/v1"

	"github.com/containernetworking/plugins/pkg/ns"
	"github.com/vishvananda/netlink"
	"k8s.io/klog"
)

func (csh CniServerHandler) configureNic(podName, podNamespace, netns, containerID, mac, ip, portName, ifName string, routes []multiovnv1.Route) error {
	klog.Infof("configure nic %s %s", containerID, ifName)
	var err error
	hostNicName, containerNicName := generateNicName(containerID, ifName)
	// Create a veth pair, put one end to container ,the other to ovs port
	// NOTE: DO NOT use ovs internal type interface for container.
	// Kubernetes will detect 'eth0' nic in pod, so the nic name in pod must be 'eth0'.
	// When renaming internal interface to 'eth0', ovs will delete and recreate this interface.
	veth := netlink.Veth{LinkAttrs: netlink.LinkAttrs{Name: hostNicName, MTU: 1400}, PeerName: containerNicName}
	defer func() {
		// Remove veth link in case any error during creating pod network.
		if err != nil {
			netlink.LinkDel(&veth)
		}
	}()
	err = netlink.LinkAdd(&veth)
	if err != nil {
		return fmt.Errorf("failed to crate veth for %s %v", podName, err)
	}

	// Add veth pair host end to ovs port
	output, err := exec.Command(
		"ovs-vsctl", "--may-exist", "add-port", "br-int", hostNicName, "--",
		"set", "interface", hostNicName, fmt.Sprintf("external_ids:iface-id=%s", portName)).CombinedOutput()
	if err != nil {
		klog.Info("podNamespace=" + podNamespace)
		return fmt.Errorf("add nic to ovs failed %v: %s", err, output)
	}

	// host and container nic must use same mac address, otherwise ovn will reject these packets by default
	macAddr, err := net.ParseMAC(mac)
	if err != nil {
		return fmt.Errorf("failed to parse mac %s %v", macAddr, err)
	}

	err = configureHostNic(hostNicName, macAddr)
	if err != nil {
		return err
	}

	podNS, err := ns.GetNS(netns)
	if err != nil {
		return fmt.Errorf("failed to open netns %q: %v", netns, err)
	}
	err = configureContainerNic(containerNicName, ip, macAddr, podNS, ifName, routes)
	if err != nil {
		return err
	}

	return nil
}

func (csh CniServerHandler) deleteNic(containerID, ifName string) error {
	klog.Infof("delete nic %s %s", containerID, ifName)
	hostNicName, _ := generateNicName(containerID, ifName)
	// Remove ovs port
	output, err := exec.Command("ovs-vsctl", "--if-exists", "--with-iface", "del-port", "br-int", hostNicName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to delete ovs port %v, %s", err, output)
	}

	hostLink, err := netlink.LinkByName(hostNicName)
	if err != nil {
		// If link already not exists, return quietly
		if _, ok := err.(netlink.LinkNotFoundError); ok {
			return nil
		}
		return fmt.Errorf("find host link %s failed %v", hostNicName, err)
	}
	err = netlink.LinkDel(hostLink)
	if err != nil {
		return fmt.Errorf("delete host link %s failed %v", hostLink, err)
	}
	return nil
}

// 使用containerID的前9位和ifName的前5位生成hostNicName和containerNicName
func generateNicName(containerID, ifName string) (string, string) {
	return fmt.Sprintf("%s%sh", containerID[0:9], truncateString(ifName, 5)), fmt.Sprintf("%s%sc", containerID[0:9], truncateString(ifName, 5))
}

func truncateString(str string, n int) string {
	if len(str) <= n {
		return str
	}
	return str[0:n]
}

func configureHostNic(nicName string, macAddr net.HardwareAddr) error {
	hostLink, err := netlink.LinkByName(nicName)
	if err != nil {
		return fmt.Errorf("can not find host nic %s %v", nicName, err)
	}

	err = netlink.LinkSetHardwareAddr(hostLink, macAddr)
	if err != nil {
		return fmt.Errorf("can not set mac address to host nic %s %v", nicName, err)
	}
	err = netlink.LinkSetUp(hostLink)
	if err != nil {
		return fmt.Errorf("can not set host nic %s up %v", nicName, err)
	}
	return nil
}

func configureContainerNic(nicName, ipAddr string, macAddr net.HardwareAddr, netns ns.NetNS, ifName string, routes []multiovnv1.Route) error {
	containerLink, err := netlink.LinkByName(nicName)
	if err != nil {
		return fmt.Errorf("can not find container nic %s %v", nicName, err)
	}

	err = netlink.LinkSetNsFd(containerLink, int(netns.Fd()))
	if err != nil {
		return fmt.Errorf("failed to link netns %v", err)
	}

	// TODO: use github.com/containernetworking/plugins/pkg/ipam.ConfigureIface to refactor this logical
	return ns.WithNetNSPath(netns.Path(), func(_ ns.NetNS) error {
		err = netlink.LinkSetName(containerLink, ifName)
		if err != nil {
			return err
		}
		addr, err := netlink.ParseAddr(ipAddr)
		if err != nil {
			return fmt.Errorf("can not parse %s %v", ipAddr, err)
		}
		err = netlink.AddrAdd(containerLink, addr)
		if err != nil {
			return fmt.Errorf("can not add address to container nic %v", err)
		}

		err = netlink.LinkSetHardwareAddr(containerLink, macAddr)
		if err != nil {
			return fmt.Errorf("can not set mac address to container nic %v", err)
		}
		err = netlink.LinkSetUp(containerLink)
		if err != nil {
			return fmt.Errorf("can not set container nic %s up %v", nicName, err)
		}

		for _, route := range routes {
			klog.Infof("add route %s %s", route.Destination, route.NextHop)
			_, dst, err := net.ParseCIDR(route.Destination)
			if err != nil {
				klog.Infof("add route %s %s", route.Destination, route.NextHop)
				return fmt.Errorf("can not parse %s %v", route.Destination, err)
			}

			ip := net.ParseIP(route.NextHop)
			if ip == nil {
				return fmt.Errorf("can not parse %s %v", route.NextHop, err)
			}
			err = netlink.RouteReplace(&netlink.Route{
				LinkIndex: containerLink.Attrs().Index,
				Dst:       dst,
				Gw:        ip,
			})
			if err != nil {
				return fmt.Errorf("config gateway failed %v", err)
			}
		}

		return nil
	})
}
