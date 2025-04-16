package daemon

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/multiovn/multi-ovn/pkg/request"

	"github.com/emicklei/go-restful"
	"k8s.io/klog"
)

var RequestLogString = "[%s] Incoming %s %s %s request from %s"
var ResponseLogString = "[%s] Outcoming response to %s %s %s with %d status code in %vms"

func RunServer(config *Configuration) {
	csh, err := createCniServerHandler(config)
	if err != nil {
		klog.Fatalf("create cni server handler failed %v", err)
		return
	}
	server := http.Server{
		Handler: createHandler(csh),
	}
	if _, err := os.Stat(config.BindSocket); err == nil {
		if err := os.Remove(config.BindSocket); err != nil {
			klog.Errorf("failed to remove existing socket file %s: %v", config.BindSocket, err)
			return
		}
	}
	unixListener, err := net.Listen("unix", config.BindSocket)
	if err != nil {
		klog.Errorf("bind socket to %s failed %v", config.BindSocket, err)
		return
	}
	defer os.Remove(config.BindSocket)
	klog.Infof("start listen on %s", config.BindSocket)
	klog.Fatal(server.Serve(unixListener))
}

func createHandler(csh *CniServerHandler) http.Handler {
	wsContainer := restful.NewContainer()
	wsContainer.EnableContentEncoding(true)

	ws := new(restful.WebService)
	ws.Path("/api/v1").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)
	wsContainer.Add(ws)

	ws.Route(
		ws.POST("/add").
			To(csh.handleAdd).
			Reads(request.PodRequest{}))
	ws.Route(
		ws.POST("/del").
			To(csh.handleDel).
			Reads(request.PodRequest{}))

	ws.Filter(requestAndResponseLogger)

	return wsContainer
}

// web-service filter function used for request and response logging.
func requestAndResponseLogger(request *restful.Request, response *restful.Response, chain *restful.FilterChain) {
	klog.Infoln(formatRequestLog(request))
	start := time.Now()
	chain.ProcessFilter(request, response)
	elapsed := float64((time.Since(start)) / time.Millisecond)
	klog.Infoln(formatResponseLog(response, request, elapsed))
}

// formatRequestLog formats request log string.
func formatRequestLog(request *restful.Request) string {
	uri := ""
	if request.Request.URL != nil {
		uri = request.Request.URL.RequestURI()
	}

	return fmt.Sprintf(RequestLogString, time.Now().Format(time.RFC3339), request.Request.Proto,
		request.Request.Method, uri, request.Request.RemoteAddr)
}

// formatResponseLog formats response log string.
func formatResponseLog(response *restful.Response, request *restful.Request, reqTime float64) string {
	uri := ""
	if request.Request.URL != nil {
		uri = request.Request.URL.RequestURI()
	}
	return fmt.Sprintf(ResponseLogString, time.Now().Format(time.RFC3339),
		request.Request.RemoteAddr, request.Request.Method, uri, response.StatusCode(), reqTime)
}
