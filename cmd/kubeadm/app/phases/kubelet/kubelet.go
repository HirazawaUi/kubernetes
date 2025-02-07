/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubelet

import (
	"fmt"
	"io"
	"text/template"

	"github.com/lithammer/dedent"
	"k8s.io/klog/v2"

	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/initsystem"
)

var (
	kubeletFailMsg = dedent.Dedent(`
	Unfortunately, an error has occurred, likely caused by:
		- The kubelet is not running
		- The kubelet is unhealthy due to a misconfiguration of the node in some way (required cgroups disabled)

	If you are on a systemd-powered system, you can try to troubleshoot the error with the following commands:
		- 'systemctl status kubelet'
		- 'journalctl -xeu kubelet'`)

	controlPlaneFailTempl = template.Must(template.New("init").Parse(dedent.Dedent(`
	Additionally, a control plane component may have crashed or exited when started by the container runtime.
	To troubleshoot, list all containers using your preferred container runtimes CLI.
	Here is one example how you may list all running Kubernetes containers by using crictl:
		- 'crictl --runtime-endpoint {{ .Socket }} ps -a | grep kube | grep -v pause'
		Once you have found the failing container, you can inspect its logs with:
		- 'crictl --runtime-endpoint {{ .Socket }} logs CONTAINERID'
`)))
)

// TryStartKubelet attempts to bring up kubelet service
func TryStartKubelet() {
	// If we notice that the kubelet service is inactive, try to start it
	initSystem, err := initsystem.GetInitSystem()
	if err != nil {
		fmt.Println("[kubelet-start] No supported init system detected, won't make sure the kubelet is running properly.")
		return
	}

	if !initSystem.ServiceExists(kubeadmconstants.Kubelet) {
		fmt.Println("[kubelet-start] Couldn't detect a kubelet service, can't make sure the kubelet is running properly.")
	}

	// This runs "systemctl daemon-reload && systemctl restart kubelet"
	if err := initSystem.ServiceRestart(kubeadmconstants.Kubelet); err != nil {
		klog.Warningf("[kubelet-start] WARNING: unable to start the kubelet service: [%v]\n", err)
		fmt.Printf("[kubelet-start] Please ensure kubelet is reloaded and running manually.\n")
	}
}

// PrintKubeletErrorHelpScreen prints help text on kubelet errors.
func PrintKubeletErrorHelpScreen(outputWriter io.Writer, criSocket string, waitControlPlaneComponents bool) {
	context := struct {
		Socket string
	}{
		Socket: criSocket,
	}

	fmt.Fprintln(outputWriter, kubeletFailMsg)
	if waitControlPlaneComponents {
		_ = controlPlaneFailTempl.Execute(outputWriter, context)
	}
	fmt.Println("")
}

// TryStopKubelet attempts to bring down the kubelet service momentarily
func TryStopKubelet() {
	// If we notice that the kubelet service is inactive, try to start it
	initSystem, err := initsystem.GetInitSystem()
	if err != nil {
		fmt.Println("[kubelet-start] No supported init system detected, won't make sure the kubelet not running for a short period of time while setting up configuration for it.")
		return
	}

	if !initSystem.ServiceExists(kubeadmconstants.Kubelet) {
		fmt.Println("[kubelet-start] Couldn't detect a kubelet service, can't make sure the kubelet not running for a short period of time while setting up configuration for it.")
	}

	// This runs "systemctl daemon-reload && systemctl stop kubelet"
	if err := initSystem.ServiceStop(kubeadmconstants.Kubelet); err != nil {
		klog.Warningf("[kubelet-start] WARNING: unable to stop the kubelet service momentarily: [%v]\n", err)
	}
}

// TryRestartKubelet attempts to restart the kubelet service
func TryRestartKubelet() {
	// If we notice that the kubelet service is inactive, try to start it
	initSystem, err := initsystem.GetInitSystem()
	if err != nil {
		fmt.Println("[kubelet-start] No supported init system detected, won't make sure the kubelet not running for a short period of time while setting up configuration for it.")
		return
	}

	if !initSystem.ServiceExists(kubeadmconstants.Kubelet) {
		fmt.Println("[kubelet-start] Couldn't detect a kubelet service, can't make sure the kubelet not running for a short period of time while setting up configuration for it.")
	}

	// This runs "systemctl daemon-reload && systemctl stop kubelet"
	if err := initSystem.ServiceRestart(kubeadmconstants.Kubelet); err != nil {
		klog.Warningf("[kubelet-start] WARNING: unable to restart the kubelet service momentarily: [%v]\n", err)
	}
}
