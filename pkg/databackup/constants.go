/*
Copyright 2021 The Fluid Authors.

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

package databackup

const (
	Finalizer            = "fluid-databackup-controller-finalizer"
	AlluxioBackupPathPod = "/alluxio_backups"
	DatabackupChart      = "fluid-databackup"

	BackupLocationPath     = "BackupLocationPath"
	BackupLocationNodeName = "BackupLocationNodeName"
)

// Keys persisted into DataBackup.status.infos as the terminal-state evidence of the
// backup pod. They are written atomically with the phase transition once the backup pod
// is observed in a terminal phase (Succeeded/Failed), so that later reconciles, controller
// restarts and pod garbage collection (Pod GC, manual deletion or owner cascading deletion)
// can still finish the phase, infos, dataset lock release, TTL and finalizer bookkeeping.
const (
	// BackupPodName is the name of the pod that executed the backup.
	BackupPodName = "BackupPodName"
	// BackupPodUID is the UID of the pod that executed the backup. It binds the persisted
	// evidence to a specific executor so that a same-name DataBackup (and its pod) recreation
	// cannot reuse stale evidence.
	BackupPodUID = "BackupPodUID"
	// BackupPodNodeName is the node the backup pod was scheduled to. local:// backup files
	// reside on that node, while pvc:// backups stay node-independent and record "NA" in
	// BackupLocationNodeName instead.
	BackupPodNodeName = "BackupPodNodeName"
	// BackupPodPhase is the observed terminal pod phase, either Succeeded or Failed.
	BackupPodPhase = "BackupPodPhase"
	// BackupPodStartTime and BackupPodFinishTime are RFC3339 timestamps observed from the
	// backup pod and allow the duration to be reconstructed after the pod is deleted.
	BackupPodStartTime  = "BackupPodStartTime"
	BackupPodFinishTime = "BackupPodFinishTime"
	// BackupPodMissingSince is an RFC3339 timestamp marking the first reconcile where the
	// backup pod was missing while no terminal state had ever been observed. It is transient:
	// removed when the pod reappears or when the DataBackup is marked failed after the grace
	// period.
	BackupPodMissingSince = "BackupPodMissingSince"
)

// Condition reasons used by the DataBackup controller.
const (
	// BackupPodMissingReason is used when the backup pod disappears before its terminal state
	// is ever observed. The DataBackup is marked failed with this reason instead of guessing
	// an unconfirmed successful result.
	BackupPodMissingReason = "BackupPodMissing"
)
