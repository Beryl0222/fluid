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
	// BackupExecutionJob records the job name of a cron backup round for diagnosis
	BackupExecutionJob = "BackupExecutionJob"
	// BackupExecutionPod records the pod name of the current (or last) backup round for diagnosis
	BackupExecutionPod = "BackupExecutionPod"

	// ExecutionIDAnnotation is stamped onto the Job (and propagates to Pods) of each cron
	// backup round to make the scheduled execution generation recognizable from Kubernetes objects.
	ExecutionIDAnnotation = "databackup.fluid.io/execution-id"

	// CronJobLabel is the label carried by the Job/Pod of a cron DataBackup, its value is the CronJob name.
	// It shares the same key used by other cron data operations so jobs can be listed generically.
	CronJobLabel = "cronjob"

	// ReasonInvalidCronSchedule is the condition reason when the cron schedule is missing or invalid.
	ReasonInvalidCronSchedule = "InvalidCronSchedule"
	// ReasonBackupMissed is the condition reason recorded when a scheduled round is skipped
	// because it is older than the missed schedule threshold.
	ReasonBackupMissed = "BackupScheduleMissed"
)
