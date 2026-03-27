package workflow

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	workspacepb "go.temporal.io/api/workspace/v1"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

// EnsureWorkspaceForActivity ensures the workspace exists, creating it lazily
// on first use. Returns the workspace info for populating the activity task.
func EnsureWorkspaceForActivity(
	ms historyi.MutableState,
	workspaceID string,
) (*workspacepb.WorkspaceInfo, error) {
	executionInfo := ms.GetExecutionInfo()

	// Initialize workspace map if needed.
	if executionInfo.WorkspaceInfos == nil {
		executionInfo.WorkspaceInfos = make(map[string]*workspacepb.WorkspaceInfo)
	}

	ws, exists := executionInfo.WorkspaceInfos[workspaceID]
	if !exists {
		// Lazy creation: first activity referencing this workspace creates it.
		ws = &workspacepb.WorkspaceInfo{
			WorkspaceId:      workspaceID,
			CommittedVersion: 0,
		}
		executionInfo.WorkspaceInfos[workspaceID] = ws
	}

	return ws, nil
}

// ValidateWorkspaceForActivity checks that the workspace exists.
// Deprecated: Use EnsureWorkspaceForActivity instead.
func ValidateWorkspaceForActivity(
	ms historyi.MutableState,
	workspaceID string,
) (*workspacepb.WorkspaceInfo, error) {
	executionInfo := ms.GetExecutionInfo()
	if executionInfo.WorkspaceInfos == nil {
		return nil, workflow.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
			Message: fmt.Sprintf("workspace %q not found", workspaceID),
		}
	}

	ws, exists := executionInfo.WorkspaceInfos[workspaceID]
	if !exists {
		return nil, workflow.FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
			Message: fmt.Sprintf("workspace %q not found", workspaceID),
		}
	}

	return ws, nil
}

// normalizeAccessMode returns READ_WRITE for UNSPECIFIED, otherwise the mode as-is.
func normalizeAccessMode(mode enumspb.WorkspaceAccessMode) enumspb.WorkspaceAccessMode {
	if mode == enumspb.WORKSPACE_ACCESS_MODE_UNSPECIFIED {
		return enumspb.WORKSPACE_ACCESS_MODE_READ_WRITE
	}
	return mode
}

// ValidateWorkspaceAccess checks whether the requested access mode is compatible
// with the current workspace lock state. Returns an error if the workspace is
// in use by an incompatible activity. Call this BEFORE creating the scheduled event.
func ValidateWorkspaceAccess(
	ws *workspacepb.WorkspaceInfo,
	accessMode enumspb.WorkspaceAccessMode,
) error {
	mode := normalizeAccessMode(accessMode)

	switch mode {
	case enumspb.WORKSPACE_ACCESS_MODE_READ_WRITE:
		if ws.ActiveWriterScheduledId != 0 {
			return workflow.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
				Message: fmt.Sprintf("workspace %q is in use by writer activity (scheduled event ID %d)", ws.WorkspaceId, ws.ActiveWriterScheduledId),
			}
		}
		if len(ws.ActiveReaderScheduledIds) > 0 {
			return workflow.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
				Message: fmt.Sprintf("workspace %q is in use by %d reader activity(s)", ws.WorkspaceId, len(ws.ActiveReaderScheduledIds)),
			}
		}

	case enumspb.WORKSPACE_ACCESS_MODE_READ_ONLY:
		if ws.ActiveWriterScheduledId != 0 {
			return workflow.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
				Message: fmt.Sprintf("workspace %q is in use by writer activity (scheduled event ID %d)", ws.WorkspaceId, ws.ActiveWriterScheduledId),
			}
		}
	}

	return nil
}

// AcquireWorkspaceAccess records the activity's scheduled event ID in the
// workspace lock state. Call this AFTER the scheduled event has been created.
func AcquireWorkspaceAccess(
	ws *workspacepb.WorkspaceInfo,
	accessMode enumspb.WorkspaceAccessMode,
	scheduledEventID int64,
) {
	mode := normalizeAccessMode(accessMode)

	switch mode {
	case enumspb.WORKSPACE_ACCESS_MODE_READ_WRITE:
		ws.ActiveWriterScheduledId = scheduledEventID
	case enumspb.WORKSPACE_ACCESS_MODE_READ_ONLY:
		ws.ActiveReaderScheduledIds = append(ws.ActiveReaderScheduledIds, scheduledEventID)
	}
}

// ReleaseWorkspaceAccess removes the activity's scheduled event ID from the
// workspace lock state. Safe to call even if the activity doesn't hold a lock.
func ReleaseWorkspaceAccess(
	ms historyi.MutableState,
	workspaceID string,
	scheduledEventID int64,
) {
	if workspaceID == "" {
		return
	}
	executionInfo := ms.GetExecutionInfo()
	if executionInfo.WorkspaceInfos == nil {
		return
	}
	ws, exists := executionInfo.WorkspaceInfos[workspaceID]
	if !exists {
		return
	}

	if ws.ActiveWriterScheduledId == scheduledEventID {
		ws.ActiveWriterScheduledId = 0
	}

	readers := ws.ActiveReaderScheduledIds
	for i, id := range readers {
		if id == scheduledEventID {
			ws.ActiveReaderScheduledIds = append(readers[:i], readers[i+1:]...)
			break
		}
	}
}
