package parade

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jmoiron/sqlx"
	nanoid "github.com/matoous/go-nanoid"
	"github.com/treeverse/lakefs/logging"
)

type TaskID string

type ActorID string

type PerformanceToken pgtype.UUID

// nolint: stylecheck
func (dst *PerformanceToken) Scan(src interface{}) error {
	var scanned pgtype.UUID
	if err := scanned.Scan(src); err != nil {
		return err
	}
	*dst = PerformanceToken(scanned)
	return nil
}

func (src PerformanceToken) Value() (driver.Value, error) {
	return pgtype.UUID(src).Value()
}

func (src PerformanceToken) String() string {
	res := strings.Builder{}
	offset := 0
	addBytes := func(n int) {
		for i := 0; i < n; i++ {
			res.WriteString(fmt.Sprintf("%02x", src.Bytes[offset+i]))
		}
		offset += n
	}
	// nolint:gomnd
	addBytes(4)
	res.WriteString("-")
	// nolint:gomnd
	addBytes(2)
	res.WriteString("-")
	// nolint:gomnd
	addBytes(2)
	res.WriteString("-")
	// nolint:gomnd
	addBytes(2)
	res.WriteString("-")
	// nolint:gomnd
	addBytes(6)
	return res.String()
}

type TaskStatusCodeValue string

const (
	// TaskPending indicates a task is waiting for an actor to perform it (new or being
	// retried)
	TaskPending TaskStatusCodeValue = "pending"
	// IN_PROGRESS indicates a task is being performed by an actor.
	TaskInProgress TaskStatusCodeValue = "in-progress"
	// ABORTED indicates an actor has aborted this task with message, will not be reissued
	TaskAborted TaskStatusCodeValue = "aborted"
	// TaskCompleted indicates an actor has completed this task with message, will not reissued
	TaskCompleted TaskStatusCodeValue = "completed"
	// TaskInvalid is used by the API to report errors
	TaskInvalid TaskStatusCodeValue = "[invalid]"
)

// TaskData is a row in table "tasks".  It describes a task to perform.
type TaskData struct {
	ID     TaskID `db:"task_id"`
	Action string `db:"action"`
	// Body is JSON-formatted
	Body              *string           `db:"body"`
	Status            *string           `db:"status"`
	StatusCode        string            `db:"status_code"`
	NumTries          int               `db:"num_tries"`
	MaxTries          *int              `db:"max_tries"`
	TotalDependencies *int              `db:"total_dependencies"`
	ToSignal          []TaskID          `db:"to_signal"`
	ActorID           ActorID           `db:"actor_id"`
	ActionDeadline    *time.Time        `db:"action_deadline"`
	PerformanceToken  *PerformanceToken `db:"performance_token"`
	FinishChannelName *string           `db:"finish_channel"`
}

// TaskDataIterator implements the pgx.CopyFromSource interface and allows using CopyFrom to insert
// multiple TaskData rapidly.
type TaskDataIterator struct {
	Data []TaskData
	next int
	err  error
}

func (td *TaskDataIterator) Next() bool {
	if td.next > len(td.Data) {
		td.err = ErrNoMoreData
		return false
	}
	ret := td.next < len(td.Data)
	td.next++
	return ret
}

var ErrNoMoreData = errors.New("no more data")

func (td *TaskDataIterator) Err() error {
	return td.err
}

func (td *TaskDataIterator) Values() ([]interface{}, error) {
	if td.next > len(td.Data) {
		td.err = ErrNoMoreData
		return nil, td.err
	}
	value := td.Data[td.next-1]
	// Convert ToSignal to a text array so pgx can convert it to text.  Needed because Go
	// types.
	toSignal := make([]string, len(value.ToSignal))
	for i := 0; i < len(value.ToSignal); i++ {
		toSignal[i] = string(value.ToSignal[i])
	}
	return []interface{}{
		value.ID,
		value.Action,
		value.Body,
		value.Status,
		value.StatusCode,
		value.NumTries,
		value.MaxTries,
		value.TotalDependencies,
		toSignal,
		value.ActorID,
		value.ActionDeadline,
		value.PerformanceToken,
		value.FinishChannelName,
	}, nil
}

var TaskDataColumnNames = []string{
	"id", "action", "body", "status", "status_code", "num_tries", "max_tries",
	"total_dependencies", "to_signal", "actor_id", "action_deadline", "performance_token",
	"finish_channel",
}

var tasksTable = pgx.Identifier{"tasks"}

func InsertTasks(ctx context.Context, conn *pgx.Conn, source pgx.CopyFromSource) error {
	_, err := conn.CopyFrom(ctx, tasksTable, TaskDataColumnNames, source)
	return err
}

// OwnedTaskData is a row returned from "SELECT * FROM own_tasks(...)".
type OwnedTaskData struct {
	ID     TaskID           `db:"task_id"`
	Token  PerformanceToken `db:"token"`
	Action string           `db:"action"`
	Body   *string
}

// OwnTasks owns for actor and returns up to maxTasks tasks for performing any of actions.
func OwnTasks(conn *sqlx.DB, actor ActorID, maxTasks int, actions []string, maxDuration *time.Duration) ([]OwnedTaskData, error) {
	// Use sqlx.In to expand slice actions
	query, args, err := sqlx.In(`SELECT * FROM own_tasks(?, ARRAY[?], ?, ?)`, maxTasks, actions, actor, maxDuration)
	if err != nil {
		return nil, fmt.Errorf("expand own tasks query: %w", err)
	}
	query = conn.Rebind(query)
	rows, err := conn.Queryx(query, args...)
	if err != nil {
		return nil, fmt.Errorf("try to own tasks: %w", err)
	}
	tasks := make([]OwnedTaskData, 0, maxTasks)
	for rows.Next() {
		var task OwnedTaskData
		if err = rows.StructScan(&task); err != nil {
			return nil, fmt.Errorf("failed to scan row %+v: %w", rows, err)
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// ExtendTaskDeadline extends the deadline for completing taskID which was acquired with the
// specified token, for maxDuration longer.  It returns nil if the task is still owned and its
// deadline was extended, or an SQL error, or ErrInvalidToken.  deadline was extended.
func ExtendTaskDeadline(conn *sqlx.DB, taskID TaskID, token PerformanceToken, maxDuration time.Duration) error {
	var res *bool
	query, args, err := sqlx.In(`SELECT * FROM extend_task_deadline(?, ?, ?)`, taskID, token, maxDuration)
	if err != nil {
		return fmt.Errorf("create extend_task_deadline query: %w", err)
	}
	query = conn.Rebind(query)
	err = conn.Get(&res, query, args...)
	if err != nil {
		return fmt.Errorf("extend_task_deadline: %w", err)
	}

	if res == nil {
		return fmt.Errorf("extend task %s token %s for %s: %w",
			taskID, token, maxDuration, ErrInvalidToken)
	}
	return nil
}

// ReturnTask returns taskId which was acquired using the specified performanceToken, giving it
// resultStatus and resultStatusCode.  It returns ErrInvalidToken if the performanceToken is
// invalid; this happens when ReturnTask is called after its deadline expires, or due to a logic
// error.
func ReturnTask(conn *sqlx.DB, taskID TaskID, token PerformanceToken, resultStatus string, resultStatusCode TaskStatusCodeValue) error {
	var res int
	query, args, err := sqlx.In(`SELECT return_task(?, ?, ?, ?)`, taskID, token, resultStatus, resultStatusCode)
	if err != nil {
		return fmt.Errorf("create return_task query: %w", err)
	}
	query = conn.Rebind(query)
	err = conn.Get(&res, query, args...)
	if err != nil {
		return fmt.Errorf("return_task: %w", err)
	}

	if res != 1 {
		return fmt.Errorf("return task %s token %s with (%s, %s): %w",
			taskID, token, resultStatus, resultStatusCode, ErrInvalidToken)
	}

	return nil
}

// WaitForTask blocks until taskId ends, and returns its result status and status code.  It
// needs a pgx.Conn -- *not* a sqlx.Conn -- because it depends on PostgreSQL specific features.
func WaitForTask(ctx context.Context, conn *pgx.Conn, taskID TaskID) (resultStatus string, resultStatusCode TaskStatusCodeValue, err error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return "", TaskInvalid, fmt.Errorf("BEGIN: %w", err)
	}
	defer func() {
		if tx != nil {
			// No useful error handling for an error here, return value is
			// already out there.  Just log.
			if err := tx.Rollback(ctx); err != nil {
				logging.FromContext(ctx).Errorf("rollback after error: %s", err)
			}
		}
	}()
	row := tx.QueryRow(ctx, `SELECT finish_channel, status_code, status FROM tasks WHERE id=$1`, taskID)
	var (
		finishChannel sql.NullString
		statusCode    TaskStatusCodeValue = TaskInvalid
		status        sql.NullString      = sql.NullString{Valid: false}
	)
	err = row.Scan(&finishChannel, &statusCode, &status)
	if !status.Valid {
		status.String = "unknown"
	}
	if err != nil {
		return status.String, statusCode, fmt.Errorf("check task %s to listen: %w", taskID, err)
	}
	if !finishChannel.Valid {
		return status.String, statusCode, fmt.Errorf("cannot wait for task %s: %w", taskID, ErrNoFinishChannel)
	}
	if statusCode != TaskInProgress && statusCode != TaskPending {
		// Already done, no need to sleep.
		return status.String, statusCode, nil
	}

	if _, err = tx.Exec(ctx, "LISTEN "+pgx.Identifier{finishChannel.String}.Sanitize()); err != nil {
		return "", TaskInvalid, fmt.Errorf("listen for %s: %w", finishChannel.String, err)
	}

	_, err = conn.WaitForNotification(ctx)
	if err != nil {
		return "", TaskInvalid, fmt.Errorf("wait for notification %s: %w", finishChannel.String, err)
	}

	row = tx.QueryRow(ctx, `SELECT status, status_code FROM tasks WHERE id=$1`, taskID)
	err = row.Scan(&status, &statusCode)
	if err != nil {
		err = fmt.Errorf("query status for task %s: %w", taskID, err)
	}
	if err == nil {
		err = tx.Commit(ctx)
		tx = nil
	}
	return status.String, statusCode, err
}

// taskWithNewIterator is a pgx.CopyFromSource iterator that passes each task along with a
// 'new' copy status.
type taskWithNewIterator struct {
	tasks []TaskID
	idx   int
}

func (it *taskWithNewIterator) Next() bool {
	it.idx++
	return it.idx < len(it.tasks)
}

func (it *taskWithNewIterator) Values() ([]interface{}, error) {
	return []interface{}{it.tasks[it.idx], "new"}, nil
}

func (it *taskWithNewIterator) Err() error {
	return nil
}

func makeTaskWithNewIterator(tasks []TaskID) *taskWithNewIterator {
	return &taskWithNewIterator{tasks, -1}
}

// DeleteTasks deletes taskIds, removing dependencies and deleting (effectively recursively) any
// tasks that are left with no dependencies.  It creates a temporary table on tx, so ideally
// close the transaction shortly after.  The effect is easiest to analyze when all deleted tasks
// have been either completed or been aborted.
func DeleteTasks(ctx context.Context, tx pgx.Tx, taskIds []TaskID) error {
	uniqueID, err := nanoid.Nanoid()
	if err != nil {
		return fmt.Errorf("generate random component for table name: %w", err)
	}
	tableName := fmt.Sprintf("delete_tasks_%s", uniqueID)
	if _, err = tx.Exec(
		ctx,
		fmt.Sprintf(`CREATE TEMP TABLE "%s" (id VARCHAR(64), mark tasks_recurse_value NOT NULL) ON COMMIT DROP`, tableName),
	); err != nil {
		return fmt.Errorf("create temp work table %s: %w", tableName, err)
	}

	if _, err = tx.CopyFrom(ctx, pgx.Identifier{tableName}, []string{"id", "mark"}, makeTaskWithNewIterator(taskIds)); err != nil {
		return fmt.Errorf("COPY: %w", err)
	}
	_, err = tx.Exec(ctx, `SELECT delete_tasks($1)`, tableName)
	return err
}
