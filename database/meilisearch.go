package database

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/valyala/fasthttp"
	"path"
	"strconv"
	"time"

	"github.com/huacnlee/gobackup/logger"
)

// Meilisearch database
//
// type: Meilisearch
// api_url: 127.0.0.1:7700
// master_key:
// backups_path:
type Meilisearch struct {
	Base
	apiUrl      string
	masterKey   string
	backupsPath string
}

func (db *Meilisearch) perform() (err error) {
	viper := db.viper
	viper.SetDefault("api_url", "127.0.0.1:7700")

	db.apiUrl = viper.GetString("api_url")
	db.masterKey = viper.GetString("master_key")
	db.backupsPath = viper.GetString("backups_path")

	err = db.dump()
	if err != nil {
		return err
	}
	return nil
}

// taskStatus is the status of a task.
type taskStatus string

const (
	// TaskStatusUnknown is the default taskStatus, should not exist
	taskStatusUnknown taskStatus = "unknown"
	// TaskStatusEnqueued the task request has been received and will be processed soon
	taskStatusEnqueued taskStatus = "enqueued"
	// TaskStatusProcessing the task is being processed
	taskStatusProcessing taskStatus = "processing"
	// TaskStatusSucceeded the task has been successfully processed
	taskStatusSucceeded taskStatus = "succeeded"
	// TaskStatusFailed a failure occurred when processing the task, no changes were made to the database
	taskStatusFailed taskStatus = "failed"
)

type createDumpResp struct {
	Status  taskStatus `json:"status"`
	TaskUID int64      `json:"taskUid,omitempty"`
}

type taskDetails struct {
	DumpUid string `json:"dumpUid,omitempty"`
}

type taskResp struct {
	Status  taskStatus  `json:"status"`
	TaskUID int64       `json:"taskUid,omitempty"`
	Details taskDetails `json:"details,omitempty"`
}

func (db *Meilisearch) dump() error {
	localLog := logger.Tag("Meilisearch")
	resp, err := fetchJson[createDumpResp]("POST", fullUrl(db.apiUrl, "/dumps"), db.masterKey)
	if err != nil {
		return err
	}

	localLog.Info("-> creating backup")
	taskUid := resp.TaskUID
	task, err := db.getTask(taskUid)
	if err != nil {
		return err
	}

	localLog.Info("-> waiting for completion")
	const maxAttempts = 10
	attemptNo := 0
	for task.Status != taskStatusSucceeded && task.Status != taskStatusFailed {
		if attemptNo >= maxAttempts {
			return fmt.Errorf("-> Failed to fetch backup task info: : %s", err)
		}

		time.Sleep(time.Millisecond * 200)
		task, err = db.getTask(taskUid)
		if err != nil {
			attemptNo++
			localLog.Warn(fmt.Sprintf("-> (%d) Failure on fetching backup task info: %s", attemptNo, err))
			continue
		}

		attemptNo = 0
	}

	localLog.Info("-> completed")
	dumpId := task.Details.DumpUid
	db.dumpPath = path.Join(db.backupsPath, dumpId+".dump")

	if err != nil {
		return fmt.Errorf("-> Dump error: %s", err)
	}

	localLog.Info("dump path:", db.dumpPath)
	return nil
}

func (db *Meilisearch) getTask(taskUid int64) (*taskResp, error) {
	return fetchJson[taskResp]("GET", fullUrl(db.apiUrl, "/tasks/"+strconv.Itoa(int(taskUid))), db.masterKey)
}

func fullUrl(host, path string) string {
	if path[0] != '/' {
		path = "/" + path
	}

	return host + path
}

func fetchJson[T any](method, url, masterKey string) (*T, error) {
	rawResp, err := fetch(method, url, masterKey)

	resp := new(T)
	if err = json.Unmarshal(rawResp, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

func fetch(method, url, masterKey string) ([]byte, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(url)
	req.Header.SetMethod(method)
	req.Header.SetContentType("application/json")
	if len(masterKey) > 0 {
		req.Header.Set("Authorization", masterKey)
	}

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	err := fasthttp.Do(req, resp)
	if err != nil {
		return nil, fmt.Errorf("Client get failed: %s\n", err)
	}
	if resp.StatusCode() >= 300 {
		return nil, fmt.Errorf("Expected status code %d but got %d\n", fasthttp.StatusOK, resp.StatusCode())
	}

	contentEncoding := resp.Header.Peek("Content-Encoding")
	var body []byte
	if bytes.EqualFold(contentEncoding, []byte("gzip")) {
		body, _ = resp.BodyGunzip()
	} else {
		body = resp.Body()
	}

	return body, nil
}
