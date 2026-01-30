package wlImpl

import (
	"context"
	"fmt"
	"math"
	stdpath "path"
	"path/filepath"
	"strings"
	"time"

	"github.com/alist-org/alist/v3/internal/fs"
	"github.com/alist-org/alist/v3/internal/model"
	"github.com/alist-org/alist/v3/internal/op"
	"github.com/alist-org/alist/v3/internal/stream"
	"github.com/alist-org/alist/v3/internal/task"
	"github.com/alist-org/alist/v3/pkg/utils"
	"github.com/alist-org/alist/v3/server/common"
	"github.com/alist-org/alist/v3/server/handles"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/gin-gonic/gin"
	"github.com/volcengine/ve-tos-golang-sdk/v2/tos"
	"github.com/xhofe/tache"
)

var CloudTaskManager *tache.Manager[*CloudTask]

const (
	MaxKeys = 10
)

type CloudTask struct {
	task.TaskExtension
	ID       string         `json:"id"`
	TaskInfo *CloudTaskInfo `json:"task_info"`

	State         tache.State `json:"state"`
	Status        string      `json:"status"`
	Progress      float64     `json:"progress"`
	StartTime     *time.Time  `json:"start_time"`
	EndTime       *time.Time  `json:"end_time"`
	ProcFileCount int64       `json:"proc_file_count"`
	TotalBytes    int64       `json:"total_bytes"`
	Error         string      `json:"error"`

	uploadTaskMap map[string]task.TaskExtensionInfo `json:"-"`
	startAfter    string
	stop          bool
	Stoped        bool
	running       bool
	failRetry     int
}

func (t *CloudTask) GetName() string {
	return fmt.Sprintf("%s:%s:%s", t.TaskInfo.CloudType, t.TaskInfo.BucketName, t.TaskInfo.Prefix)
}
func (t *CloudTask) GetStatus() string {
	if t.Status != "" {
		return t.Status
	}
	if t.Stoped {
		return "Stoped"
	}
	if t.stop && !t.Stoped {
		return "Stopping"
	}
	if t.EndTime == nil {
		return "uploading"
	} else {
		return "finish"
	}
}
func (t *CloudTask) GetStartTime() *time.Time {
	return t.StartTime
}
func (t *CloudTask) GetEndTime() *time.Time {
	return t.EndTime
}
func (t *CloudTask) GetTotalBytes() int64 {
	return t.TotalBytes
}

func (t *CloudTask) Stop() {
	t.stop = true
	defer func() {
		if !t.Stoped {
			t.Stoped = true
		}
		t.Persist()
	}()

	if t.running {
		for {
			if t.Stoped {
				break
			}
			time.Sleep(1 * time.Second)
			utils.Log.Info("stopping sync cloud task: ", t.GetName())
		}
	}

	uploadTasks := fs.UploadTaskManager.GetAll()
	for _, task := range uploadTasks {
		if _, ok := t.uploadTaskMap[task.ID]; ok {
			fs.UploadTaskManager.Remove(task.ID)
			delete(t.uploadTaskMap, task.ID)
		}
	}
	utils.Log.Info("stoped sync cloud task: ", t.GetName())
}

func (t *CloudTask) Run() error {
	t.startAfter = ""
	if t.uploadTaskMap == nil {
		t.uploadTaskMap = map[string]task.TaskExtensionInfo{}
	}
	if t.StartTime == nil {
		startTime := time.Now()
		t.StartTime = &startTime
	}
	if t.EndTime != nil {
		return nil
	}

	persistTicker := time.NewTicker(4 * time.Second)

	go func() {
		for range persistTicker.C {
			t.Persist()
		}
	}()

	if t.Stoped {
		return nil
	}

	defer func() {
		persistTicker.Stop()
		t.running = false
		t.Persist()
	}()

	t.running = true
	var err error
	for {
		if t.stop {
			break
		}
		time.Sleep(1 * time.Second)
		remainTaskCount := t.clearUploadTask()
		if remainTaskCount > 10 {
			t.Status = fmt.Sprintf("Wait upload task to many:%d", remainTaskCount)
			continue
		} else {
			t.Status = ""
		}
		if len(t.uploadTaskMap) > 0 {
			t.Status = fmt.Sprintf("Wait exist not finish upload task:%d", remainTaskCount)
			continue
		} else {
			t.Status = ""
		}

		if t.startAfter != "" {
			t.TaskInfo.StartAfter = t.startAfter
		}

		if t.TaskInfo.CloudType == "oss" {
			t.running, err = t.procOss()
			if err != nil {
				return err
			}
			if !t.running {
				break
			}
		}
		if t.TaskInfo.CloudType == "tos" {
			t.running, err = t.procTos()
			if err != nil {
				return err
			}
			if !t.running {
				break
			}
		}

		if !t.running {
			break
		}
	}
	if t.stop {
		t.Stoped = true
		return nil
	}
	endTime := time.Now()
	t.EndTime = &endTime
	return nil
}

func (t *CloudTask) procOss() (bool, error) {
	start := time.Now()
	var procSize int64
	fmt.Println("=================================procOss start ")
	defer func() {
		rate := float64(procSize) / 1024 / 1024 / time.Since(start).Seconds()
		fmt.Println("=================================procOss end ", time.Since(start).Seconds(),
			", rate: ", rate, "M/s")
		t.TaskInfo.Rate = rate
	}()
	client := newOssClient(t.TaskInfo)

	listResult, err := client.ListObjectsV2(context.Background(), &oss.ListObjectsV2Request{
		Bucket:     &t.TaskInfo.BucketName,
		Prefix:     &t.TaskInfo.Prefix,
		MaxKeys:    MaxKeys,
		StartAfter: &t.TaskInfo.StartAfter,
	})
	if err != nil {
		err = fmt.Errorf("procOss ListObjectsV2 error: %s ", err.Error())
		utils.Log.Error(err)
		return true, err
	}

	if len(listResult.Contents) == 0 {
		return false, nil
	}

	for _, objInfo := range listResult.Contents {
		if t.stop {
			break
		}
		if !strings.HasSuffix(*objInfo.Key, "/") {
			clouldObj, err := client.GetObject(context.Background(), &oss.GetObjectRequest{
				Bucket: &t.TaskInfo.BucketName,
				Key:    objInfo.Key,
			})
			if err != nil {
				err = fmt.Errorf("procOss GetObjectV2 error: %s", err.Error())
				utils.Log.Error(err)
				return true, err
			}

			size := clouldObj.ContentLength
			h := make(map[*utils.HashType]string)
			if clouldObj.ContentMD5 != nil {
				h[utils.MD5] = *clouldObj.ContentMD5
			}

			mimetype := ""
			if clouldObj.ContentType != nil {
				mimetype = *clouldObj.ContentType
			}
			if len(mimetype) == 0 {
				mimetype = utils.GetMimeType(*objInfo.Key)
			}

			asTask := true
			fullPath := filepath.Join(t.TaskInfo.DstDir, *objInfo.Key)
			dir, name := stdpath.Split(fullPath)
			ctx := context.WithValue(context.Background(), "user", t.Creator)
			s := &stream.FileStream{
				Obj: &model.Object{
					Name:     name,
					Size:     size,
					Modified: *objInfo.LastModified,
					HashInfo: utils.NewHashInfoByMap(h),
				},
				Reader:       clouldObj.Body,
				Mimetype:     mimetype,
				WebPutAsTask: asTask,
			}

			uploadTask, err := fs.PutAsTask(ctx, dir, s)
			if err != nil {
				err = fmt.Errorf("procOss PutAsTask error: %s", err.Error())
				utils.Log.Error(err)
				return true, err
			}

			t.uploadTaskMap[uploadTask.GetID()] = uploadTask

			t.ProcFileCount++
			t.TotalBytes += objInfo.Size
			procSize += objInfo.Size
		}
	}

	t.startAfter = *listResult.Contents[listResult.KeyCount-1].Key

	return true, nil
}

func (t *CloudTask) clearUploadTask() int {
	count := 0
	failCount := 0
	uploadTasks := fs.UploadTaskManager.GetAll()
	for _, task := range uploadTasks {
		/*
			    manager.RemoveByCondition(func(task T) bool {
						return (isAdmin || uid == task.GetCreator().ID) && task.GetState() == tache.StateSucceeded
					})
		*/
		if task.GetState() == tache.StateSucceeded {
			if _, ok := t.uploadTaskMap[task.ID]; ok {
				fs.UploadTaskManager.Remove(task.ID)
				delete(t.uploadTaskMap, task.ID)
			}
		} else {
			count++
		}
		if task.GetState() == tache.StateFailed {
			failCount++
		}
	}
	if failCount > 0 {
		if count == failCount {

			for _, task := range uploadTasks {
				if _, ok := t.uploadTaskMap[task.ID]; ok {
					fs.UploadTaskManager.Remove(task.ID)
					delete(t.uploadTaskMap, task.ID)
				}
			}
			t.startAfter = t.TaskInfo.StartAfter
			t.Status = fmt.Sprintf("fail retry: %d", t.failRetry)
			t.failRetry++
			time.Sleep(time.Duration(5*t.failRetry) * time.Second)
		}
	} else {
		t.failRetry = 1
	}

	return count
}

func (t *CloudTask) procTos() (bool, error) {
	start := time.Now()
	var procSize int64
	fmt.Println("=================================procTos start ")
	defer func() {
		rate := float64(procSize) / 1024 / 1024 / time.Since(start).Seconds()
		fmt.Println("=================================procTos end ", time.Since(start).Seconds(),
			", rate: ", rate, "M/s")
		t.TaskInfo.Rate = rate
	}()
	client, err := newTosClient(t.TaskInfo)
	if err != nil {
		return true, err
	}
	defer client.Close()
	listInput := &tos.ListObjectsV2Input{
		Bucket: t.TaskInfo.BucketName,
	}
	listInput.Prefix = t.TaskInfo.Prefix
	listInput.Marker = t.TaskInfo.StartAfter
	listInput.MaxKeys = MaxKeys

	listResult, err := client.ListObjectsV2(context.Background(), listInput)
	if err != nil {
		err = fmt.Errorf("procTos ListObjectsV2 error: %s ", err.Error())
		utils.Log.Error(err)
		return true, err
	}

	for _, objInfo := range listResult.Contents {
		if t.stop {
			break
		}
		if !strings.HasSuffix(objInfo.Key, "/") {
			clouldObj, err := client.GetObjectV2(context.Background(), &tos.GetObjectV2Input{
				Bucket: t.TaskInfo.BucketName,
				Key:    objInfo.Key,
			})
			if err != nil {
				err = fmt.Errorf("tos GetObjectV2 error: %s", err.Error())
				utils.Log.Error(err)
				return true, err
			}

			size := clouldObj.ContentLength
			h := make(map[*utils.HashType]string)
			if clouldObj.ETag != "" {
				h[utils.MD5] = clouldObj.ETag
			}

			mimetype := ""
			if clouldObj.ContentType != "" {
				mimetype = clouldObj.ContentType
			}
			if len(mimetype) == 0 {
				mimetype = utils.GetMimeType(objInfo.Key)
			}

			asTask := true
			fullPath := filepath.Join(t.TaskInfo.DstDir, objInfo.Key)
			dir, name := stdpath.Split(fullPath)
			ctx := context.WithValue(context.Background(), "user", t.Creator)
			s := &stream.FileStream{
				Obj: &model.Object{
					Name:     name,
					Size:     size,
					Modified: objInfo.LastModified,
					HashInfo: utils.NewHashInfoByMap(h),
				},
				Reader:       clouldObj.Content,
				Mimetype:     mimetype,
				WebPutAsTask: asTask,
			}

			uploadTask, err := fs.PutAsTask(ctx, dir, s)
			if err != nil {
				err = fmt.Errorf("tos PutAsTask error: %s", err.Error())
				utils.Log.Error(err)
				return true, err
			}
			t.uploadTaskMap[uploadTask.GetID()] = uploadTask
			t.ProcFileCount++
			t.TotalBytes += objInfo.Size
			procSize += objInfo.Size
		}
	}

	if listResult.NextMarker == "" {
		return false, nil
	}

	t.startAfter = listResult.NextMarker

	return true, nil
}

type CloudTaskInfo struct {
	Name       string `json:"name"`
	CloudType  string `json:"cloud_type"`
	AccessKey  string `json:"access_key"`
	SecretKey  string `json:"secret_key"`
	BucketName string `json:"bucket_name"`
	Prefix     string `json:"prefix"`
	Endpoint   string `json:"endpoint"`
	Region     string `json:"region"`
	StartAfter string `json:"start_after"`
	MaxKeys    int    `json:"max_keys"`
	DstDir     string `json:"dst_dir"`

	TaskInfo      *handles.TaskInfo `json:"proc_info"`
	ProcFileCount int64             `json:"proc_file_count"`
	Rate          float64           `json:"rate"`
}

func FsCloudCreate(c *gin.Context) {
	req := &CloudTaskInfo{}
	if err := c.ShouldBind(req); err != nil {
		utils.Log.Error(err)
		common.ErrorResp(c, err, 400)
		return
	}

	storagePath := req.DstDir
	storagePath, _ = strings.CutPrefix(req.DstDir, "/")
	storagePath = storagePath[:strings.Index(storagePath, "/")]
	storagePath = "/" + storagePath
	_, _, err := op.GetStorageAndActualPath(storagePath)
	storage := op.GetBalancedStorage(storagePath)
	if storage == nil {
		utils.Log.Errorf("GetStorage(%s) err: %s", storagePath, err.Error())
		common.ErrorResp(c, err, 400)
		return
	}

	err = checkCloudConfig(req)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}

	// taskCreator, _ := c.Value("user").(*model.User) // taskCreator is nil when convert failed
	taskCreator, err := op.GetUserByName("admin")
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	t := &CloudTask{
		TaskExtension: task.TaskExtension{
			Creator: taskCreator,
		},
		TaskInfo: req,
	}

	tasks := CloudTaskManager.GetAll()
	for _, task := range tasks {
		if task.GetName() == t.GetName() {
			err = fmt.Errorf("exist task: %s", task.GetName())
			common.ErrorResp(c, err, 400)
			return
		}
	}

	CloudTaskManager.Add(t)
	common.SuccessResp(c)
}

func FsCloudList(c *gin.Context) {
	cloudTasks := CloudTaskManager.GetAll()

	taskInfos := []*CloudTaskInfo{}

	for _, ct := range cloudTasks {
		procInfo := getTaskInfo(ct)
		taskInfo := ct.TaskInfo
		taskInfo.TaskInfo = &procInfo
		taskInfo.ProcFileCount = ct.ProcFileCount
		taskInfos = append(taskInfos, taskInfo)
	}

	common.SuccessResp(c, gin.H{
		"tasks": taskInfos,
	})
}

func FsCloudDelete(c *gin.Context) {
	req := &struct {
		ID string `json:"id"`
	}{}
	if err := c.ShouldBind(req); err != nil {
		utils.Log.Error(err)
		common.ErrorResp(c, err, 400)
		return
	}

	task, ok := CloudTaskManager.GetByID(req.ID)
	if ok {
		task.Stop()
	}
	CloudTaskManager.Remove(req.ID)
	common.SuccessResp(c)
}

func FsCloudStop(c *gin.Context) {
	req := &struct {
		ID string `json:"id"`
	}{}
	if err := c.ShouldBind(req); err != nil {
		utils.Log.Error(err)
		common.ErrorResp(c, err, 400)
		return
	}

	task, ok := CloudTaskManager.GetByID(req.ID)
	if ok {
		task.Stop()
	}
	common.SuccessResp(c)
}

func FsCloudStart(c *gin.Context) {
	req := &struct {
		ID string `json:"id"`
	}{}
	if err := c.ShouldBind(req); err != nil {
		utils.Log.Error(err)
		common.ErrorResp(c, err, 400)
		return
	}

	task, ok := CloudTaskManager.GetByID(req.ID)
	if ok {
		if task.Stoped {
			task.Stoped = false
			task.stop = false
			go task.Run()
		}
	}
	common.SuccessResp(c)
}

func newOssClient(info *CloudTaskInfo) *oss.Client {
	endpoint := info.Endpoint
	region := info.Region

	client := oss.NewClient(&oss.Config{
		Endpoint:            &endpoint,
		CredentialsProvider: credentials.NewStaticCredentialsProvider(info.AccessKey, info.SecretKey),
		Region:              &region,
	})
	return client
}

func checkCloudConfig(info *CloudTaskInfo) error {
	switch info.CloudType {
	case "oss":
		client := newOssClient(info)
		_, err := client.ListObjectsV2(context.Background(), &oss.ListObjectsV2Request{
			Bucket:  &info.BucketName,
			Prefix:  &info.Prefix,
			MaxKeys: MaxKeys,
		})
		if err != nil {
			return err
		}
	case "tos":
		client, err := newTosClient(info)
		if err != nil {
			return err
		}
		listInput := &tos.ListObjectsV2Input{
			Bucket: info.BucketName,
		}
		listInput.Prefix = info.Prefix
		listInput.Marker = info.StartAfter
		listInput.MaxKeys = MaxKeys

		_, err = client.ListObjectsV2(context.Background(), listInput)
		if err != nil {
			return err
		}
	default:
		err := fmt.Errorf("unsupport cloud type: %s", info.CloudType)
		return err
	}
	return nil
}

func newTosClient(info *CloudTaskInfo) (*tos.ClientV2, error) {
	client, err := tos.NewClientV2(info.Endpoint, tos.WithRegion(info.Region),
		tos.WithCredentials(tos.NewStaticCredentials(info.AccessKey, info.SecretKey)),
	)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func getTaskInfo[T task.TaskExtensionInfo](task T) handles.TaskInfo {
	errMsg := ""
	if task.GetErr() != nil {
		errMsg = task.GetErr().Error()
	}
	progress := task.GetProgress()
	// if progress is NaN, set it to 100
	if math.IsNaN(progress) {
		progress = 100
	}
	creatorName := ""
	var creatorRole model.Roles
	if task.GetCreator() != nil {
		creatorName = task.GetCreator().Username
		creatorRole = task.GetCreator().Role
	}
	return handles.TaskInfo{
		ID:          task.GetID(),
		Name:        task.GetName(),
		Creator:     creatorName,
		CreatorRole: creatorRole,
		State:       task.GetState(),
		Status:      task.GetStatus(),
		Progress:    progress,
		StartTime:   task.GetStartTime(),
		EndTime:     task.GetEndTime(),
		TotalBytes:  task.GetTotalBytes(),
		Error:       errMsg,
	}
}
