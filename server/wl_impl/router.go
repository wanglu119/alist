package wlImpl

import (
	"github.com/alist-org/alist/v3/internal/db"
	"github.com/gin-gonic/gin"
	"github.com/xhofe/tache"
)

func SyncCloudTask(g *gin.RouterGroup) {
	g.POST("/create", FsCloudCreate)
	g.GET("/list", FsCloudList)
	g.POST("/delete", FsCloudDelete)
	g.POST("/stop", FsCloudStop)
	g.POST("/start", FsCloudStart)

	CloudTaskManager = tache.NewManager[*CloudTask](
		tache.WithWorks(1),
		tache.WithMaxRetry(3),
		tache.WithPersistFunction(
			db.GetTaskDataFunc("sync_cloud", true),
			db.UpdateTaskDataFunc("sync_cloud", true),
		),
		tache.WithPersistPath("./data/sync_cloud"),
	)

	/*
		CloudTaskManager = tache.NewManager[*CloudTask](
			tache.WithWorks(setting.GetInt(conf.TaskUploadThreadsNum, conf.Conf.Tasks.Upload.Workers)),
			tache.WithMaxRetry(conf.Conf.Tasks.Upload.MaxRetry)) //upload will not support persist

		op.RegisterSettingChangingCallback(func() {
			CloudTaskManager.SetWorkersNumActive(
				taskFilterNegative(setting.GetInt(conf.TaskUploadThreadsNum, conf.Conf.Tasks.Upload.Workers)))
		})
	*/
}
