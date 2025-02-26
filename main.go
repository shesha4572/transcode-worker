package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"time"
)

type WorkerPodStatus struct {
	PodName        string `json:"podName"`
	IsAssignedTask bool   `json:"isAssignedTask"`
	AssignedTaskId string `json:"assignedTaskId"`
}

type WorkerJobFinish struct {
	PodName        string `json:"podName"`
	AssignedTaskId string `json:"assignedTaskId"`
	MpdName        string `json:"mpdName"`
}

type TranscodeJob struct {
	VideoInternalFileId string `json:"videoInternalFileId"`
	StartTime           string `json:"startTime"`
	EndTime             string `json:"endTime"`
	AssignedTaskID      string `json:"assignedTaskID"`
}

var currentStatus = WorkerPodStatus{PodName: "", IsAssignedTask: false, AssignedTaskId: ""}
var finishModel = WorkerJobFinish{PodName: "", AssignedTaskId: "", MpdName: ""}
var TR_CONTROLLER_URL = os.Getenv("TR_CONTROLLER_URL")
var VIDEO_SERVER_URL = os.Getenv("VIDEO_SERVER_URL")

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func main() {
	router := gin.Default()
	router.GET("/hello", hello)
	router.POST("/job", transcodeJob)
	currentStatus.PodName = os.Getenv("POD_NAME")
	finishModel.PodName = currentStatus.PodName
	ticker := time.NewTicker(time.Second * 10)
	go func() {
		for range ticker.C {
			contactController("ping")
		}
	}()

	err := router.Run()
	if err != nil {
		fmt.Printf("Error starting server : %s\n", err)
	}
	return

}

func contactController(endpoint string) {
	url := TR_CONTROLLER_URL + fmt.Sprintf("/api/v1/worker/%s", endpoint)
	var bodyObject any
	if endpoint == "ping" {
		bodyObject = currentStatus
	} else {
		bodyObject = finishModel
	}
	fmt.Printf("Sending %s to controller Status : %s\n", endpoint, bodyObject)
	body, err := json.Marshal(bodyObject)
	if err != nil {
		fmt.Printf("Error marshaling status to JSON\n")
		return
	}
	res, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	var resBody []byte
	if res == nil {
		fmt.Printf("Error getting response\n")
		return
	}
	_, e := res.Body.Read(resBody)
	if e != nil {
		fmt.Printf("Error reading response body\n")
		return
	}
	if err != nil {
		fmt.Printf("Error sending %s to controller Response : %s\n", endpoint, resBody)
		return
	}
	defer res.Body.Close()
	fmt.Printf("Successfully sent %s to controller\n", endpoint)
	return
}

func transcodeJob(c *gin.Context) {
	var job TranscodeJob
	if err := c.BindJSON(&job); err != nil {
		fmt.Println("Error reading transcode request body")
		c.IndentedJSON(http.StatusInternalServerError, map[string]any{"error": "Error reading transcode request body"})
		return
	}
	currentStatus.IsAssignedTask = true
	currentStatus.AssignedTaskId = job.AssignedTaskID
	fmt.Printf("Transcode request recieved %s\n", job)
	c.IndentedJSON(http.StatusOK, map[string]any{"message": "Job recieved"})
	go performTranscode(job)
	return
}

func RandomString(n int) string {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func performTranscode(job TranscodeJob) {
	videoUrl := fmt.Sprintf("%s/read/%s", VIDEO_SERVER_URL, job.VideoInternalFileId)
	chunkName := fmt.Sprintf("%s_%s", job.VideoInternalFileId, RandomString(5))
	finishModel.MpdName = chunkName
	cmdArgs := []string{
		"-ss", job.StartTime, "-to", job.EndTime, "-i", videoUrl,
		"-map", "0:v:0", "-map", "0:a:0",
		"-map", "0:v:0", "-map", "0:a:0",
		"-map", "0:v:0",
		"-map", "0:v:0",
		"-map", "0:v:0",
		"-map", "0:v:0",
		"-c:v:0", "libsvtav1", "-preset", "10", "-crf", "35", "-filter:v:0", "scale=256:144",
		"-c:v:1", "libsvtav1", "-preset", "10", "-crf", "32", "-filter:v:1", "scale=426:240",
		"-c:v:2", "libsvtav1", "-preset", "10", "-crf", "30", "-filter:v:2", "scale=640:360",
		"-c:v:3", "libsvtav1", "-preset", "10", "-crf", "28", "-filter:v:3", "scale=854:480",
		"-c:v:4", "libsvtav1", "-preset", "10", "-crf", "26", "-filter:v:4", "scale=1280:720",
		"-c:v:5", "libsvtav1", "-preset", "10", "-crf", "24", "-filter:v:5", "scale=1920:1080",
		"-c:a:0", "aac", "-b:a:0", "128k", "-ac", "2",
		"-c:a:1", "aac", "-b:a:1", "192k", "-ac", "2",
		"-f", "dash", "-min_seg_duration", "2000", "-use_template", "1", "-use_timeline", "1",
		"-init_seg_name", fmt.Sprintf("%s_chunk_$RepresentationID$_init", chunkName),
		"-media_seg_name", fmt.Sprintf("%s_chunk_$RepresentationID$_$Number$", chunkName),
		"-adaptation_sets", "id=0,streams=v id=1,streams=a",
		fmt.Sprintf("%s", chunkName),
	}
	makeDir := exec.Command("mkdir", "./"+job.VideoInternalFileId)
	makeDir.Stdout = os.Stdout
	err := makeDir.Run()
	if err != nil {
		fmt.Println("Error making directory")
		return
	}
	ffmpegCmd := exec.Command("ffmpeg", cmdArgs...)
	ffmpegCmd.Dir = "./" + job.VideoInternalFileId
	ffmpegCmd.Stdout = os.Stdout
	ffmpegCmd.Stderr = os.Stderr
	if err := ffmpegCmd.Run(); err != nil {
		fmt.Println("Could not run command: ", err.Error())
	}
	uploadChunks(job)
	cleanUp(job)

}

func cleanUp(job TranscodeJob) {
	fmt.Printf("Resetting worker state to available\n")
	currentStatus.IsAssignedTask = false
	currentStatus.AssignedTaskId = ""
	finishModel.AssignedTaskId = job.AssignedTaskID
	contactController("finish")
	fmt.Printf("Deleting dir %s\n", job.VideoInternalFileId)
	deleteFilesInDir(job.VideoInternalFileId)
}

func deleteFilesInDir(dirName string) {
	cmd := exec.Command("rm", "-rf", dirName)
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		fmt.Println("Could not run command: ", err.Error())
	}
}

func uploadChunks(job TranscodeJob) {
	fmt.Printf("Uploading chunks of video #%s\n", job.VideoInternalFileId)
	dirPath := "./" + job.VideoInternalFileId
	files, err := os.ReadDir(dirPath)
	if err != nil {
		fmt.Printf("Error reading files in dir %s\n", dirPath)
	}

	for _, file := range files {
		fmt.Printf("Uploading file %s of task %s\n", file.Name(), job.AssignedTaskID)
		uploadFile(dirPath+"/"+file.Name(), file.Name(), file.Name())
	}
}

func uploadFile(filePath, fileID, fileName string) {
	url := VIDEO_SERVER_URL + "/upload"
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Uploading file %s failed : %v", fileName, err)
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("Uploading file %s failed : %v", fileName, err)
		}
	}(file)

	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)
	filePart, err := writer.CreateFormFile("file", fileName)
	if err != nil {
		fmt.Printf("Uploading file %s failed : %v", fileName, err)
		return
	}
	_, err = io.Copy(filePart, file)
	if err != nil {
		fmt.Printf("Uploading file %s failed : %v", fileName, err)
		return
	}

	_ = writer.WriteField("file_id", fileID)
	_ = writer.WriteField("file_name", fileName)

	err = writer.Close()
	if err != nil {
		fmt.Printf("Uploading file %s failed : %v", fileName, err)
		return
	}
	req, err := http.NewRequest("POST", url, &requestBody)
	if err != nil {
		fmt.Printf("Uploading file %s failed : %v", fileName, err)
		return
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Uploading file %s failed : %v", fileName, err)
		return
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("Uploading file %s failed : %v", fileName, err)
		}
	}(resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		fmt.Printf("File %s uploaded successfully!\n", fileName)
		return
	} else {
		fmt.Printf("Failed to upload file %s. Status: %d\n", fileName, resp.StatusCode)
		return
	}
}

func hello(c *gin.Context) {
	c.JSON(http.StatusOK, map[string]string{"message": "hello"})
	return
}
