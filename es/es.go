package es

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
)

type ESMsg struct {
	Message string `json:"message"`
}

type Message struct {
	Lib         string `json:"lib"`
	ReceiveTime int64  `json:"receive_time"`
	Type        string `json:"type"`
	TrackID     int    `json:"_track_id"`
	ReportTime  int64  `json:"report_time"`
	DistinctID  string `json:"distinct_id"`
	SinkTime    int64  `json:"sink_time"`
	Time        int64  `json:"time"`
	Event       string `json:"event"`
	Properties  string `json:"properties"`
}

type ESClient struct {
	// 保存初始化es得到的client对象
	client *elastic.Client

	// 表示client连接的index
	index string

	// 表示client是否要停止发送数据
	CloseChan chan struct{}
}

// 初始化ES
func ESInit(url string) (esClient *ESClient, err error) {
	client, err := elastic.NewClient(elastic.SetURL(url))
	if err != nil {
		return &ESClient{nil, "", make(chan struct{}, 1)}, err
	}
	return &ESClient{client, "", make(chan struct{}, 1)}, nil
}

// 设置es client对应的index
func (esClient *ESClient) SetIndex(index string) {
	esClient.index = index
	logrus.Info("es client has changed its index to :", index)
}

// 从chan中读取数据保存到ES
func (esClient ESClient) SaveData(msgChan <-chan string) {
	go func() {
		for {
			select {
			case <-esClient.CloseChan:
				esClient.client.Stop()
				logrus.Warning("es server stoped")
				return
			case msg, ok := <-msgChan:
				if !ok {
					logrus.Warning("msgChan has been closed")
					esClient.CloseChan <- struct{}{}
					break
				}
				logrus.Info("revive a message from msgchan, save to es")
				// p1 := ESMsg{msg}
				message := &Message{}
				err := json.Unmarshal([]byte(msg), message)
				if err != nil {
					fmt.Println("Unmarshal message failed, err:", err)
					continue
				}
				_, err = esClient.client.Index().Index(esClient.index).BodyJson(message).Do(context.Background())
				if err != nil {
					fmt.Println("save message failed, err:", err)
					// esClient.CloseChan <- struct{}{}
					// logrus.Errorf("save message to es failed, message:%s, id:%v, index:%v, type:%v\n", msg, put1.Id, put1.Index, put1.Type)
				}
			}
		}
	}()
}
