package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"golang.org/x/text/encoding/simplifiedchinese"
)

var Client *elasticsearch.Client

//elasticsearch 服务器地址
var host = "http://121.36.60.252:9200"

//elasticsearch 索引库
var index = []string{"document"}

//文档路径
var filepath string

//正则表达式变量
var doc_split = regexp.MustCompile(`<REC>`)
var key_value = regexp.MustCompile(`<.+>=.+`)
var kv_split = regexp.MustCompile(`>=`)
var kv_start = regexp.MustCompile(`<`)

//多类型变量设置
var s string
var ss []string = make([]string, 0)
var successdoccount int = 0
var encodingmethod string

//init 包含：连接es
func init() {
	host = os.Args[1]
	index[0] = os.Args[2]
	encodingmethod = os.Args[3]
	filepath = os.Args[4]
	var err error
	config := elasticsearch.Config{}
	config.Addresses = []string{host}
	Client, err = elasticsearch.NewClient(config)
	checkError(err)
	log.Println(Client.Info())
	fmt.Println("Init Finished, Start...")
}

//checkError 查看错误
func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func main() {
	//创建索引库
	createIndex(index[0])

	//测试utf-8
	//docHandleutf8("./CJFD1.txt")
	//测试gbk
	if encodingmethod == "UTF-8" {
		docHandleutf8(filepath)
	} else if encodingmethod == "GBK" {
		docHandlegbk(filepath)
	} else {
		fmt.Println("Error: Encoding Method Incorrect!")
		fmt.Println("Usage: esclient [Host] [Index] [Encoding Method] [Filepath]")
		return
	}
}

//docHandleutf8 用来处理文本，作为本程序的第一个功能
func docHandleutf8(file string) {
	//传入目标文本路径
	doc, openfile_Error := os.Open(file) //测试文本
	//doc, openfile_Error := os.Open("./test2.txt") //测试文本
	//出错处理
	if openfile_Error != nil {
		fmt.Printf("Error: %s\n", openfile_Error)
		fmt.Println("Usage: esclient [Host] [Index] [Encoding Method] [Filepath]")
		return
	}
	//获得文件名用于制作id
	filename := strings.ReplaceAll(doc.Name(), "./", "")
	//关闭文件
	defer doc.Close()

	//记录完成文档数目
	var testcount = 0

	//按行处理文件
	//创建reader
	reader := bufio.NewReader(doc)
	//创建map变量用于外置变量
	var docmap map[string]interface{}
	//开始读取
	//创建一个可变类型interface
	var lineinterface interface{}
	for {
		//读取一行原始数据
		line, lineerr := reader.ReadString('\n')
		line = strings.ReplaceAll(line, "\n", "")
		line = strings.ReplaceAll(line, "\r", "")
		if lineerr == io.EOF {
			break
		}
		//如果匹配到<REC>则分割文档
		for doc_split.MatchString(line) {
			//创建map来保存文档信息
			docmap = make(map[string]interface{})
			for {
				//读取下一行原始数据
				line, lineerr = reader.ReadString('\n')
				line = strings.ReplaceAll(line, "\n", "")
				line = strings.ReplaceAll(line, "\r", "")
				if lineerr == io.EOF {
					break
				}
				//预先得知下一行数据，但不读取
				nextiskv, _ := isnextkv(reader)
				//取得一条完整可以解析为key value 的数据
				//放入空接口中
				lineinterface = line
				for (kv_split.MatchString(line) && !nextiskv) || (!kv_split.MatchString(line) && !nextiskv) {
					//当前条存放入数组中
					ss = append(ss, line)

					//获得一条得数据
					line, lineerr = reader.ReadString('\n')
					line = strings.ReplaceAll(line, "\n", "")
					line = strings.ReplaceAll(line, "\r", "")
					//重定位预知行
					nextiskv, _ = isnextkv(reader)
					//跳出判断
					if !kv_split.MatchString(line) && nextiskv {
						//加入最后不匹配的最后一行
						ss = append(ss, line)
						lineinterface = ss
						ss = make([]string, 0)
						break
					}
					if lineerr == io.EOF {
						lineinterface = ss
						ss = make([]string, 0)
						break
					}

				}
				//跳出文档内循环
				if doc_split.MatchString(line) {
					break
				}
				//生成map
				switch v := lineinterface.(type) {
				case string:
					str1, str2 := kvSplit(line)
					//存入map
					docmap[str1] = str2
				case []string:
					ss = v
					str1, _ := kvSplit(ss[0])
					str2 := ss[1:]
					//存入map
					docmap[str1] = str2

					//fmt.Printf("%s%s", str1, str2)
				}

			}

			//将docmap存入es
			testcount++
			insertSingle(docmap, filename+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))

			//输出处理完成提示
			fmt.Println("已操作文档数目：", testcount, "\n成功文档数目：", successdoccount)
			//time.Sleep(time.Millisecond * 50)
		}

	}
	//清楚successdoccount
	successdoccount = 0
}

//docHandlegbk 用来处理文本，作为本程序的第一个功能
func docHandlegbk(file string) {
	//传入目标文本路径
	doc, openfile_Error := os.Open(file) //测试文本
	//doc, openfile_Error := os.Open("./test2.txt") //测试文本
	//出错处理
	if openfile_Error != nil {
		fmt.Printf("Error: %s\n", openfile_Error)
		fmt.Println("Usage: esclient [Host] [Index] [Encoding Method] [Filepath]")
		return
	}
	//获得文件名用于制作id
	filename := strings.ReplaceAll(doc.Name(), "./", "")
	//关闭文件
	defer doc.Close()

	//记录完成文档数目
	var testcount = 0

	//按行处理文件
	//创建reader
	reader := bufio.NewReader(doc)
	//创建map变量用于外置变量
	var docmap map[string]interface{}
	//开始读取
	//创建一个可变类型interface
	var lineinterface interface{}
	for {
		//读取一行原始数据
		thisline, lineerr := reader.ReadString('\n')
		//重编码
		line, _ := gbk_decoder(thisline)
		line = strings.ReplaceAll(line, "\n", "")
		line = strings.ReplaceAll(line, "\r", "")
		if lineerr == io.EOF {
			break
		}
		//如果匹配到<REC>则分割文档
		for doc_split.MatchString(line) {
			//创建map来保存文档信息
			docmap = make(map[string]interface{})
			for {
				//读取下一行原始数据
				thisline, lineerr = reader.ReadString('\n')
				//重编码
				line, _ = gbk_decoder(thisline)
				line = strings.ReplaceAll(line, "\n", "")
				line = strings.ReplaceAll(line, "\r", "")
				if lineerr == io.EOF {
					break
				}
				//预先得知下一行数据，但不读取
				nextiskv, _ := isnextkv(reader)
				//取得一条完整可以解析为key value 的数据
				//放入空接口中
				lineinterface = line
				for (kv_split.MatchString(line) && !nextiskv) || (!kv_split.MatchString(line) && !nextiskv) {
					//当前条存放入数组中
					ss = append(ss, line)

					//获得一条得数据
					line, lineerr = reader.ReadString('\n')
					//重编码
					line, _ = gbk_decoder(thisline)
					line = strings.ReplaceAll(line, "\n", "")
					line = strings.ReplaceAll(line, "\r", "")
					//重定位预知行
					nextiskv, _ = isnextkv(reader)
					//跳出判断
					if !kv_split.MatchString(line) && nextiskv {
						//加入最后不匹配的最后一行
						ss = append(ss, line)
						lineinterface = ss
						ss = make([]string, 0)
						break
					}
					if lineerr == io.EOF {
						lineinterface = ss
						ss = make([]string, 0)
						break
					}

				}
				//跳出文档内循环
				if doc_split.MatchString(line) {
					break
				}
				//生成map
				switch v := lineinterface.(type) {
				case string:
					str1, str2 := kvSplit(line)
					//存入map
					docmap[str1] = str2
				case []string:
					ss = v
					str1, _ := kvSplit(ss[0])
					str2 := ss[1:]
					//存入map
					docmap[str1] = str2

					//fmt.Printf("%s%s", str1, str2)
				}

			}

			//将docmap存入es
			testcount++
			insertSingle(docmap, filename+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))

			//输出处理完成提示
			fmt.Println("已操作文档数目：", testcount, "\n成功文档数目：", successdoccount)
			//time.Sleep(time.Millisecond * 50)
		}

	}
	//清楚successdoccount
	successdoccount = 0
}

//gbk_decoder 将GBK编码的字符串转换成UTF-8字符串
func gbk_decoder(s string) (string, error) {
	gbk_dec := simplifiedchinese.GBK.NewDecoder()
	return gbk_dec.String(s)

}

//kvSplit 将通过正则表达式获得的字符串进行分离
//返回容量2的字符串数组
func kvSplit(s string) (string, string) {
	str := regexp.MustCompile(`>=`).Split(s, 2)
	str[0] = strings.TrimPrefix(str[0], "<")
	return str[0], str[1]
}

//预知下一行的信息
//比较是否匹配kv
//如果读完了返回一个错误
func isnextkv(reader *bufio.Reader) (bool, error) {
	//预先得知下一行数据，但不读取
	nextlineheadbytes, err := reader.Peek(1)
	return kv_start.Match(nextlineheadbytes), err
}

//createIndex 创建一个索引
func createIndex(idx string) {
	if IsExistsIndex(idx) {
		fmt.Println("Index :" + idx + " Already Exist.")
		return
	} else {
		body := map[string]interface{}{
			"settings": map[string]interface{}{
				"number_of_shards":   1,
				"number_of_replicas": 0,
			},
		}

		// body := map[string]interface{}{
		// 	"mappings": map[string]interface{}{
		// 		"properties": map[string]interface{}{
		// 			"str": map[string]interface{}{
		// 				"type": "keyword", // 表示这个字段不分词
		// 			},
		// 		},
		// 	},
		// }
		jsonBody, _ := json.Marshal(body)
		req := esapi.IndicesCreateRequest{
			Index: idx,
			Body:  bytes.NewReader(jsonBody),
		}
		res, err := req.Do(context.Background(), Client)
		checkError(err)
		defer res.Body.Close()
		log.Println(res.String())
		fmt.Println("Index: " + idx + " Created!")
	}
}

// //deleteIndex 删除指定索引
// func deleteIndex() {
//     req := esapi.IndicesDeleteRequest{
//         Index: []string{"test_index"},
//     }
//     res, err := req.Do(context.Background(), Client)
//     checkError(err)
//     defer res.Body.Close()
//     fmt.Println(res.String())
// }

//insertSingle 向索引中插入单条数据
func insertSingle(docmap map[string]interface{}, id string) {
	jsonBody, _ := json.Marshal(docmap)
	req := esapi.CreateRequest{ // 如果是esapi.IndexRequest则是插入/替换
		Index:      index[0],
		DocumentID: id,
		Body:       bytes.NewReader(jsonBody),
	}
	res, err := req.Do(context.Background(), Client)
	checkError(err)
	defer res.Body.Close()
	log.Println(res.String())
	if res.StatusCode == 201 {
		successdoccount++
	}
}

//insertBatch 批量插入
func insertBatch() {
	var bodyBuf bytes.Buffer
	for i := 2; i < 10; i++ {
		createLine := map[string]interface{}{
			"create": map[string]interface{}{
				"_index": "test_index",
				"_id":    "test_" + strconv.Itoa(i),
				"_type":  "test_type",
			},
		}
		jsonStr, _ := json.Marshal(createLine)
		bodyBuf.Write(jsonStr)
		bodyBuf.WriteByte('\n')

		body := map[string]interface{}{
			"num": i % 3,
			"v":   i,
			"str": "test" + strconv.Itoa(i),
		}
		jsonStr, _ = json.Marshal(body)
		bodyBuf.Write(jsonStr)
		bodyBuf.WriteByte('\n')
	}

	req := esapi.BulkRequest{
		Body: &bodyBuf,
	}
	res, err := req.Do(context.Background(), Client)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

//selectBySql
func selectBySql() {
	query := map[string]interface{}{
		"query": "select count(*) as cnt, max(v) as value, num from test_index where num > 0 group by num",
	}
	jsonBody, _ := json.Marshal(query)
	req := esapi.SQLQueryRequest{
		Body: bytes.NewReader(jsonBody),
	}
	res, err := req.Do(context.Background(), Client)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

//selectBySearch 通过json查询
func selectBySearch() {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"filter": map[string]interface{}{
					"range": map[string]interface{}{
						"num": map[string]interface{}{
							"gt": 0,
						},
					},
				},
			},
		},
		"size": 0,
		"aggs": map[string]interface{}{
			"num": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "num",
					//"size":  1,
				},
				"aggs": map[string]interface{}{
					"max_v": map[string]interface{}{
						"max": map[string]interface{}{
							"field": "v",
						},
					},
				},
			},
		},
	}
	jsonBody, _ := json.Marshal(query)

	req := esapi.SearchRequest{
		Index: index,
		Body:  bytes.NewReader(jsonBody),
	}
	res, err := req.Do(context.Background(), Client)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

//updateSingle 根据id更新
func updateSingle(id string) {
	body := map[string]interface{}{
		"doc": map[string]interface{}{
			"v": 100,
		},
	}
	jsonBody, _ := json.Marshal(body)
	req := esapi.UpdateRequest{
		Index:      index[0],
		DocumentID: id,
		Body:       bytes.NewReader(jsonBody),
	}

	res, err := req.Do(context.Background(), Client)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
	if res.StatusCode == 200 {
		successdoccount++
	}
}

//updateByQuery 根据条件更新
func updateByQuery() {
	body := map[string]interface{}{
		"script": map[string]interface{}{
			"lang": "painless",
			"source": `
                ctx._source.v = params.value;
            `,
			"params": map[string]interface{}{
				"value": 101,
			},
		},
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	jsonBody, _ := json.Marshal(body)
	req := esapi.UpdateByQueryRequest{
		Index: index,
		Body:  bytes.NewReader(jsonBody),
	}
	res, err := req.Do(context.Background(), Client)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

//deleteSingle 根据Id删除
func deleteSingle(id string) {
	req := esapi.DeleteRequest{
		Index:      index[0],
		DocumentID: id,
	}

	res, err := req.Do(context.Background(), Client)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

//deleteSingle 根据条件删除
func deleteByQuery() {
	body := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	jsonBody, _ := json.Marshal(body)
	req := esapi.DeleteByQueryRequest{
		Index: index,
		Body:  bytes.NewReader(jsonBody),
	}
	res, err := req.Do(context.Background(), Client)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

func IsExistsIndex(idx string) bool {
	res, err := Client.Cat.Indices(
		Client.Cat.Indices.WithIndex(idx),
	)
	defer res.Body.Close()
	if err != nil {
		fmt.Println(err.Error())
		return false
	}

	if res.StatusCode == 200 {
		return true
	}
	return false
}
