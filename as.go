package main

import (
    "fmt"
    "os"
    "time"
    "strings"
    "strconv"
    "io/ioutil"
    "net/http"
    "encoding/json"
    "gopkg.in/mgo.v2/bson"
    "github.com/jasonlvhit/gocron"
)

type Input struct {
    Id int64 `bson:"id"`
    Success_http_response_code int `bson:"success_http_response_code"`
    Max_retries int `bson:"max_retries"`
    Callback_webhook_url string  `bson:"callback_webhook_url"`
    Request Request `bson:"request"`
}

type Request struct {
    Url string `bson:"url"`
    Method string `bson:"method"`
    Http_headers Http_headers `bson:"http_headers"`
    Body map[string]interface{} `bson:"body"`
}

type Http_headers struct {
    Content_Type string `bson:"Content-Type"`
    Accept string `bson:"Accept"`
}


type Job struct { 
    Status string `bson:"status"`
    Num_retries int `bson:"num_retries"`
}


type Output struct {
    Response Response `bson:"response"`
}

type Response struct {
    Http_response_code int `bson:"http_response_code"`
    Http_headers Http_headers_response `bson:"http_headers"`
    Body map[string]interface{} `bson:"body"`
}

type Http_headers_response struct {
    Date string `bson:"Date"`
    Content_Type string `bson:"Content-Type"`
    Content_Length int `bson:"Content-Length"`
}

type OutputFile struct {
    Job Job `bson:"job"`
    Input Input `bson:"input"`
    Output Output `bson:"output"`
    Callback_response_code int `bson:"callback_response_code"`
}

var array_input []Input
var array_outputFile []OutputFile

var temp_value map[string]interface{}

	
func check(e error) {
    if e != nil {
        panic(e)
    }
}

func writeFiledata(data []byte, fileName string)  {
    file, err := os.Create(fileName)
    check(err)
    
    defer file.Close()   
    
    numBytes, err := file.Write(data)
   fmt.Printf("\nwrote %d bytes to %s\n", numBytes, fileName)
    
    file.Sync()
}

func readInputFiledata(fileName string)  {
    in, err := ioutil.ReadFile(fileName)
    check(err)

  
    err = bson.Unmarshal(in, &temp_value)
    check(err)
    
} 

func createOutputBSON() []byte {
    data, err := bson.Marshal(&array_outputFile)
    check(err)

    return data    
}

func restCall(outputIndex int, inputIndex int) {

    array_client := &http.Client{}
    jsonString, _ := json.Marshal(array_input[inputIndex].Request.Body)

    req,_ := http.NewRequest(array_input[inputIndex].Request.Method, array_input[inputIndex].Request.Url, strings.NewReader(string(jsonString)))
    req.Header.Set("Content-Type", array_input[inputIndex].Request.Http_headers.Content_Type)
    req.Header.Set("Accept", array_input[inputIndex].Request.Http_headers.Accept)

    res, _ := array_client.Do(req)

    try_count := 0
    status_value := "INITIAL"

    if outputIndex >= 0 {
        try_count = array_outputFile[outputIndex].Job.Num_retries
        status_value = array_outputFile[outputIndex].Job.Status
    }

    if status_value == "STILL_TRYING" {
        try_count += 1
    }

    var job Job
    callback_response_code := 0

    if res.StatusCode == array_input[inputIndex].Success_http_response_code {

        callbackReq, _ := http.NewRequest("POST", array_input[inputIndex].Callback_webhook_url, strings.NewReader(string(jsonString)))
        callbackRes, _ := array_client.Do(callbackReq)
        callback_response_code = callbackRes.StatusCode

        job = Job{Status: "COMPLETED", Num_retries: try_count}
    
    } else {

        if try_count >= array_input[inputIndex].Max_retries {

            callbackReq1, _ := http.NewRequest("POST", array_input[inputIndex].Callback_webhook_url, strings.NewReader(string(jsonString)))
            callbackRes1, _ := array_client.Do(callbackReq1)
            callback_response_code = callbackRes1.StatusCode

            job = Job{Status: "FAILED", Num_retries: try_count}

        } else {

            job = Job{Status: "STILL_TRYING", Num_retries: try_count}

        }
    }

    contentLength, _ := strconv.Atoi(res.Header["Content-Length"][0])
    http_headers_response := Http_headers_response{Date: res.Header["Date"][0], Content_Type: res.Header["Content-Type"][0], Content_Length: contentLength}
    response_body := map[string] interface{} {}
    response_body = array_input[inputIndex].Request.Body
    resp := Response{Http_response_code: res.StatusCode, Http_headers: http_headers_response, Body: response_body}
    output := Output{Response: resp}
    output_file := OutputFile{Job: job, Input: array_input[inputIndex], Output: output, Callback_response_code: callback_response_code}
    if outputIndex < 0 {
        array_outputFile = append(array_outputFile, output_file)
    } else {
        array_outputFile[outputIndex] = output_file
    }
    write_data := createOutputBSON()  
    writeFiledata(write_data, "output.bson")
  
}

func makeTimestamp() int64 {
    return time.Now().UnixNano() / (int64(time.Millisecond)/int64(time.Nanosecond))
}

func performTask() {
    t := makeTimestamp()
    fmt.Printf("Running performTask...@%d\n", t)
    
    readInputFiledata("input.bson")
    array_input = array_input[:0]

    var in Input
    tempBytes, _ := bson.Marshal(temp_value)
    err := bson.Unmarshal(tempBytes, &in)
    if in.Id == 0 || err != nil {
        for _,v := range temp_value {
            d,_ := json.Marshal(v)
            var i Input
            json.Unmarshal(d,&i)
            i.Request.Http_headers.Content_Type = "application/json"
            array_input = append(array_input, i)
        }
    } else {
        array_input = append(array_input, in)
    }
    max_try := 1
    count := 0
    for max_try > 0 {
        for i := 0; i < len(array_input); i++ {
            if count == 0 && max_try <= array_input[i].Max_retries {
                max_try = array_input[i].Max_retries + 1
            }
            if len(array_outputFile) - 1 < i {
                restCall(-1, i)
            } else if array_outputFile[i].Job.Status == "STILL_TRYING" {
                restCall(i, i)
            }
        }
        max_try -= 1
        count = 1
    }
}

func main() {
    
    st := gocron.NewScheduler()
    st.Every(5).Seconds().Do(performTask)
    <- st.Start()

}