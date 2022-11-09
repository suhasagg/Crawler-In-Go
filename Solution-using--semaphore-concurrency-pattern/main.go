/**
Common description:
You need to write a program that gets webpages, extracts content length of a page and puts this value into the map.
When all address processed, program prints all pairs (webpage address, content length).

Specific requirements:

Worker is a function that receives webpage address (ex: "google.com") and gets page content.
Worker gets only basic html document and do not receive external resources like images, css and so on.
You need to create two workers (two go routines).
You need to use channel(s) to send webpage address to the workers.

On success, if it's possible to get webpage, put the length of webpage content into rusults.ContentLength map. Use as a key webpage address, and content length as the value.
On failure, if it's impossible to get webpage because some errors, put into the results.ContentLength webpage address as a key, and -1 as a value.

When all webpages from webPages slice processed, print each key-value from webPages.
Example:
google.com - 4501
...


If the program runs more than 30 seconds, you must stop all workers (go routines), print message "out of time" and exit.


You can modify provided sources as you wish. You can extend the results structure as you need just keep two provided fields in it.
*/

package main

import (
	"context"
	"encoding/csv"
	"errors"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Response struct {
	Body  string
	Error error
}

// BackoffPolicy implements a backoff policy, randomizing its delay and
// saturating at the final value in Millis
type BackoffPolicy struct {
	Millis []int
}

// Default is a backoff policy ranging up to 5 seconds
var Default = BackoffPolicy{
	[]int{10, 100, 500, 500, 3000, 3000, 5000},
}

// Duration returns the time duration of the nth wait cycle in a
// backoff policy. This is b.Millis[n], randomized to avoid thundering herds
func (b BackoffPolicy) Duration(n int) time.Duration {
	if n >= len(b.Millis) {
		n = len(b.Millis) - 1
	}
	return time.Duration(jitter(b.Millis[n])) * time.Millisecond
}

//Exponential backoff structure
//Supports different methods for jitter implementation

// Full Jitter,Decorr Jitter are not implemented -> Equal jitter is demonstratred here
// jitter returns a random integer uniformly distributed in the range
// [0.5 * millis .. 1.5 * millis]
func jitter(millis int) int {
	if millis == 0 {
		return 0
	}
	return millis / 2 * rand.Intn(millis)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func getURLData(url string) (*http.Response, []byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	return resp, body, nil
}

func fetch(url string) *Response {
	req, _ := http.NewRequest("GET", url, nil)
	// Rotate User Agents for scraping
        user_agent_list := [5]string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 14_4_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1",
		"Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36 Edg/87.0.664.75",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.18363",
	}
	randomIndex := rand.Intn(len(user_agent_list))
	pick := user_agent_list[randomIndex]
	req.Header.Set("User-Agent", pick)

	client := &http.Client{}
	resp, _ := client.Do(req)

	var response *Response
	if resp != nil {
		body, err := ioutil.ReadAll(resp.Body)
		response = &Response{
			Body:  string(body),
			Error: err,
		}

	} else {
		response = &Response{
			Body:  "",
			Error: errors.New("critical error"),
		}
	}
	return response
}

func getURLDataWithRetries(url string, numberofretries int) string {
	response := "critical error" + "@" + url
	for i := 0; i < numberofretries+1; i++ {
		log.Println("Retry", url+":Retry:", i)
		resp := fetch(url)
		if resp.Body != "" {
			return strconv.Itoa((len(resp.Body))) + "@" + url
		}
		if numberofretries > 0 {
			time.Sleep(Default.Duration(i))
		}
	}
	return response
}

func readCsvFile(filePath string) [][]string {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read input file "+filePath, err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+filePath, err)
	}

	return records
}

func openLogFile(path string) (*os.File, error) {
	logFile, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return logFile, nil
}

var (
	urls    = readCsvFile("/home/swordfish/Downloads/b.csv")
        //Url database used consists of all the different types of urls - normal response, error from url or url timing out
	urllist []string
        //Please specify output file path
        outputfilelog = "crawlersemaphores.log"
        results struct {
		// put here content length of each page
		ContentLength map[string]int

		// accumulate here the content length of all pages
		TotalContentLength int
	}
	urldata map[string]int
)

func main() {
	urldata = make(map[string]int)
	file, err := openLogFile(outputfilelog)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	start1 := time.Now()
	d := time.Now().Add(30000 * time.Millisecond)
	for _, line := range urls {
		for j, field := range line {
			if j == 1 {
				urllist = append(urllist, field)

			}
		}
	}
        //Stopping crawling go routines after 30 second timeout using context with deadlines
	ctx, cancel := context.WithDeadline(context.Background(), d)
	sum := 0
        //semaphore capacity for limiting concurrency
	maxConcurrency := 60
	//Exponential backoff - number of retries - default to zero (this covers scenario if url is being throttled, retry after a delay)
        numberofRetries := 0
        var response []string
	defer cancel()
	//Processing response from response channel
        for o := range multiWorkers(ctx, maxConcurrency, numberofRetries, toStream(ctx, urllist...)) {
                res := o.(string)
		response = append(response, res)
        }
	for _, value := range response {
		urlResponse := strings.Split(value, "@")
		if strings.Contains(urlResponse[0], "critical") {
			urldata[urlResponse[1]] = -1
		} else {
			contentlength, _ := strconv.Atoi(urlResponse[0])
			urldata[urlResponse[1]] = contentlength
			sum += contentlength
		}

	}

	results.ContentLength = urldata
	results.TotalContentLength = sum

        //Response data printed
        //Program will try to  collect maximum url responses before time out desdline of 30 seconds
	log.Println("Total Response sum", sum)
	log.Println("multi workers completed in", time.Since(start1))
	log.Println("Url Map", results.ContentLength)
	log.Println("TotalContentLength", results.TotalContentLength)
}

// Fanning out the crawling jobs to multiple go routines equal to semaphore capacity
func multiWorkers(ctx context.Context, maxConcurrency int, numberofRetries int, inStream <-chan interface{}) <-chan interface{} {
	//Url Frontier fill capacity without getting blocked
        outStream := make(chan interface{})
     	var wg sync.WaitGroup
        wg.Add(1)
	multiplex := func(in <-chan interface{}) {
		defer wg.Done()		
                //We set the max concurrency to 60 - which means me limit the goroutine to run
		//a maximum of 60 at a time for parallel url page downloads
		semaphore := make(chan bool, maxConcurrency)
		for o := range in {
			log.Println("url", o.(string))
			semaphore <- true
			log.Println("Semaphore", cap(semaphore), len(semaphore))
			wg.Add(1)
			go func(url string) {
				defer wg.Done()
                              //Releasing semaphore after url crawling is done
                                defer func() {
				<-semaphore
			        }()
				select {
                                //context with deadline is used - signal is received here after 30 seconds deadline
				case <-ctx.Done():
					log.Println("out of time")
                                        return
				//Block until url has been crawled
				case outStream <- getURLDataWithRetries(url, numberofRetries):	                                     
                                     log.Println("url response written to channel")
				}
				
			}(o.(string))

		}
		
	}
	go multiplex(inStream)
        go func() {
		// Wait for all the crawling goroutines to complete (accomplished using wait groups)
                //Closing the pipeline, url response channel
		wg.Wait()
                time.Sleep(5 * time.Second)
                defer close(outStream)
		log.Println("stop pipeline")
	}()

	return outStream
}

// Generator channel for URLs
func toStream(ctx context.Context, urls ...string) <-chan interface{} {
	outStream := make(chan interface{})

	go func() {
		defer close(outStream)
                for _, v := range urls {
			select {
			case <-ctx.Done():
				return
			case outStream <- "http://" + v:
			}

		}
		
	}()

	return outStream
}

