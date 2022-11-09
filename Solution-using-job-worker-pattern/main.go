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
	"encoding/csv"
	"errors"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
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
	urllist []string
	results struct {
		// put here content length of each page
		ContentLength map[string]int

		// accumulate here the content length of all pages
		TotalContentLength int
	}
	urldata map[string]int
        outputfilelog = "crawlerworkerqueue.log"
   
)

var (
	MaxWorkers = 50
	MaxQueue  = 1000
        Numberofretries = 0
)

// Job represents the url job to be executed
type Job struct {
	url string
}

//URL frontier - A buffered channel that accumulates urls to be fetched

var UrlFrontier = make(chan Job, MaxQueue)


//URL Response queue - A buffered channel that accumulates url responses

var UrlResQueue = make(chan string, MaxQueue)

//Buffered quit channel to close crawling workers, worker stops after receiving a respective quit signal after 30 seconds timeout   
var quitroutines = make(chan bool, MaxWorkers)

// Worker represents the crawling worker that executes the url fetch job
type URLWorker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

func NewWorker(workerPool chan chan Job) URLWorker {
	return URLWorker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       quitroutines,
	}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w URLWorker) Start() {
	go func() {
		for {
			// register the current crawling worker into the crawling worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// we have received a work request (url to be fetched)
				// Fetch url (Exponential backoff Algorithm is also implemented - default retries = 0 and can be configured here) 
				res := getURLDataWithRetries(job.url, Numberofretries)
				log.Println("Response:", res)
				UrlResQueue <- res

			case <-w.quit:
				log.Println("Workers shutting down")
				// we have received a signal to stop
				return
			}
		}
	}()
}


type UrlDispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool chan chan Job
}

func NewUrlDispatcher(maxWorkers int) *UrlDispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &UrlDispatcher{WorkerPool: pool}
}

func (d *UrlDispatcher) Run(numberofWorkers int) {
	// starting n number of workers 
	for i := 0; i < numberofWorkers; i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}
        //Crawl deadline is 30 seconds, all workers shut down after 30 seconds
	deadline := time.After(30000 * time.Millisecond)
	go d.dispatch()
	for {
		select {
		case <-deadline:
			log.Println("workers close signal")
			for i := 0; i < numberofWorkers; i++ {
				quitroutines <- true
			}
			time.Sleep(5 * time.Second)
                        //Closing response pipeline
			close(UrlResQueue)
			return
		default:
		}

	}

}

func (d *UrlDispatcher) dispatch() {
	for job := range UrlFrontier {
		// a url job request has been received
		go func(job Job) {
			// try to obtain a crawl worker job channel that is available.
			// this will block until a crawl worker is idle
			jobChannel := <-d.WorkerPool

			// dispatch the job to the crawl worker job channel
			jobChannel <- job

		}(job)
	}
}

// Generator channel for URLs
// Writes url to url frontier
func toStream(urls ...string) {

	go func() {
		defer close(UrlFrontier)
		for _, url := range urls {
			UrlFrontier <- Job{"http://" + url}
		}
	}()
}

func main() {
	urldata = make(map[string]int)
	file, err := openLogFile(outputfilelog)
	if err != nil {
		log.Fatal(err)
	}
	sum := 0
	log.SetOutput(file)
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	start1 := time.Now()
	for _, line := range urls {
		for j, field := range line {
			if j == 1 {
				urllist = append(urllist, field)

			}
		}
	}
        
	toStream(urllist...)
        //Start Url dispatcher to assign url jobs to crawl workers 
	d := NewUrlDispatcher(MaxWorkers)
	d.Run(MaxWorkers)

        //Processing url response queue 
	for value := range UrlResQueue {
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
        //Url response data printed
	log.Println("Total Response sum", sum)
	log.Println("multi workers completed in", time.Since(start1))
	log.Println("Url Map", results.ContentLength)
	log.Println("TotalContentLength", results.TotalContentLength)
}

