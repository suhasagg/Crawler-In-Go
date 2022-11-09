Solution- 1 (using semaphores - concurrency limit concurrency pattern)

Configuration Parameters - 

To define urls - url csv
urls = readCsvFile("/home/swordfish/Downloads/urls.csv")
//Url database used consists of all the different types of urls - normal response, error from url or url timing out (to consider different cases)

numberofRetries := 0 
//Use this option for exponential backoff based retries 
//This covers the scenario where url is being throttled and multiple attempts might be needed to download the page

maxConcurrency := 60
// We set the max concurrency to 60 (semaphore capacity) - which means limit the goroutine to run
// a maximum of 60 at a time
// This can be changed according to number of urls

//Please specify output file path
outputfilelog = "crawlersemaphores.log"

Program is able to process size of urls before timing out in most cases.
Crawling go routines receive a shudown signal after 30 seconds via context with 30 seconds deadlines, url responses are accumulated till 30 seconds only.

url data is printed in the logs.

Url Response Data.

log.Println("Total Response sum", sum)
log.Println("multi workers completed in", time.Since(start1))
log.Println("Url Map", results.ContentLength)
log.Println("TotalContentLength", results.TotalContentLength)


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




Solution - 2 ( Job/Worker concurrency pattern )
2-level channel system, one for queuing jobs and another to control how many workers operate on the JobQueue concurrently.


Configuration Parameters - 

var (
       //Maximum number of crawling workers
	MaxWorker = 50

       //Maximum size of url frontier/job queue
	MaxQueue  = 1000

       //Exponential backoff retries configuration (default = 0)

        Numberofretries = 0

        databasePath = "100urls.csv"

        //To define urls - url csv database
        //Url database used consists of all the different types of urls - normal response, error from url or url timing out (to consider different cases)
        Program is able to process size of urls before timing out in most cases.
        Crawling workers receive a shudown signal after 30 seconds via their respective quit channels, url responses are accumulated till 30 seconds only.

        outputfilelog = "crawlerworkerqueue.log"

)


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

        numberofWorkers := 50

	toStream(urllist...)

        //Start Url dispatcher to assign url jobs to crawl workers 
	d := NewUrlDispatcher(numberofWorkers)
	d.Run()

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
        //Url Response Data.
	log.Println("Total Response sum", sum)
	log.Println("multi workers completed in", time.Since(start1))
	log.Println("Url Map", results.ContentLength)
	log.Println("TotalContentLength", results.TotalContentLength)
}
