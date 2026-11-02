package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

type Result struct {
	at         time.Time
	elapsed    time.Duration
	status     int
	err        error
	retryAfter string
}

type Config struct {
	urls     []string
	method   string
	clientID string
	nClient  int
	nConcur  int
	nRequest int
}

func main() {
	url := flag.String("url", "http://localhost:8080/api/hello", "target URL")
	method := flag.String("method", "GET", "http method")
	clientID := flag.String("client-id", "rainatapw", "client id")
	nClient := flag.Int("n-client", 1, "number of clients")
	nRequest := flag.Int("n", 10, "n requests")
	nConcur := flag.Int("c", 3, "n concurrent")
	rate := flag.Int("rate", 30, "n requests per second")
	flag.Parse()

	cfg := &Config{
		urls:     getUrls(*url),
		method:   *method,
		clientID: *clientID,
		nClient:  *nClient,
		nConcur:  *nConcur,
		nRequest: *nRequest,
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = cfg.nConcur // set the number of connection pool

	client := http.Client{Timeout: 5 * time.Second, Transport: transport}
	tick, stopTicker := getTickChannel(*rate)
	defer stopTicker()

	statusCountByTime := make(map[time.Time]map[int]int)

	results := make(chan Result)
	jobs := make(chan int)

	var wg sync.WaitGroup

	runWorkers(&wg, jobs, results, &client, cfg)
	runProducer(cfg.nRequest, tick, jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	for res := range results {
		t := res.at.Truncate(time.Second)
		if _, ok := statusCountByTime[t]; !ok {
			statusCountByTime[t] = make(map[int]int)
		}
		statusCountByTime[t][res.status]++
	}

	keys := slices.Collect(maps.Keys(statusCountByTime))

	slices.SortFunc(keys, func(a, b time.Time) int {
		return a.Compare(b)
	})

	for _, t := range keys {
		statusCount := statusCountByTime[t]
		okCount := statusCount[200]
		queuedCount := statusCount[202]
		rateLimitedCount := statusCount[429]
		errCount := statusCount[0]
		fmt.Printf("%s\n200:%d\n202:%d\n429:%d\nerr:%d\n\n", t.Format("15:04:05"), okCount, queuedCount, rateLimitedCount, errCount)
	}
}

func runProducer(nRequest int, tick <-chan time.Time, jobs chan int) {
	go func() {
		for i := range nRequest {
			if tick != nil {
				<-tick
			}
			jobs <- i
		}
		close(jobs)
	}()
}

func runWorkers(wg *sync.WaitGroup, jobs chan int, results chan Result, client *http.Client, cfg *Config) {
	for range cfg.nConcur {
		wg.Go(func() {
			for i := range jobs {
				assignedUrl := cfg.urls[i%len(cfg.urls)]
				assignedClientID := fmt.Sprintf("%s-%d", cfg.clientID, i%cfg.nClient)
				results <- send(client, cfg.method, assignedUrl, assignedClientID)
			}
		})
	}
}

func getTickChannel(rate int) (<-chan time.Time, func()) {
	var tick <-chan time.Time
	if rate > 0 {
		ticker := time.NewTicker(time.Second / time.Duration(rate))
		tick = ticker.C
		return tick, ticker.Stop
	}

	return nil, func() {}
}

func send(client *http.Client, method, url, clientID string) Result {
	start := time.Now()

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		log.Printf("failed creating a request: %v", err)
		return Result{err: err, at: start, elapsed: time.Since(start)}
	}

	req.Header.Set("X-Client-ID", clientID)

	resp, err := client.Do(req)

	elapsed := time.Since(start)

	if err != nil {
		log.Printf("error sending the request: %v", err)
		return Result{err: err, at: start, elapsed: elapsed}
	}

	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	return Result{
		at:         start,
		elapsed:    elapsed,
		status:     resp.StatusCode,
		retryAfter: resp.Header.Get("Retry-After"),
	}
}

func getUrls(urls string) []string {
	return strings.Split(urls, ",")
}
