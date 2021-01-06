package lib

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/klog/v2"
)

type BasicAuth struct {
	Username string
	Password string
}

type CouchdbClient struct {
	BaseUri           string
	basicAuth         BasicAuth
	client            *http.Client
	ResetRequestCount func()
	GetRequestCount   func() int
}

type HttpError struct {
	Status     string
	StatusCode int
	RespBody   []byte
}

func (httpError *HttpError) Error() string {
	return fmt.Errorf("status %s (%d): %s", httpError.Status, httpError.StatusCode, httpError.RespBody).Error()
}

type MembershipResponse struct {
	AllNodes     []string `json:"all_nodes"`
	ClusterNodes []string `json:"cluster_nodes"`
}

func (c *CouchdbClient) getHealthByNodeName(urisByNodeName map[string]string) (map[string]HealthResponse, error) {
	healthByNodeName := make(map[string]HealthResponse)
	for name, uri := range urisByNodeName {
		var health HealthResponse

		// Instead of http://localhost:15984/_node/couchdb@172.16.238.13/_up
		// we have to request http://localhost:15984/_up,
		// but how do we find the correct base uris, say: ip and port, for each node?
		data, err := c.Request("GET", fmt.Sprintf("%s/_up", uri), nil)
		if err != nil {
			err = fmt.Errorf("error reading couchdb health: %v", err)
			//if !strings.Contains(err.Error(), "\"error\":\"nodedown\"") {
			//	return nil, err
			//}
			klog.Error(fmt.Errorf("continuing despite error: %v", err))
			continue
		}

		err = json.Unmarshal(data, &health)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling health: %v", err)
		}

		healthByNodeName[name] = health
	}

	if len(urisByNodeName) == 0 {
		return nil, fmt.Errorf("all nodes down")
	}

	return healthByNodeName, nil
}

func (c *CouchdbClient) getNodeInfo(uri string) (NodeInfo, error) {
	data, err := c.Request("GET", fmt.Sprintf("%s/", uri), nil)
	if err != nil {
		return NodeInfo{}, err
	}
	var root NodeInfo
	err = json.Unmarshal(data, &root)
	if err != nil {
		return NodeInfo{}, err
	}
	return root, nil
}

func (c *CouchdbClient) getServerVersion() (string, error) {
	nodeInfo, err := c.getNodeInfo(c.BaseUri)
	if err != nil {
		return "", err
	}
	return nodeInfo.Version, nil
}

func (c *CouchdbClient) isCouchDbV1() (bool, error) {
	serverVersion, err := c.getServerVersion()
	if err != nil {
		return false, err
	}

	versionParts := strings.Split(serverVersion, ".")
	major, err := strconv.Atoi(versionParts[0])
	if err != nil {
		return false, err
	}

	return major < 2, nil
}

func (c *CouchdbClient) GetNodeNames() ([]string, error) {
	data, err := c.Request("GET", fmt.Sprintf("%s/_membership", c.BaseUri), nil)
	if err != nil {
		return nil, err
	}
	var membership MembershipResponse
	err = json.Unmarshal(data, &membership)
	if err != nil {
		return nil, err
	}
	// for i, name := range membership.ClusterNodes {
	// 	klog.Infof("node[%d]: %s\n", i, name)
	// }
	return membership.ClusterNodes, nil
}

func (c *CouchdbClient) getNodeBaseUrisByNodeName(baseUri string) (map[string]string, error) {
	names, err := c.GetNodeNames()
	if err != nil {
		return nil, err
	}
	urisByNodeName := make(map[string]string)
	for _, name := range names {
		urisByNodeName[name] = fmt.Sprintf("%s/_node/%s", baseUri, name)
	}
	return urisByNodeName, nil
}

func (c *CouchdbClient) getStatsByNodeName(urisByNodeName map[string]string) (map[string]StatsResponse, error) {
	statsByNodeName := make(map[string]StatsResponse)
	for name, uri := range urisByNodeName {
		var stats StatsResponse
		data, err := c.Request("GET", fmt.Sprintf("%s/_stats", uri), nil)
		if err != nil {
			err = fmt.Errorf("error reading couchdb stats: %v", err)
			if !strings.Contains(err.Error(), "\"error\":\"nodedown\"") {
				return nil, err
			}

			stats.Up = 0
			klog.Error(fmt.Errorf("continuing despite error: %v", err))
			continue
		}

		stats.Up = 1

		err = json.Unmarshal(data, &stats)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling stats for node %s: %v", name, err)
		}

		// TODO this one is expected to retrieve other nodes' info
		nodeInfo, err := c.getNodeInfo(c.BaseUri)
		if err != nil {
			return nil, err
		}
		stats.NodeInfo = nodeInfo
		statsByNodeName[name] = stats
	}

	if len(urisByNodeName) == 0 {
		return nil, fmt.Errorf("all nodes down")
	}

	return statsByNodeName, nil
}

func (c *CouchdbClient) getSystemByNodeName(urisByNodeName map[string]string) (map[string]SystemResponse, error) {
	systemByNodeName := make(map[string]SystemResponse)
	for name, uri := range urisByNodeName {
		var stats SystemResponse

		data, err := c.Request("GET", fmt.Sprintf("%s/_system", uri), nil)
		if err != nil {
			err = fmt.Errorf("error reading couchdb system stats: %v", err)
			if !strings.Contains(err.Error(), "\"error\":\"nodedown\"") {
				return nil, err
			}
			klog.Error(fmt.Errorf("continuing despite error: %v", err))
			continue
		}

		err = json.Unmarshal(data, &stats)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling system stats for node %s: %v", name, err)
		}

		systemByNodeName[name] = stats
	}

	if len(urisByNodeName) == 0 {
		return nil, fmt.Errorf("all nodes down")
	}

	return systemByNodeName, nil
}

func (c *CouchdbClient) getStats(config CollectorConfig) (Stats, error) {
	isCouchDbV1, err := c.isCouchDbV1()
	if err != nil {
		return Stats{}, err
	}
	if !isCouchDbV1 {
		urisByNode, err := c.getNodeBaseUrisByNodeName(c.BaseUri)
		if err != nil {
			return Stats{}, err
		}
		//nodeHealth, err := c.getHealthByNodeName(urisByNode)
		//if err != nil {
		//	return Stats{}, err
		//}
		nodeStats, err := c.getStatsByNodeName(urisByNode)
		if err != nil {
			return Stats{}, err
		}
		databaseStats, err := c.getDatabasesStatsByDbName(config.ObservedDatabases, config.ConcurrentRequests)
		if err != nil {
			return Stats{}, err
		}
		if config.CollectViews {
			err := c.enhanceWithViewUpdateSeq(isCouchDbV1, databaseStats, config.ConcurrentRequests)
			if err != nil {
				return Stats{}, err
			}
		}
		schedulerJobs := SchedulerJobsResponse{}
		if config.CollectSchedulerJobs {
			schedulerJobs, err = c.getSchedulerJobs()
		}
		activeTasks, err := c.getActiveTasks()
		if err != nil {
			return Stats{}, err
		}
		databasesList, err := c.getDatabaseList()
		if err != nil {
			return Stats{}, err
		}
		systemStats, err := c.getSystemByNodeName(urisByNode)
		if err != nil {
			return Stats{}, err
		}

		return Stats{
			//HealthByNodeName:      nodeHealth,
			StatsByNodeName:       nodeStats,
			DatabasesTotal:        len(databasesList),
			DatabaseStatsByDbName: databaseStats,
			ActiveTasksResponse:   activeTasks,
			SchedulerJobsResponse: schedulerJobs,
			SystemByNodeName:      systemStats,
			ApiVersion:            "2"}, nil
	} else {
		urisByNode := map[string]string{
			"master": c.BaseUri,
		}
		nodeStats, err := c.getStatsByNodeName(urisByNode)
		if err != nil {
			return Stats{}, err
		}
		databaseStats, err := c.getDatabasesStatsByDbName(config.ObservedDatabases, config.ConcurrentRequests)
		if err != nil {
			return Stats{}, err
		}
		if config.CollectViews {
			err := c.enhanceWithViewUpdateSeq(isCouchDbV1, databaseStats, config.ConcurrentRequests)
			if err != nil {
				return Stats{}, err
			}
		}
		activeTasks, err := c.getActiveTasks()
		if err != nil {
			return Stats{}, err
		}
		databasesList, err := c.getDatabaseList()
		if err != nil {
			return Stats{}, err
		}
		return Stats{
			StatsByNodeName:       nodeStats,
			DatabasesTotal:        len(databasesList),
			DatabaseStatsByDbName: databaseStats,
			ActiveTasksResponse:   activeTasks,
			ApiVersion:            "1"}, nil
	}
}

type dbStatsResult struct {
	dbName  string
	dbStats DatabaseStats
	err     error
}

func (c *CouchdbClient) getDatabasesStatsByDbName(databases []string, concurrency uint) (map[string]DatabaseStats, error) {
	dbStatsByDbName := make(map[string]DatabaseStats)
	// Setup for concurrent scatter/gather scrapes, with concurrency limit
	r := make(chan dbStatsResult, len(databases))
	semaphore := NewSemaphore(concurrency) // semaphore to limit concurrency

	// scatter
	for _, dbName := range databases {
		dbName := dbName // rebind for closure to capture the value
		escapedDbName := url.QueryEscape(dbName)
		go func() {
			err := semaphore.Acquire()
			if err != nil {
				return
			}
			var dbStats DatabaseStats
			data, err := c.Request("GET", fmt.Sprintf("%s/%s", c.BaseUri, escapedDbName), nil)
			semaphore.Release()
			if err != nil {
				r <- dbStatsResult{err: fmt.Errorf("error reading database '%s' stats: %v", dbName, err)}
				return
			}

			err = json.Unmarshal(data, &dbStats)
			if err != nil {
				r <- dbStatsResult{err: fmt.Errorf("error unmarshalling database '%s' stats: %v", dbName, err)}
				return
			}
			if dbStats.DiskSize == 0 && dbStats.Sizes.File > 0 {
				dbStats.DiskSizeOverhead = dbStats.Sizes.File - dbStats.Sizes.Active
			} else {
				dbStats.DiskSizeOverhead = dbStats.DiskSize - dbStats.DataSize
			}
			if dbStats.CompactRunningBool {
				dbStats.CompactRunning = 1
			} else {
				dbStats.CompactRunning = 0
			}
			r <- dbStatsResult{dbName, dbStats, nil}
		}()
	}
	// gather
	for range databases {
		res := <-r
		if res.err != nil {
			semaphore.Abort()
			return nil, res.err
		}
		dbStatsByDbName[res.dbName] = res.dbStats
	}
	return dbStatsByDbName, nil
}

type viewStats struct {
	viewName  string
	updateSeq string
	warn      string
	err       error
}

func (c *CouchdbClient) viewStats(isCouchdbV1 bool, dbName string, designDocId string, viewName string) viewStats {
	escapedDbName := url.QueryEscape(dbName)

	query := strings.Join([]string{
		"stale=ok",
		"update=false",
		"stable=true",
		"update_seq=true",
		"include_docs=false",
		"limit=0",
	}, "&")
	var viewDoc ViewResponse
	viewDocData, err := c.Request("GET", fmt.Sprintf("%s/%s/%s/_view/%s?%s", c.BaseUri, escapedDbName, designDocId, viewName, query), nil)
	if err != nil {
		if httpError, ok := err.(*HttpError); ok == true {
			err = json.Unmarshal(httpError.RespBody, &viewDoc)
			if err != nil {
				return viewStats{err: fmt.Errorf("error unmarshalling http error response when requesting view '%s/%s/_view/%s': %v", dbName, designDocId, viewName, err)}
			}
			if viewDoc.Error != "" {
				return viewStats{err: fmt.Errorf("error reading view '%s/%s/_view/%s': %v", dbName, designDocId, viewName, errors.New(fmt.Sprintf("%s, reason: %s", viewDoc.Error, viewDoc.Reason)))}
			}
		}
		return viewStats{err: fmt.Errorf("error reading view '%s/%s/_view/%s': %v", dbName, designDocId, viewName, errors.New(fmt.Sprintf("%s, reason: %s", viewDoc.Error, viewDoc.Reason)))}
	}
	err = json.Unmarshal(viewDocData, &viewDoc)
	if err != nil {
		return viewStats{err: fmt.Errorf("error unmarshalling view doc for view '%s/%s/_view/%s': %v", dbName, designDocId, viewName, err)}
	}
	if viewDoc.Error != "" {
		return viewStats{err: fmt.Errorf("error reading view '%s/%s/_view/%s': %v", dbName, designDocId, viewName, errors.New(fmt.Sprintf("%s, reason: %s", viewDoc.Error, viewDoc.Reason)))}
	}

	var updateSeq string
	if isCouchdbV1 {
		updateSeq = c.updateSeqFromInt(viewDoc.UpdateSeq)
	} else {
		updateSeq = c.updateSeqFromString(viewDoc.UpdateSeq)
	}
	return viewStats{viewName, updateSeq, "", nil}
}

func (c *CouchdbClient) updateSeqFromInt(message json.RawMessage) string {
	var updateSeq int64
	err := json.Unmarshal(message, &updateSeq)
	if err != nil {
		klog.Warningf("%v", err)
	}
	return strconv.FormatInt(updateSeq, 10)
}

func (c *CouchdbClient) updateSeqFromString(message json.RawMessage) string {
	var updateSeq string
	err := json.Unmarshal(message, &updateSeq)
	if err != nil {
		klog.Warningf("%v", err)
	}
	return updateSeq
}

func (c *CouchdbClient) enhanceWithViewUpdateSeq(isCouchdbV1 bool, dbStatsByDbName map[string]DatabaseStats, concurrency uint) error {
	// Setup for concurrent scatter/gather scrapes, with concurrency limit
	r := make(chan dbStatsResult, len(dbStatsByDbName))
	semaphore := NewSemaphore(concurrency) // semaphore to limit concurrency

	// scatter
	for dbName, dbStats := range dbStatsByDbName {
		dbName := dbName   // rebind for closure to capture the value
		dbStats := dbStats // rebind for closure to capture the value
		escapedDbName := url.QueryEscape(dbName)
		go func() {
			err := semaphore.Acquire()
			if err != nil {
				return
			}
			query := strings.Join([]string{
				"startkey=\"_design/\"",
				"endkey=\"_design0\"",
				"include_docs=true",
			}, "&")
			designDocData, err := c.Request("GET", fmt.Sprintf("%s/%s/_all_docs?%s", c.BaseUri, escapedDbName, query), nil)
			semaphore.Release()
			if err != nil {
				r <- dbStatsResult{err: fmt.Errorf("error reading database '%s' stats: %v", dbName, err)}
				return
			}

			var designDocs DocsResponse
			err = json.Unmarshal(designDocData, &designDocs)
			if err != nil {
				r <- dbStatsResult{err: fmt.Errorf("error unmarshalling design docs for database '%s': %v", dbName, err)}
				return
			}
			views := make(ViewStatsByDesignDocName)

			viewmutex := &sync.Mutex{}
			done := make(chan struct{}, len(designDocs.Rows))
			for _, row := range designDocs.Rows {
				row := row
				go func() {
					defer func() {
						done <- struct{}{}
					}()
					updateSeqByView := make(ViewStats)
					v := make(chan viewStats, len(row.Doc.Views))
					for viewName := range row.Doc.Views {
						viewName := viewName
						if dbStats.Props.Partitioned {
							v <- viewStats{warn: fmt.Sprintf("partitioned database /%s currently not supported for view stats", dbName)}
							continue
						}
						go func() {
							//klog.Infof("/%s/%s/_view/%s\n", dbName, row.Doc.Id, viewName)
							err := semaphore.Acquire()
							if err != nil {
								// send something to parent coroutine so it doesn't block forever on receive
								v <- viewStats{err: fmt.Errorf("aborted view stats for /%s/%s/_view/%s", dbName, row.Doc.Id, viewName)}
								return
							}
							defer semaphore.Release()
							v <- c.viewStats(isCouchdbV1, dbName, row.Doc.Id, viewName)
						}()
					}
					for range row.Doc.Views {
						res := <-v
						if res.warn != "" {
							// TODO consider adding a metric to make warnings more visible
							//klog.Warning(res.warn)
						}
						if res.err != nil {
							// TODO consider adding a metric to make errors more visible
							klog.Error(res.err)
							continue
							//r <- dbStatsResult{err: res.err}
							//return
						}
						if res.updateSeq != "" {
							updateSeqByView[res.viewName] = res.updateSeq
						}
					}
					viewmutex.Lock()
					views[row.Doc.Id] = updateSeqByView
					viewmutex.Unlock()

				}()
			}
			for range designDocs.Rows {
				<-done
			}
			dbStats.Views = views
			r <- dbStatsResult{dbName, dbStats, nil}
		}()
	}

	// gather
	for range dbStatsByDbName {
		resp := <-r
		dbName, dbStats, err := resp.dbName, resp.dbStats, resp.err
		if err != nil {
			semaphore.Abort() // let any goroutines waiting on semaphores terminate
			return err
		}
		dbStatsByDbName[dbName] = dbStats
	}
	return nil
}

// CouchDB 2.x+ only
func (c *CouchdbClient) getSchedulerJobs() (SchedulerJobsResponse, error) {
	data, err := c.Request("GET", fmt.Sprintf("%s/_scheduler/jobs", c.BaseUri), nil)
	if err != nil {
		return SchedulerJobsResponse{}, fmt.Errorf("error reading scheduler jobs: %v", err)
	}

	var schedulerJobs SchedulerJobsResponse
	err = json.Unmarshal(data, &schedulerJobs)
	if err != nil {
		return SchedulerJobsResponse{}, fmt.Errorf("error unmarshalling scheduler jobs: %v", err)
	}
	//for _, job := range schedulerJobs.Jobs {
	//	replDoc, err := c.Request("GET", fmt.Sprintf("%s/%s/%s", c.BaseUri, job.Database, job.DocID), nil)
	//	if err != nil {
	//		return SchedulerJobsResponse{}, fmt.Errorf("error reading replication doc '%s/%s': %v", job.Database, job.DocID, err)
	//	}
	//}
	return schedulerJobs, nil
}

func (c *CouchdbClient) getActiveTasks() (ActiveTasksResponse, error) {
	data, err := c.Request("GET", fmt.Sprintf("%s/_active_tasks", c.BaseUri), nil)
	if err != nil {
		return ActiveTasksResponse{}, fmt.Errorf("error reading active tasks: %v", err)
	}

	var activeTasks ActiveTasksResponse
	err = json.Unmarshal(data, &activeTasks)
	if err != nil {
		return ActiveTasksResponse{}, fmt.Errorf("error unmarshalling active tasks: %v", err)
	}
	for _, activeTask := range activeTasks {
		// CouchDB 1.x doesn't know anything about nodes.
		if activeTask.Node == "" {
			activeTask.Node = "master"
		}
	}
	return activeTasks, nil
}

func (c *CouchdbClient) getDatabaseList() ([]string, error) {
	data, err := c.Request("GET", fmt.Sprintf("%s/%s", c.BaseUri, AllDbs), nil)
	if err != nil {
		return nil, err
	}
	var dbs []string
	err = json.Unmarshal(data, &dbs)
	if err != nil {
		return nil, err
	}
	return dbs, nil
}

func (c *CouchdbClient) Request(method string, uri string, body io.Reader) (respData []byte, err error) {
	req, err := http.NewRequest(method, uri, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header = http.Header{
			"Content-Type": []string{"application/json"},
		}
	}
	if len(c.basicAuth.Username) > 0 {
		req.SetBasicAuth(c.basicAuth.Username, c.basicAuth.Password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp != nil {
		defer func() {
			if cerr := resp.Body.Close(); cerr != nil {
				err = cerr
			}
		}()
	}

	respData, err = ioutil.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		if err != nil {
			respData = []byte(err.Error())
		}
		return nil, &HttpError{resp.Status, resp.StatusCode, respData}
	}
	if err != nil {
		return nil, err
	}
	return respData, nil
}

type requestCountingRoundTripper struct {
	RequestCount int64
	rt           http.RoundTripper
}

func (rt *requestCountingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	atomic.AddInt64(&rt.RequestCount, 1)
	//klog.Infof("req[%d] %s", atomic.LoadInt64(&rt.RequestCount), req.URL.String())
	return rt.rt.RoundTrip(req)
}

func NewCouchdbClient(uri string, basicAuth BasicAuth, insecure bool) *CouchdbClient {
	countingRoundTripper := &requestCountingRoundTripper{
		0,
		&http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
		},
	}

	httpClient := &http.Client{
		Transport: countingRoundTripper,
	}

	return &CouchdbClient{
		BaseUri:   uri,
		basicAuth: basicAuth,
		client:    httpClient,
		ResetRequestCount: func() {
			atomic.StoreInt64(&countingRoundTripper.RequestCount, 0)
		},
		GetRequestCount: func() int {
			return int(atomic.LoadInt64(&countingRoundTripper.RequestCount))
		},
	}
}
