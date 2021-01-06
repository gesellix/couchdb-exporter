package lib

import (
	"encoding/json"
	"time"
)

type Counter struct {
	// v1.x api
	Description string
	Current     float64
	// v2.x api
	Value float64
	Type  string
	Desc  string
}

type Percent map[int]float64

// v2.x api
type HistogramValue struct {
	Min               float64     `json:"min"`
	Max               float64     `json:"max"`
	ArithmeticMean    float64     `json:"arithmetic_mean"`
	GeometricMean     float64     `json:"geometric_mean"`
	HarmonicMean      float64     `json:"harmonic_mean"`
	Median            float64     `json:"median"`
	Variance          float64     `json:"variance"`
	StandardDeviation float64     `json:"standard_deviation"`
	Skewness          float64     `json:"skewness"`
	Kurtosis          float64     `json:"kurtosis"`
	Percentile        [][]float64 `json:"percentile"`
	N                 float64     `json:"n"`
}

type Histogram struct {
	// v1.x api
	Description string
	Current     float64 `json:"current"`
	Min         float64 `json:"min"`
	Max         float64 `json:"max"`
	Mean        float64 `json:"mean"`
	Sum         float64 `json:"sum"`
	Stddev      float64 `json:"stddev"`

	// v2.x api
	Value HistogramValue
	Type  string
	Desc  string
}

type CouchdbStats struct {
	// v1.x, and v2.x api
	AuthCacheHits   Counter   `json:"auth_cache_hits"`
	AuthCacheMisses Counter   `json:"auth_cache_misses"`
	DatabaseReads   Counter   `json:"database_reads"`
	DatabaseWrites  Counter   `json:"database_writes"`
	OpenDatabases   Counter   `json:"open_databases"`
	OpenOsFiles     Counter   `json:"open_os_files"`
	RequestTime     Histogram `json:"request_time"`
	// v2.x api
	Httpd               Httpd               `json:"httpd"`
	HttpdRequestMethods HttpdRequestMethods `json:"httpd_request_methods"`
	HttpdStatusCodes    HttpdStatusCodes    `json:"httpd_status_codes"`
}

type MangoStats struct {
	UnindexedQueries   Counter   `json:"unindexed_queries"`
	QueryInvalidIndex  Counter   `json:"query_invalid_index"`
	TooManyDocs        Counter   `json:"too_many_docs_scanned"`
	DocsExamined       Counter   `json:"docs_examined"`
	QuorumDocsExamined Counter   `json:"quorum_docs_examined"`
	ResultsReturned    Counter   `json:"results_returned"`
	QueryTime          Histogram `json:"query_time"`
	EvaluateSelector   Counter   `json:"evaluate_selector"`
}

type HttpdRequestMethods struct {
	COPY   Counter `json:"COPY"`
	DELETE Counter `json:"DELETE"`
	GET    Counter `json:"GET"`
	HEAD   Counter `json:"HEAD"`
	POST   Counter `json:"POST"`
	PUT    Counter `json:"PUT"`
}

type HttpdStatusCodes map[string]Counter

type Httpd struct {
	BulkRequests             Counter `json:"bulk_requests"`
	ClientsRequestingChanges Counter `json:"clients_requesting_changes"`
	Requests                 Counter `json:"requests"`
	TemporaryViewReads       Counter `json:"temporary_view_reads"`
	ViewReads                Counter `json:"view_reads"`
}

type NodeFeatures []string

type Vendor struct {
	Name string `json:"name"`
}

type NodeInfo struct {
	Couchdb  string       `json:"couchdb"`
	Features NodeFeatures `json:"features"`
	Vendor   Vendor       `json:"vendor"`
	Version  string       `json:"version"`
}

type LogLevel struct {
	Level map[string]Counter `json:"level"`
}

type Fabric struct {
	Worker      map[string]Counter `json:"worker"`
	OpenShard   map[string]Counter `json:"open_shard"`
	ReadRepairs map[string]Counter `json:"read_repairs"`
	DocUpdate   map[string]Counter `json:"doc_update"`
}

type CouchReplicator struct {
	ChangesReadFailures  Counter            `json:"changes_read_failures"`
	ChangesReaderDeaths  Counter            `json:"changes_reader_deaths"`
	ChangesManagerDeaths Counter            `json:"changes_manager_deaths"`
	ChangesQueueDeaths   Counter            `json:"changes_queue_deaths"`
	Checkpoints          map[string]Counter `json:"checkpoints"`
	FailedStarts         Counter            `json:"failed_starts"`
	Requests             Counter            `json:"requests"`
	Responses            map[string]Counter `json:"responses"`
	StreamResponses      map[string]Counter `json:"stream_responses"`
	WorkerDeaths         Counter            `json:"worker_deaths"`
	WorkersStarted       Counter            `json:"workers_started"`
	ClusterIsStable      Counter            `json:"cluster_is_stable"`
	DbScans              Counter            `json:"db_scans"`
	Docs                 map[string]Counter `json:"docs"`
	Jobs                 map[string]Counter `json:"jobs"`
	Connection           map[string]Counter `json:"connection"`
}

type HealthResponse struct {
	Status string `json:"status"`
	// see https://github.com/apache/couchdb/pull/1658
	//Seeds map[string]SeedStat `json:"seeds"`
}

type StatsResponse struct {
	Couchdb  CouchdbStats `json:"couchdb"`
	Mango    MangoStats   `json:"mango"`
	Up       float64      `json:"-"`
	NodeInfo NodeInfo     `json:"-"`
	// v1.x api
	Httpd               Httpd               `json:"httpd"`
	HttpdRequestMethods HttpdRequestMethods `json:"httpd_request_methods"`
	HttpdStatusCodes    HttpdStatusCodes    `json:"httpd_status_codes"`
	// v2.x api
	CouchLog        LogLevel        `json:"couch_log"`
	Fabric          Fabric          `json:"fabric"`
	CouchReplicator CouchReplicator `json:"couch_replicator"`
}

type View map[string]interface{}

type Doc struct {
	Id    string          `json:"_id"`
	Views map[string]View `json:"views"`
}

type Row struct {
	Id  string `json:"id"`
	Doc Doc    `json:"doc"`
}

type Rows []Row

type DocsResponse struct {
	Rows Rows `json:"rows"`
}

type ViewResponse struct {
	UpdateSeq json.RawMessage `json:"update_seq"`
	Error     string          `json:"error,omitempty"`
	Reason    string          `json:"reason,omitempty"`
}

type ViewStats map[string]string

type ViewStatsByDesignDocName map[string]ViewStats

// v2.x api
type DatabaseSizes struct {
	Active   float64 `json:"active"`   // data_size
	File     float64 `json:"file"`     // disk_size
	External float64 `json:"external"` // uncompressed database content size
}

// v3.x api
type DatabaseProps struct {
	Partitioned bool `json:"partitioned"`
}

type DatabaseStats struct {
	DataSize           float64       `json:"data_size"`
	DiskSize           float64       `json:"disk_size"`
	Sizes              DatabaseSizes `json:"sizes,omitempty"`
	DiskSizeOverhead   float64
	DocCount           float64 `json:"doc_count"`
	DocDelCount        float64 `json:"doc_del_count"`
	CompactRunningBool bool    `json:"compact_running"`
	CompactRunning     float64
	DiskFormatVersion  float64         `json:"disk_format_version"`
	UpdateSeq          json.RawMessage `json:"update_seq"`
	Views              ViewStatsByDesignDocName
	Props              DatabaseProps `json:"props,omitempty"`
}

type DatabaseStatsByDbName map[string]DatabaseStats

type ActiveTask struct {
	Type       string  `json:"type"`
	Node       string  `json:"node,omitempty"`
	Continuous bool    `json:"continuous,omitempty"`
	UpdatedOn  float64 `json:"updated_on,omitempty"`
	Source     string  `json:"source,omitempty"`
	Target     string  `json:"target,omitempty"`
	DocId      string  `json:"doc_id,omitempty"`
}

type ActiveTasksResponse []ActiveTask

type SchedulerJobsResponse struct {
	TotalRows int `json:"total_rows"`
	Offset    int `json:"offset"`
	Jobs      []struct {
		Database string `json:"database"`
		ID       string `json:"id"`
		Pid      string `json:"pid"`
		Source   string `json:"source"`
		Target   string `json:"target"`
		User     string `json:"user"`
		DocID    string `json:"doc_id"`
		History  []struct {
			Timestamp time.Time `json:"timestamp"`
			Type      string    `json:"type"`
		} `json:"history"`
		Node      string    `json:"node"`
		StartTime time.Time `json:"start_time"`
	} `json:"jobs"`
}

type MemoryStats struct {
	// v2.x api
	Other         float64 `json:"other"`
	Atom          float64 `json:"atom"`
	AtomUsed      float64 `json:"atom_used"`
	Processes     float64 `json:"processes"`
	ProcessesUsed float64 `json:"processes_used"`
	Binary        float64 `json:"binary"`
	Code          float64 `json:"code"`
	Ets           float64 `json:"ets"`
}

type SystemResponse struct {
	MemoryStatsResponse MemoryStats `json:"memory"`
}

type Stats struct {
	//HealthByNodeName      map[string]HealthResponse
	StatsByNodeName       map[string]StatsResponse
	DatabasesTotal        int
	DatabaseStatsByDbName DatabaseStatsByDbName
	ActiveTasksResponse   ActiveTasksResponse
	// SchedulerJobsResponse: CouchDB 2.x+ only
	SchedulerJobsResponse SchedulerJobsResponse
	SystemByNodeName      map[string]SystemResponse
	ApiVersion            string
}
