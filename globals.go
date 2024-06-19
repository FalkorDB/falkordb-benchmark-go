package main

import (
	"github.com/HdrHistogram/hdrhistogram-go"
	"golang.org/x/time/rate"
	"math"
	"sync"
)

var totalCommands uint64
var totalEmptyResultsets uint64
var totalErrors uint64
var errorsPerQuery []uint64

var totalNodesCreated uint64
var totalNodesDeleted uint64
var totalLabelsAdded uint64
var totalPropertiesSet uint64
var totalRelationshipsCreated uint64
var totalRelationshipsDeleted uint64

var totalNodesCreatedPerQuery []uint64
var totalNodesDeletedPerQuery []uint64
var totalLabelsAddedPerQuery []uint64
var totalPropertiesSetPerQuery []uint64
var totalRelationshipsCreatedPerQuery []uint64
var totalRelationshipsDeletedPerQuery []uint64

var randIntPlaceholder = "__rand_int__"

// no locking is required when using the histograms. data is duplicated on the instant and overall histograms
var clientSideAllQueriesOverallLatencies *hdrhistogram.Histogram
var serverSideAllQueriesGraphInternalTimeOverallLatencies *hdrhistogram.Histogram

var clientSidePerQueryOverallLatencies []*hdrhistogram.Histogram
var serverSidePerQueryGraphInternalTimeOverallLatencies []*hdrhistogram.Histogram

// this mutex does not affect any of the client go-routines ( it's only to sync between main thread and datapoints processor go-routines )
var instantHistogramsResetMutex sync.Mutex
var clientSideAllQueriesInstantLatencies *hdrhistogram.Histogram
var serverSideAllQueriesGraphInternalTimeInstantLatencies *hdrhistogram.Histogram

const Inf = rate.Limit(math.MaxFloat64)

func createRequiredGlobalStructs(totalDifferentCommands int) {
	errorsPerQuery = make([]uint64, totalDifferentCommands)
	totalNodesCreatedPerQuery = make([]uint64, totalDifferentCommands)
	totalNodesDeletedPerQuery = make([]uint64, totalDifferentCommands)
	totalLabelsAddedPerQuery = make([]uint64, totalDifferentCommands)
	totalPropertiesSetPerQuery = make([]uint64, totalDifferentCommands)
	totalRelationshipsCreatedPerQuery = make([]uint64, totalDifferentCommands)
	totalRelationshipsDeletedPerQuery = make([]uint64, totalDifferentCommands)

	clientSideAllQueriesOverallLatencies = hdrhistogram.New(1, 90000000000, 4)
	clientSideAllQueriesInstantLatencies = hdrhistogram.New(1, 90000000000, 4)
	serverSideAllQueriesGraphInternalTimeOverallLatencies = hdrhistogram.New(1, 90000000000, 4)
	serverSideAllQueriesGraphInternalTimeInstantLatencies = hdrhistogram.New(1, 90000000000, 4)

	clientSidePerQueryOverallLatencies = make([]*hdrhistogram.Histogram, totalDifferentCommands)
	serverSidePerQueryGraphInternalTimeOverallLatencies = make([]*hdrhistogram.Histogram, totalDifferentCommands)
	for i := 0; i < totalDifferentCommands; i++ {
		clientSidePerQueryOverallLatencies[i] = hdrhistogram.New(1, 90000000000, 4)
		serverSidePerQueryGraphInternalTimeOverallLatencies[i] = hdrhistogram.New(1, 90000000000, 4)
	}
}

func resetInstantHistograms() {
	instantHistogramsResetMutex.Lock()
	clientSideAllQueriesInstantLatencies.Reset()
	serverSideAllQueriesGraphInternalTimeInstantLatencies.Reset()
	instantHistogramsResetMutex.Unlock()
}
