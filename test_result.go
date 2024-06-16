package main

import (
	"encoding/json"
	"fmt"
	"github.com/HdrHistogram/hdrhistogram-go"
	"log"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const resultFormatVersion = "0.0.1"

type GraphQueryDatapoint struct {
	CmdPos                      int // command that was used
	ClientDurationMicros        int64
	GraphInternalDurationMicros int64
	Error                       bool
	Empty                       bool
	NodesCreated                uint64
	NodesDeleted                uint64
	LabelsAdded                 uint64
	PropertiesSet               uint64
	RelationshipsCreated        uint64
	RelationshipsDeleted        uint64
}

type TestResult struct {

	// Test Configs
	ResultFormatVersion              string `json:"ResultFormatVersion"`
	Metadata                         string `json:"Metadata"`
	Clients                          uint64 `json:"Clients"`
	MaxRps                           uint64 `json:"MaxRps"`
	RandomSeed                       int64  `json:"RandomSeed"`
	BenchmarkConfiguredCommandsLimit uint64 `json:"BenchmarkConfiguredCommandsLimit"`
	IssuedCommands                   uint64 `json:"IssuedCommands"`
	BenchmarkFullyRun                bool   `json:"BenchmarkFullyRun"`

	// Test Description
	TestDescription string `json:"TestDescription"`

	// DB Spefic Configs
	DBSpecificConfigs map[string]interface{} `json:"DBSpecificConfigs"`

	StartTime      int64 `json:"StartTime"`
	EndTime        int64 `json:"EndTime"`
	DurationMillis int64 `json:"DurationMillis"`

	// Populated after benchmark
	// Benchmark Totals
	Totals map[string]interface{} `json:"Totals"`

	// Overall Rates
	OverallQueryRates map[string]interface{} `json:"OverallQueryRates"`

	// Overall Client Quantiles
	OverallClientLatencies map[string]interface{} `json:"OverallClientLatencies"`

	// Overall Graph Internal Quantiles
	OverallGraphInternalLatencies map[string]interface{} `json:"OverallGraphInternalLatencies"`

	// Relative Internal External Latencies Differences
	RelativeInternalExternalLatencyDiff map[string]float64 `json:"OverallRelativeInternalExternalLatencyDiff"`

	// Relative Internal External Latencies Differences
	AbsoluteInternalExternalLatencyDiff map[string]float64 `json:"OverallAbsoluteInternalExternalLatencyDiff"`

	// Per second ( tick ) client stats
	ClientRunTimeStats map[int64]interface{} `json:"ClientRunTimeStats"`

	// Per second ( tick ) server stats
	ServerRunTimeStats map[int64]interface{} `json:"ServerRunTimeStats"`
}

func NewTestResult(metadata string, clients uint64, commandsLimit uint64, maxRps uint64, testDescription string) *TestResult {
	return &TestResult{ResultFormatVersion: resultFormatVersion, BenchmarkConfiguredCommandsLimit: commandsLimit, BenchmarkFullyRun: false, Metadata: metadata, Clients: clients, MaxRps: maxRps, TestDescription: testDescription}
}

func (r *TestResult) SetUsedRandomSeed(seed int64) *TestResult {
	r.RandomSeed = seed
	return r
}

func (r *TestResult) FillDurationInfo(startTime time.Time, endTime time.Time, duration time.Duration) {
	r.StartTime = startTime.UTC().UnixNano() / 1000000
	r.EndTime = endTime.UTC().UnixNano() / 1000000
	r.DurationMillis = duration.Milliseconds()
}

func processGraphDatapointsChannel(graphStatsChann chan GraphQueryDatapoint, c chan os.Signal, numberRequests uint64, wg *sync.WaitGroup, instantMutex *sync.Mutex) {
	defer wg.Done()
	var totalProcessedCommands uint64 = 0
	for {
		select {
		case dp := <-graphStatsChann:
			{
				cmdPos := dp.CmdPos
				clientDurationMicros := dp.ClientDurationMicros
				instantMutex.Lock()
				clientSidePerQueryOverallLatencies[cmdPos].RecordValue(clientDurationMicros)
				clientSideAllQueriesOverallLatencies.RecordValue(clientDurationMicros)
				graphInternalDurationMicros := dp.GraphInternalDurationMicros
				serverSidePerQueryGraphInternalTimeOverallLatencies[cmdPos].RecordValue(graphInternalDurationMicros)
				serverSideAllQueriesGraphInternalTimeOverallLatencies.RecordValue(graphInternalDurationMicros)
				instantMutex.Unlock()
				// Only needs to be atomic due to CLI print
				atomic.AddUint64(&totalCommands, uint64(1))
				if dp.Error {
					// Only needs to be atomic due to CLI print
					atomic.AddUint64(&totalErrors, uint64(1))
					errorsPerQuery[cmdPos]++
				} else {
					totalNodesCreated = totalNodesCreated + dp.NodesCreated
					totalNodesDeleted = totalNodesDeleted + dp.NodesDeleted
					totalLabelsAdded = totalLabelsAdded + dp.LabelsAdded
					totalPropertiesSet = totalPropertiesSet + dp.PropertiesSet
					totalRelationshipsCreated = totalRelationshipsCreated + dp.RelationshipsCreated
					totalRelationshipsDeleted = totalRelationshipsDeleted + dp.RelationshipsDeleted

					totalNodesCreatedPerQuery[cmdPos] = totalNodesCreatedPerQuery[cmdPos] + dp.NodesCreated
					totalNodesDeletedPerQuery[cmdPos] = totalNodesDeletedPerQuery[cmdPos] + dp.NodesDeleted
					totalLabelsAddedPerQuery[cmdPos] = totalLabelsAddedPerQuery[cmdPos] + dp.LabelsAdded
					totalPropertiesSetPerQuery[cmdPos] = totalPropertiesSetPerQuery[cmdPos] + dp.PropertiesSet
					totalRelationshipsCreatedPerQuery[cmdPos] = totalRelationshipsCreatedPerQuery[cmdPos] + dp.RelationshipsCreated
					totalRelationshipsDeletedPerQuery[cmdPos] = totalRelationshipsDeletedPerQuery[cmdPos] + dp.RelationshipsDeleted

					if dp.Empty {
						totalEmptyResultsets++
					}
				}

				instantMutex.Lock()
				clientSideAllQueriesInstantLatencies.RecordValue(clientDurationMicros)
				serverSideAllQueriesGraphInternalTimeInstantLatencies.RecordValue(graphInternalDurationMicros)
				instantMutex.Unlock()

				totalProcessedCommands++
				// if all commands have been processed return
				// otherwise keep looping
				if totalProcessedCommands >= numberRequests {
					return
				}
				break
			}

		case <-c:
			fmt.Println("\nReceived Ctrl-c - shutting down datapoints processor go-routine")
			return
		}
	}
}

func saveJsonResult(testResult *TestResult, jsonOutputFile string) {
	file, err := json.MarshalIndent(testResult, "", " ")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Saving JSON results file to %s\n", jsonOutputFile)
	err = os.WriteFile(jsonOutputFile, file, 0644)
	if err != nil {
		log.Fatal(err)
	}
}

func calculateRateMetrics(current, prev int64, took time.Duration) (rate float64) {
	rate = float64(current-prev) / took.Seconds()
	return
}

func generateLatenciesMap(hist *hdrhistogram.Histogram) (int64, map[string]float64) {
	ops := hist.TotalCount()
	q0 := 0.0
	q50 := 0.0
	q95 := 0.0
	q99 := 0.0
	q999 := 0.0
	q100 := 0.0
	average := 0.0
	if ops > 0 {
		q0 = float64(hist.ValueAtQuantile(0.0)) / 10e2
		q50 = float64(hist.ValueAtQuantile(50.0)) / 10e2
		q95 = float64(hist.ValueAtQuantile(95.0)) / 10e2
		q99 = float64(hist.ValueAtQuantile(99.0)) / 10e2
		q999 = float64(hist.ValueAtQuantile(99.90)) / 10e2
		q100 = float64(hist.ValueAtQuantile(100.0)) / 10e2
		average = (hist.Mean() / float64(1000.0))
	}

	mp := map[string]float64{"q0": q0, "q50": q50, "q95": q95, "q99": q99, "q999": q999, "q100": q100, "avg": average}
	return ops, mp
}

func GetOverallLatencies(cmds []string, perQueryHistograms []*hdrhistogram.Histogram, totalsHistogram *hdrhistogram.Histogram) (map[string]interface{}, map[string]float64) {
	perQueryQuantileMap := map[string]interface{}{}
	for i, query := range cmds {
		_, quantileMap := generateLatenciesMap(perQueryHistograms[i])
		perQueryQuantileMap[query] = quantileMap
	}
	_, totalMap := generateLatenciesMap(totalsHistogram)
	perQueryQuantileMap["Total"] = totalMap
	return perQueryQuantileMap, totalMap
}

func GenerateInternalExternalRatioLatencies(internal map[string]float64, external map[string]float64) (ratioMap map[string]float64, absoluteMap map[string]float64) {
	ratioMap = map[string]float64{}
	absoluteMap = map[string]float64{}
	for quantile, internalQuantileValue := range internal {

		externalQuantileValue := external[quantile]
		absoluteDiff := externalQuantileValue - internalQuantileValue
		relativeDiff := externalQuantileValue / internalQuantileValue
		if !math.IsNaN(relativeDiff) {
			ratioMap[quantile] = relativeDiff
		}
		if !math.IsNaN(absoluteDiff) {
			absoluteMap[quantile] = absoluteDiff
		}
	}
	return
}

func GetOverallRatesMap(took time.Duration, cmds []string, perQueryHistograms []*hdrhistogram.Histogram, totalsHistogram *hdrhistogram.Histogram) map[string]interface{} {
	/////////
	// Overall Rates
	/////////
	perQueryRatesMap := map[string]interface{}{}
	for i, query := range cmds {
		count := perQueryHistograms[i].TotalCount()
		rate := calculateRateMetrics(count, 0, took)
		perQueryRatesMap[query] = rate
	}
	count := totalsHistogram.TotalCount()
	rate := calculateRateMetrics(count, 0, took)
	perQueryRatesMap["Total"] = rate
	return perQueryRatesMap
}

func GetTotalsMap(queries []string, latenciesPerQuery []*hdrhistogram.Histogram, totalLatencies *hdrhistogram.Histogram, errorsPerQuery, totalNodesCreatedPerQuery, totalNodesDeletedPerQuery, totalLabelsAddedPerQuery, totalPropertiesSetPerQuery, totalRelationshipsCreatedPerQuery, totalRelationshipsDeletedPerQuery []uint64) map[string]interface{} {
	totalsMap := map[string]interface{}{}

	for i, query := range queries {
		totalsMap[query] = generateTotalMap(uint64(latenciesPerQuery[i].TotalCount()), errorsPerQuery[i], totalNodesCreatedPerQuery[i], totalNodesDeletedPerQuery[i], totalLabelsAddedPerQuery[i], totalPropertiesSetPerQuery[i], totalRelationshipsCreatedPerQuery[i], totalRelationshipsDeletedPerQuery[i])
	}
	totalsMap["Total"] = generateTotalMap(uint64(totalLatencies.TotalCount()), CountTotal(errorsPerQuery), CountTotal(totalNodesCreatedPerQuery), CountTotal(totalNodesDeletedPerQuery), CountTotal(totalLabelsAddedPerQuery), CountTotal(totalPropertiesSetPerQuery), CountTotal(totalRelationshipsCreatedPerQuery), CountTotal(totalRelationshipsDeletedPerQuery))
	return totalsMap
}

func CountTotal(slice []uint64) (res uint64) {
	res = 0
	for _, i2 := range slice {
		res += i2
	}
	return
}

func generateTotalMap(IssuedQueries, Errors, NodesCreated, NodesDeleted, LabelsAdded, PropertiesSet, RelationshipsCreated, RelationshipsDeleted uint64) interface{} {
	mp := map[string]uint64{"IssuedQueries": IssuedQueries, "Errors": Errors, "NodesCreated": NodesCreated, "NodesDeleted": NodesDeleted, "LabelsAdded": LabelsAdded, "PropertiesSet": PropertiesSet, "RelationshipsCreated": RelationshipsCreated, "RelationshipsDeleted": RelationshipsDeleted}
	return mp
}
