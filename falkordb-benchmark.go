package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/FalkorDB/falkordb-go"
	"github.com/gomodule/redigo/redis"
	"golang.org/x/time/rate"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

func main() {
	verbose := flag.Bool("verbose", false, "Client verbosity level.")
	yamlConfigFile := flag.String("yaml_config", "", "A .yaml file containing the configuration for this benchmark")
	dataImportFile := flag.String("data-import-terms", "", "Read field replacement data from file in csv format. each column should start and end with '__' chars. Example __field1__,__field2__.")
	dataImportMode := flag.String("data-import-terms-mode", "seq", "Either 'seq' or 'rand'.")
	version := flag.Bool("v", false, "Output version and exit")
	loop := flag.Bool("loop", false, "Run this benchmark in a loop until interrupted")
	flag.Parse()

	yamlConfig, err := parseYaml(*yamlConfigFile)
	if err != nil {
		log.Fatal(err)
		return
	}

	gitSha := toolGitSHA1()
	gitDirtyStr := ""
	if toolGitDirty() {
		gitDirtyStr = "-dirty"
	}
	log.Printf("falkordb-benchmark (git_sha1:%s%s)\n", gitSha, gitDirtyStr)
	if *version {
		os.Exit(0)
	}

	log.Printf("Running in Verbose Mode: %b.\n", verbose)

	totalQueries := len(benchmarkQueries) + len(benchmarkQueriesRO)
	if totalQueries < 1 {
		log.Fatalf("You need to specify at least a query with the -query parameter or -query-ro. For example: -query=\"CREATE (n)\"")
	}

	RandomSeed := *yamlConfig.Parameters.RandomSeed
	testResult := NewTestResult("", yamlConfig.Parameters.NumClients, yamlConfig.Parameters.NumRequests, yamlConfig.Parameters.RequestsPerSecond, "")
	testResult.SetUsedRandomSeed(RandomSeed)
	log.Printf("Using random seed: %d.\n", RandomSeed)

	var requestRate = Inf
	var requestBurst = 1
	useRateLimiter := false
	if yamlConfig.Parameters.RequestsPerSecond != 0 {
		requestRate = rate.Limit(yamlConfig.Parameters.RequestsPerSecond)
		requestBurst = int(yamlConfig.Parameters.NumClients)
		useRateLimiter = true
	}

	var rateLimiter = rate.NewLimiter(requestRate, requestBurst)
	samplesPerClient := yamlConfig.Parameters.NumRequests / yamlConfig.Parameters.NumClients
	samplesPerClientRemainder := yamlConfig.Parameters.NumRequests % yamlConfig.Parameters.NumClients

	connectionStr := fmt.Sprintf("%s:%d", yamlConfig.DBConfig.Host, yamlConfig.DBConfig.Port)
	// a WaitGroup for the goroutines to tell us they've stopped
	wg := sync.WaitGroup{}
	if !*loop {
		log.Printf("Total clients: %d. Commands per client: %d Total commands: %d\n", yamlConfig.Parameters.NumClients, samplesPerClient, yamlConfig.Parameters.NumRequests)
		if samplesPerClientRemainder != 0 {
			log.Printf("Last client will issue: %d commands.\n", samplesPerClientRemainder+samplesPerClient)
		}
	} else {
		log.Printf("Running in loop until you hit Ctrl+C\n")
	}

	randGen := rand.New(rand.NewSource(RandomSeed))
	randLimit := *yamlConfig.Parameters.RandomIntMax - *yamlConfig.Parameters.RandomIntMin

	var replacementArr []map[string]string
	dataReplacementEnabled := false
	if *dataImportFile != "" {
		log.Printf("Reading term data import file from: %s. Using '%s' record read mode.\n", *dataImportFile, *dataImportMode)
		dataReplacementEnabled = true
		replacementArr = make([]map[string]string, 0)

		f, err := os.Open(*dataImportFile)
		if err != nil {
			log.Fatal("Unable to read input file "+*dataImportFile, err)
		}
		defer func(f *os.File) {
			err := f.Close()
			if err != nil {
			}
		}(f)

		csvReader := csv.NewReader(f)
		records, err := csvReader.ReadAll()
		headers := records[0]
		rlen := len(records) - 1
		for i := 0; i < int(yamlConfig.Parameters.NumRequests); i++ {
			// seq mode
			recordPos := i % rlen
			if strings.Compare(*dataImportMode, "rand") == 0 {
				recordPos = randGen.Intn(rlen)
			}
			record := records[recordPos+1]
			lineMap := make(map[string]string)
			for j := 0; j < len(headers); j++ {
				lineMap[headers[j]] = record[j]
			}
			replacementArr = append(replacementArr, lineMap)
		}
		if err != nil {
			log.Fatal("Unable to parse file as CSV for "+*dataImportFile, err)
		}
		log.Printf("There are a total of %d disticint lines of terms. Each line has %d columns. Prepared %d groups of records for the benchmark.\n", rlen, len(headers), len(replacementArr))

	}

	readAndWriteQueries := append(benchmarkQueries, benchmarkQueriesRO...)

	for i := 0; i < len(queries); i++ {
		queryIsReadOnly[i] = false
		// read-only queries are located after the read/write ones in queries,
		// so we start on len(benchmarkQueries) to tag them
		if i >= len(benchmarkQueries) {
			queryIsReadOnly[i] = true
		}
	}
	totalDifferentCommands, cdf := prepareCommandsDistribution(readAndWriteQueries, queries, cmdRates)

	createRequiredGlobalStructs(totalDifferentCommands)

	graphs := make([]falkordb.Graph, yamlConfig.Parameters.NumClients)
	conns := make([]falkordb.FalkorDB, yamlConfig.Parameters.NumClients)

	// a WaitGroup for the goroutines to tell us they've stopped
	dataPointProcessingWg := sync.WaitGroup{}
	graphDatapointsChann := make(chan GraphQueryDatapoint, yamlConfig.Parameters.NumClients)

	// listen for C-c
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	c1 := make(chan os.Signal, 1)
	signal.Notify(c1, os.Interrupt)

	log.Printf("Trying to extract FalkorDB version info\n")
	_, falkorConn := getStandaloneConn(yamlConfig.DBConfig.Graph, connectionStr, yamlConfig.DBConfig.Password, yamlConfig.DBConfig.TlsCaCertFile)
	falkorDBVersion, err := getFalkorDBVersion(falkorConn)
	if err != nil {
		log.Println(fmt.Sprintf("Unable to retrieve FalkorDB version. Continuing anayway. Error: %v\n", err))
	} else {
		log.Println(fmt.Sprintf("Detected FalkorDB version %d\n", falkorDBVersion))
	}

	tick := time.NewTicker(time.Duration(yamlConfig.CliUpdateTick) * time.Second)

	dataPointProcessingWg.Add(1)
	go processGraphDatapointsChannel(graphDatapointsChann, c1, yamlConfig.Parameters.NumRequests, &dataPointProcessingWg, &instantHistogramsResetMutex)

	// Total commands to be issue per client. Equal for all clients, except for the last one ( see comment bellow )
	clientTotalCmds := samplesPerClient
	startTime := time.Now()
	for clientId := 0; uint64(clientId) < yamlConfig.Parameters.NumClients; clientId++ {
		wg.Add(1)

		graphPtr, connsPtr := getStandaloneConn(yamlConfig.DBConfig.Graph, connectionStr, yamlConfig.DBConfig.Password, yamlConfig.DBConfig.TlsCaCertFile)
		graphs[clientId] = *graphPtr
		conns[clientId] = *connsPtr

		// Given the total commands might not be divisible by the #clients
		// the last client will send the remainder commands to match the desired request count.
		// It's OK to alter clientTotalCmds given this is the last time we use its value
		if uint64(clientId) == (yamlConfig.Parameters.NumClients - uint64(1)) {
			clientTotalCmds = samplesPerClientRemainder + samplesPerClient
		}
		cmdStartPos := uint64(clientId) * samplesPerClient
		go ingestionRoutine(&graphs[clientId], *yamlConfig.ContinueOnError, queries, queryIsReadOnly, cdf, *yamlConfig.Parameters.RandomIntMin, randLimit, clientTotalCmds, *loop, *verbose, &wg, useRateLimiter, rateLimiter, graphDatapointsChann, dataReplacementEnabled, replacementArr, cmdStartPos)
	}

	// enter the update loopupdateCLIupdateCLI
	updateCLI(startTime, tick, c, yamlConfig.Parameters.NumRequests, *loop)

	endTime := time.Now()
	duration := time.Since(startTime)

	// benchmarked ended, close the connections
	for _, standaloneConn := range conns {
		err = standaloneConn.Conn.Close()
	}

	//wait for all stats to be processed
	dataPointProcessingWg.Wait()

	testResult.FillDurationInfo(startTime, endTime, duration)
	testResult.BenchmarkFullyRun = totalCommands == yamlConfig.Parameters.NumRequests
	testResult.IssuedCommands = totalCommands
	overallGraphInternalLatencies, internalLatencyMap := GetOverallLatencies(queries, serverSide_PerQuery_GraphInternalTime_OverallLatencies, serverSide_AllQueries_GraphInternalTime_OverallLatencies)
	overallClientLatencies, clientLatencyMap := GetOverallLatencies(queries, clientSide_PerQuery_OverallLatencies, clientSide_AllQueries_OverallLatencies)
	relativeLatencyDiff, absoluteLatencyDiff := GenerateInternalExternalRatioLatencies(internalLatencyMap, clientLatencyMap)
	testResult.OverallClientLatencies = overallClientLatencies
	testResult.OverallGraphInternalLatencies = overallGraphInternalLatencies
	testResult.AbsoluteInternalExternalLatencyDiff = absoluteLatencyDiff
	testResult.RelativeInternalExternalLatencyDiff = relativeLatencyDiff
	testResult.OverallQueryRates = GetOverallRatesMap(duration, queries, clientSide_PerQuery_OverallLatencies, clientSide_AllQueries_OverallLatencies)
	testResult.DBSpecificConfigs = GetDBConfigsMap(falkorDBVersion)
	testResult.Totals = GetTotalsMap(queries, clientSide_PerQuery_OverallLatencies, clientSide_AllQueries_OverallLatencies, errorsPerQuery, totalNodesCreatedPerQuery, totalNodesDeletedPerQuery, totalLabelsAddedPerQuery, totalPropertiesSetPerQuery, totalRelationshipsCreatedPerQuery, totalRelationshipsDeletedPerQuery)

	// final merge of pending stats
	printFinalSummary(queries, totalCommands, duration)

	saveJsonResult(testResult, yamlConfig.JsonOutputFile)
}

func GetDBConfigsMap(version int64) map[string]interface{} {
	dbConfigsMap := map[string]interface{}{}
	dbConfigsMap["RedisGraphVersion"] = version
	return dbConfigsMap
}

// getRedisGraphVersion returns RedisGraph version by issuing "MODULE LIST" command
// and iterating through the availabe modules up until "graph" is found as the name property
func getFalkorDBVersion(falkorClient *falkordb.FalkorDB) (version int64, err error) {
	var values []interface{}
	var moduleInfo []interface{}
	var moduleName string

	ctx := context.Background()
	values, err = redis.Values(falkorClient.Conn.Do(ctx, "MODULE", "LIST").Result())
	if err != nil {
		return
	}
	for _, rawModule := range values {
		moduleInfo, err = redis.Values(rawModule, err)
		if err != nil {
			return
		}
		moduleName, err = redis.String(moduleInfo[1], err)
		if err != nil {
			return
		}
		if moduleName == "graph" {
			version, err = redis.Int64(moduleInfo[3], err)
		}
	}
	return
}
