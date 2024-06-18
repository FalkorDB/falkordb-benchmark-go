package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/FalkorDB/falkordb-go"
	"golang.org/x/time/rate"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

func printVersion(version bool) {
	gitSha := toolGitSHA1()
	if gitSha == "" {
		gitSha = "UNKNOWN"
	}
	fmt.Printf("falkordb-benchmark-go (git_sha1:%s)\n", gitSha)
	if version {
		os.Exit(0)
	}
}

func GetDBConfigsMap(version int64) map[string]interface{} {
	dbConfigsMap := map[string]interface{}{}
	dbConfigsMap["FalkorDBVersion"] = version
	return dbConfigsMap
}

// getFalkorDBVersion returns FalkorDB version by issuing "MODULE LIST" command
// and iterating through the availabe modules up until "graph" is found as the name property
func getFalkorDBVersion(falkorClient *falkordb.FalkorDB) (version int64, err error) {

	ctx := context.Background()
	result, err := falkorClient.Conn.Do(ctx, "MODULE", "LIST").Result()
	if err != nil || result == nil {
		return
	}

	modules := result.([]interface{})

	for _, module := range modules {
		moduleMap := module.(map[interface{}]interface{})
		if moduleMap["name"].(string) == "graph" {
			version = moduleMap["ver"].(int64)
		}
	}

	return
}

func main() {
	version := flag.Bool("v", false, "Output version and exit")
	verbose := flag.Bool("verbose", false, "Client verbosity level.")
	loop := flag.Bool("loop", false, "Run this benchmark in a loop until interrupted")
	cliUpdateTick := flag.Int("cli_update_tick", 5, "How often should the CLI stdout be updated")
	yamlConfigFile := flag.String("yaml_config", "", "A .yaml file containing the configuration for this benchmark")
	dataImportFile := flag.String("data-import-terms", "", "Read field replacement data from file in csv format. each column should start and end with '__' chars. Example __field1__,__field2__.")
	dataImportMode := flag.String("data-import-terms-mode", "seq", "Either 'seq' or 'rand'.")
	jsonOutputFile := flag.String("output_file", "benchmark-results.json", "The name of the output file")
	overrideImage := flag.String("override_image", "", "Override the docker image specified in the yaml file")
	flag.Parse()

	printVersion(*version)

	yamlConfig, err := parseYaml(*yamlConfigFile)
	if err != nil {
		log.Fatalf("Failed to parse YAML configuration: %v", err)
	}

	fmt.Printf("Running in Verbose Mode: %t.\n", *verbose)

	if yamlConfig.DBConfig.Dataset != nil {
		if IsURL(*yamlConfig.DBConfig.Dataset) {
			err = DownloadDataset(*yamlConfig.DBConfig.Dataset)
			if err != nil {
				log.Fatalln("Could not download dataset ", err)
			}
		} else {
			err = CopyDataset(*yamlConfig.DBConfig.Dataset)
			if err != nil {
				log.Fatalln("Could not copy dataset ", err)
			}
		}
	}

	if *overrideImage != "" {
		yamlConfig.DockerImage = *overrideImage
	}
	cancelFunc, cmd, err := RunFalkorDBProcess(yamlConfig.DockerImage, yamlConfig.DBConfig.DatasetLoadTimeoutSecs, yamlConfig.DBConfig.Dataset != nil)
	if err != nil {
		log.Fatalf("Could not start Falkor in time, %s", err)
	}

	// From here on we can't use log.Fatal in all its forms, as it will not call defer functions
	defer func() {
		killDatabase(cmd, cancelFunc)
	}()

	totalQueries := len(yamlConfig.Parameters.Queries) + len(yamlConfig.Parameters.RoQueries)
	if totalQueries < 1 {
		log.Panicln("You need to specify at least a query with the -query parameter or -query-ro. For example: -query=\"CREATE (n)\"")
	}

	RandomSeed := *yamlConfig.Parameters.RandomSeed
	testResult := NewTestResult("", yamlConfig.Parameters.NumClients, yamlConfig.Parameters.NumRequests, yamlConfig.Parameters.RequestsPerSecond, "")
	testResult.SetUsedRandomSeed(RandomSeed)
	fmt.Printf("Using RNG seed: %d.\n", RandomSeed)

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
		fmt.Printf("Total clients: %d. Commands per client: %d Total commands: %d\n", yamlConfig.Parameters.NumClients, samplesPerClient, yamlConfig.Parameters.NumRequests)
		if samplesPerClientRemainder != 0 {
			fmt.Printf("Last client will issue: %d commands.\n", samplesPerClientRemainder+samplesPerClient)
		}
	} else {
		fmt.Printf("Running in loop until you hit Ctrl+C\n")
	}

	randGen := rand.New(rand.NewSource(RandomSeed))
	randLimit := *yamlConfig.Parameters.RandomIntMax - *yamlConfig.Parameters.RandomIntMin

	var replacementArr []map[string]string
	dataReplacementEnabled := false
	if *dataImportFile != "" {
		fmt.Printf("Reading term data import file from: %s. Using '%s' record read mode.\n", *dataImportFile, *dataImportMode)
		dataReplacementEnabled = true
		replacementArr = make([]map[string]string, 0)

		f, err := os.Open(*dataImportFile)
		if err != nil {
			log.Panicln("Unable to read input file "+*dataImportFile, err)
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
			log.Panicln("Unable to parse file as CSV for "+*dataImportFile, err)
		}
		fmt.Printf("There are a total of %d disticint lines of terms. Each line has %d columns. Prepared %d groups of records for the benchmark.\n", rlen, len(headers), len(replacementArr))

	}

	allQueries, queryIsRO, queryRates := convertQueries(yamlConfig.Parameters.Queries, yamlConfig.Parameters.RoQueries)
	totalDifferentCommands, cdf := prepareCommandsDistribution(allQueries, queryRates)

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

	_, falkorConn := getStandaloneConn(yamlConfig.DBConfig.Graph, connectionStr, yamlConfig.DBConfig.Password, yamlConfig.DBConfig.TlsCaCertFile, yamlConfig.DBConfig.DatasetLoadTimeoutSecs)
	falkorDBVersion, err := getFalkorDBVersion(falkorConn)
	if err != nil {
		fmt.Printf("Unable to retrieve FalkorDB version. Continuing anayway. Error: %v\n", err)
	} else {
		fmt.Printf("Detected FalkorDB version %d\n", falkorDBVersion)
	}

	for _, command := range yamlConfig.DBConfig.InitCommands {
		interfaceArray := make([]interface{}, len(command))
		for i, v := range command {
			interfaceArray[i] = v
		}

		_, err := falkorConn.Conn.Do(context.Background(), interfaceArray...).Result()
		if err != nil {
			log.Panicf("Could not execute init query %s", err)
		}
	}

	tick := time.NewTicker(time.Duration(*cliUpdateTick) * time.Second)

	dataPointProcessingWg.Add(1)
	go processGraphDatapointsChannel(graphDatapointsChann, c1, yamlConfig.Parameters.NumRequests, &dataPointProcessingWg, &instantHistogramsResetMutex)

	panicChannel := make(chan bool)
	defer close(panicChannel)

	// Total commands to be issue per client. Equal for all clients, except for the last one ( see comment bellow )
	clientTotalCmds := samplesPerClient
	startTime := time.Now()
	for clientId := 0; uint64(clientId) < yamlConfig.Parameters.NumClients; clientId++ {
		wg.Add(1)

		graphPtr, connsPtr := getStandaloneConn(yamlConfig.DBConfig.Graph, connectionStr, yamlConfig.DBConfig.Password, yamlConfig.DBConfig.TlsCaCertFile, 5)
		graphs[clientId] = *graphPtr
		conns[clientId] = *connsPtr

		// Given the total commands might not be divisible by the #clients
		// the last client will send the remainder commands to match the desired request count.
		// It's OK to alter clientTotalCmds given this is the last time we use its value
		if uint64(clientId) == (yamlConfig.Parameters.NumClients - uint64(1)) {
			clientTotalCmds = samplesPerClientRemainder + samplesPerClient
		}
		cmdStartPos := uint64(clientId) * samplesPerClient
		go ingestionRoutine(&graphs[clientId], yamlConfig.ContinueOnError, allQueries, queryIsRO, cdf, *yamlConfig.Parameters.RandomIntMin, randLimit, clientTotalCmds, *loop, *verbose, &wg, useRateLimiter, rateLimiter, graphDatapointsChann, dataReplacementEnabled, replacementArr, cmdStartPos, panicChannel)
	}

	// enter the update loopUpdateCLIUpdateCLI
	updateCLI(startTime, tick, c, yamlConfig.Parameters.NumRequests, *loop, panicChannel)

	endTime := time.Now()
	duration := time.Since(startTime)

	// benchmarked ended, close the connections
	for _, conn := range conns {
		err = conn.Conn.Close()
	}

	//wait for all stats to be processed
	dataPointProcessingWg.Wait()

	testResult.FillDurationInfo(startTime, endTime, duration)
	testResult.BenchmarkFullyRun = totalCommands == yamlConfig.Parameters.NumRequests
	testResult.IssuedCommands = totalCommands
	overallGraphInternalLatencies, internalLatencyMap := GetOverallLatencies(allQueries, serverSidePerQueryGraphInternalTimeOverallLatencies, serverSideAllQueriesGraphInternalTimeOverallLatencies)
	overallClientLatencies, clientLatencyMap := GetOverallLatencies(allQueries, clientSidePerQueryOverallLatencies, clientSideAllQueriesOverallLatencies)
	relativeLatencyDiff, absoluteLatencyDiff := GenerateInternalExternalRatioLatencies(internalLatencyMap, clientLatencyMap)
	testResult.OverallClientLatencies = overallClientLatencies
	testResult.OverallGraphInternalLatencies = overallGraphInternalLatencies
	testResult.AbsoluteInternalExternalLatencyDiff = absoluteLatencyDiff
	testResult.RelativeInternalExternalLatencyDiff = relativeLatencyDiff
	testResult.OverallQueryRates = GetOverallRatesMap(duration, allQueries, clientSidePerQueryOverallLatencies, clientSideAllQueriesOverallLatencies)
	testResult.DBSpecificConfigs = GetDBConfigsMap(falkorDBVersion)
	testResult.Totals = GetTotalsMap(allQueries, clientSidePerQueryOverallLatencies, clientSideAllQueriesOverallLatencies, errorsPerQuery, totalNodesCreatedPerQuery, totalNodesDeletedPerQuery, totalLabelsAddedPerQuery, totalPropertiesSetPerQuery, totalRelationshipsCreatedPerQuery, totalRelationshipsDeletedPerQuery)

	// final merge of pending stats
	printFinalSummary(allQueries, totalCommands, duration)

	saveJsonResult(testResult, *jsonOutputFile)
}
