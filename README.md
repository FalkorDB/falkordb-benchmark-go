# falkordb-benchmark-go

### A FalkorDB benchmark utility

## Usage

```bash
$ go build

$ ./falkordb_benchmark -h
Usage of ./falkordb-benchmark:
    -cli_update_tick int
        How often should the CLI stdout be updated (default 5)
    -data-import-terms string
        Read field replacement data from file in csv format. each column should start and end with '__' chars. Example __field1__,__field2__.
    -data-import-terms-mode string
        Either 'seq' or 'rand'. (default "seq")
    -loop
        Run this benchmark in a loop until interrupted
    -output_file string
        The name of the output file (default "benchmark-results.json")
    -override_image string
        Override the docker image specified in the yaml file
    -v    
        Output version and exit
    -verbose
        Client verbosity level.
    -yaml_config string
        A .yaml file containing the configuration for this benchmark

```

## Configuration
A configuration file is required to run the benchmark. The configuration file is a YAML file with the following structure:

```yaml
name: "Benchmark Name"
description: "This is a quick description of the benchmark"
docker_image: falkordb/falkordb:latest
continue_on_error: false                # In case we want to test error rates etc.
db_config:
  host: 'localhost'                     # Default is `localhost`
  port: 6379                            # Default is 6379
  password: ''                          # Default is empty
  tls_ca_cert_file: ''                  # Default is empty
  dataset: <DATASET_URL>                # Can be a local path, or an http(s) URL, default is empty
  dataset_load_timeout_secs: 180        # Time to wait for the database to start when using a dataset, default is 180
  graph: 'graph_key'                    # Default is `graph`
parameters:
  num_clients: 32                       # Num of concurrent clients used to benchmark, default is 50
  num_requests: 10000                   # Total number of requests to be made, default is 1,000,000
  requests_per_second: 0                # If set to 0, all requests will be made without delay, default is 0
  queries:                              # Mandatory if no ro_queries were provided
    - query: 'CYPHER Id1=__rand_int__ MATCH (n)-[:IS_CONNECTED*3]->(z) WHERE ID(n) =
        $Id1 RETURN ID(n), count(z) '
      ratio: 0.75                       # 75% of queries will be this one
    - query: 'CYPHER Id1=__rand_int__ Id2=__rand_int__ MATCH (n1:Node {external_id:$Id1})
        MATCH (n2:Node {external_id: $Id2}) MERGE (n1)-[rel:IS_CONNECTED]->(n2)'
      ratio: 0.25                       # The other 25% of queries will be this one
  ro_queries:                           # Mandatory if no queries were provided
    - query: 'CYPHER Id1=__rand_int__ MATCH (n)-[:IS_CONNECTED*3]->(z) WHERE ID(n) =
        $Id1 RETURN ID(n), count(z) '
      ratio: 0.75                       # 75% of queries will be this one
    - query: 'CYPHER Id1=__rand_int__ Id2=__rand_int__ MATCH (n1:Node {external_id:$Id1})
        MATCH (n2:Node {external_id: $Id2}) MERGE (n1)-[rel:IS_CONNECTED]->(n2)'
      ratio: 0.25                       # The other 25% of queries will be this one
  random_int_min: 0                     # Default is 1
  random_int_max: 262016                # Default is 1000000
  random_seed: 12345                    # Default is 12345
```

## Output
During this benchmark, the client will output the progress of the benchmark to the console. The output will be updated every 5 seconds by default.

Once done, the output JSON file will look something like this:

```json
{
  "ResultFormatVersion": "0.0.1",
  "Metadata": "",
  "Clients": 1,
  "MaxRps": 0,
  "RandomSeed": 12345,
  "BenchmarkConfiguredCommandsLimit": 500,
  "IssuedCommands": 500,
  "BenchmarkFullyRun": true,
  "TestDescription": "",
  "DBSpecificConfigs": {
    "FalkorDBVersion": 40010
  },
  "StartTime": 1718711683987,
  "EndTime": 1718711688991,
  "DurationMillis": 5003,
  "Totals": {
    "MATCH (n:N {v: floor(rand()*100001)}) DELETE n RETURN 1 LIMIT 1": {
      "Errors": 0,
      "IssuedQueries": 500,
      "LabelsAdded": 0,
      "NodesCreated": 0,
      "NodesDeleted": 499,
      "PropertiesSet": 0,
      "RelationshipsCreated": 0,
      "RelationshipsDeleted": 1497
    },
    "Total": {
      "Errors": 0,
      "IssuedQueries": 500,
      "LabelsAdded": 0,
      "NodesCreated": 0,
      "NodesDeleted": 499,
      "PropertiesSet": 0,
      "RelationshipsCreated": 0,
      "RelationshipsDeleted": 1497
    }
  },
  "OverallQueryRates": {
    "MATCH (n:N {v: floor(rand()*100001)}) DELETE n RETURN 1 LIMIT 1": 99.92056447019763,
    "Total": 99.92056447019763
  },
  "OverallClientLatencies": {
    "MATCH (n:N {v: floor(rand()*100001)}) DELETE n RETURN 1 LIMIT 1": {
      "avg": 0.480322,
      "q0": 0,
      "q100": 36.789,
      "q50": 0.404,
      "q95": 0.591,
      "q99": 0.798,
      "q999": 36.789
    },
    "Total": {
      "avg": 0.480322,
      "q0": 0,
      "q100": 36.789,
      "q50": 0.404,
      "q95": 0.591,
      "q99": 0.798,
      "q999": 36.789
    }
  },
  "OverallGraphInternalLatencies": {
    "MATCH (n:N {v: floor(rand()*100001)}) DELETE n RETURN 1 LIMIT 1": {
      "avg": 0.314198,
      "q0": 0,
      "q100": 36.205,
      "q50": 0.24,
      "q95": 0.333,
      "q99": 0.446,
      "q999": 36.205
    },
    "Total": {
      "avg": 0.314198,
      "q0": 0,
      "q100": 36.205,
      "q50": 0.24,
      "q95": 0.333,
      "q99": 0.446,
      "q999": 36.205
    }
  },
  "OverallRelativeInternalExternalLatencyDiff": {
    "avg": 1.5287239256774392,
    "q100": 1.0161303687336005,
    "q50": 1.6833333333333336,
    "q95": 1.7747747747747746,
    "q99": 1.789237668161435,
    "q999": 1.0161303687336005
  },
  "OverallAbsoluteInternalExternalLatencyDiff": {
    "avg": 0.16612400000000005,
    "q0": 0,
    "q100": 0.5840000000000032,
    "q50": 0.16400000000000003,
    "q95": 0.25799999999999995,
    "q99": 0.35200000000000004,
    "q999": 0.5840000000000032
  },
  "ClientRunTimeStats": null,
  "ServerRunTimeStats": null
}
```