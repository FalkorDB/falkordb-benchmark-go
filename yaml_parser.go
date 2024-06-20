package main

import (
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
)

type Query struct {
	Query string  `yaml:"query"`
	Ratio float64 `yaml:"ratio"`
}

type YamlConfig struct {
	Name            *string `yaml:"name"`
	Description     string  `yaml:"description,omitempty"`
	DockerImage     string  `yaml:"docker_image,omitempty"`
	DatabaseModule  string  `yaml:"database_module,omitempty"`
	ContinueOnError bool    `yaml:"continue_on_error,omitempty"`
	DBConfig        struct {
		Host                   string     `yaml:"host,omitempty"`
		Port                   int        `yaml:"port,omitempty"`
		InitCommands           [][]string `yaml:"init_commands,flow,omitempty"`
		Dataset                *string    `yaml:"dataset,omitempty"`
		DatasetLoadTimeoutSecs int        `yaml:"dataset_load_timeout_secs,omitempty"`
		Password               string     `yaml:"password,omitempty"`
		TlsCaCertFile          string     `yaml:"tls_ca_cert_file,omitempty"`
	} `yaml:"db_config"`
	Parameters struct {
		Graph             string  `yaml:"graph"`
		NumClients        uint64  `yaml:"num_clients"`
		NumRequests       uint64  `yaml:"num_requests"`
		RequestsPerSecond uint64  `yaml:"rps,omitempty"`
		RandomIntMin      *int64  `yaml:"random_int_min,omitempty"`
		RandomIntMax      *int64  `yaml:"random_int_max,omitempty"`
		RandomSeed        *int64  `yaml:"random_seed,omitempty"`
		Queries           []Query `yaml:"queries,flow,omitempty"`
		RoQueries         []Query `yaml:"ro_queries,flow,omitempty"`
	} `yaml:"parameters"`
}

func parseYaml(yamlFile string) (yamlConfig YamlConfig, err error) {
	if yamlFile == "" {
		err = errors.New("received empty yaml path")
		return
	}

	data, err := os.ReadFile(yamlFile)
	if err != nil {
		err = fmt.Errorf("YAML file does not exist: %v", err)
		return
	}

	err = yaml.Unmarshal(data, &yamlConfig)
	if err != nil {
		err = fmt.Errorf("could not parse YAML file: %v", err)
		return
	}

	if yamlConfig.Name == nil {
		err = fmt.Errorf("name is required")
		return
	}

	if yamlConfig.Parameters.Queries == nil && yamlConfig.Parameters.RoQueries == nil {
		err = errors.New("no queries were provided")
		return
	}

	if yamlConfig.DBConfig.DatasetLoadTimeoutSecs == 0 {
		yamlConfig.DBConfig.DatasetLoadTimeoutSecs = 180
	}

	if yamlConfig.DBConfig.Host == "" {
		yamlConfig.DBConfig.Host = "localhost"
	}

	if yamlConfig.DBConfig.Port == 0 {
		yamlConfig.DBConfig.Port = 6379
	}

	if yamlConfig.Parameters.Graph == "" {
		yamlConfig.Parameters.Graph = "graph"
	}

	if yamlConfig.Parameters.NumClients == 0 {
		yamlConfig.Parameters.NumClients = 50
	}

	if yamlConfig.Parameters.NumRequests == 0 {
		yamlConfig.Parameters.NumRequests = 1000000
	}

	if yamlConfig.Parameters.RandomIntMin == nil {
		yamlConfig.Parameters.RandomIntMin = new(int64)
		*yamlConfig.Parameters.RandomIntMin = 1
	}

	if yamlConfig.Parameters.RandomIntMax == nil {
		yamlConfig.Parameters.RandomIntMax = new(int64)
		*yamlConfig.Parameters.RandomIntMax = 1000000
	}

	if yamlConfig.Parameters.RandomSeed == nil {
		yamlConfig.Parameters.RandomSeed = new(int64)
		*yamlConfig.Parameters.RandomSeed = 12345
	}

	return
}

func convertQueries(
	queries []Query, roQueries []Query) (allQueries []string, queryIsRO []bool, queryRates []float64) {

	for _, query := range queries {
		allQueries = append(allQueries, query.Query)
		queryIsRO = append(queryIsRO, false)
		queryRates = append(queryRates, query.Ratio)
	}

	for _, query := range roQueries {
		allQueries = append(allQueries, query.Query)
		queryIsRO = append(queryIsRO, true)
		queryRates = append(queryRates, query.Ratio)
	}

	return allQueries, queryIsRO, queryRates
}
