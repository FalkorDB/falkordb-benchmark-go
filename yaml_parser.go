package main

import (
	"errors"
	"gopkg.in/yaml.v3"
	"os"
)

type YamlConfig struct {
	Name            *string `yaml:"name"`
	Description     string  `yaml:"description,omitempty"`
	DockerImage     *string `yaml:"docker_image"`
	JsonOutputFile  string  `yaml:"json_output_file"`
	CliUpdateTick   int     `yaml:"cli_update_tick"`
	ContinueOnError *bool   `yaml:"continue_on_error"`
	DBConfig        struct {
		Host                   string   `yaml:"host"`
		Port                   int      `yaml:"port"`
		Graph                  string   `yaml:"graph"`
		InitCommands           []string `yaml:"init_commands,flow"`
		Dataset                string   `yaml:"dataset"`
		DatasetLoadTimeoutSecs int      `yaml:"dataset_load_timeout_secs"`
		Password               string   `yaml:"password"`
		TlsCaCertFile          string   `yaml:"tls_ca_cert_file"`
	} `yaml:"db_config"`
	Parameters struct {
		NumClients        uint64 `yaml:"num_clients"`
		NumRequests       uint64 `yaml:"num_requests"`
		RequestsPerSecond uint64 `yaml:"rps"`
		RandomIntMin      *int64 `yaml:"random_int_min"`
		RandomIntMax      *int64 `yaml:"random_int_max"`
		RandomSeed        *int64 `yaml:"random_seed"`
		Queries           []struct {
			Query string   `yaml:"query"`
			Ratio *float64 `yaml:"ratio"`
		} `yaml:"queries,flow"`
		RoQueries []struct {
			Query string   `yaml:"query"`
			Ratio *float64 `yaml:"ratio"`
		} `yaml:"ro_queries,flow"`
	} `yaml:"parameters"`
}

func parseYaml(yamlFile string) (yamlConfig YamlConfig, err error) {
	if yamlFile == "" {
		err = errors.New("received empty yaml path")
		return
	}

	data, err := os.ReadFile(yamlFile)
	if err != nil {
		err = errors.New("YAML file does not exist")
		return
	}

	err = yaml.Unmarshal(data, &yamlConfig)
	if err != nil {
		err = errors.New("could not parse YAML file")
		return
	}

	if yamlConfig.Parameters.Queries == nil && yamlConfig.Parameters.RoQueries == nil {
		err = errors.New("no queries were provided")
		return
	}

	if yamlConfig.DBConfig.DatasetLoadTimeoutSecs == 0 {
		yamlConfig.DBConfig.DatasetLoadTimeoutSecs = 180
	}

	if yamlConfig.JsonOutputFile == "" {
		yamlConfig.JsonOutputFile = "benchmark-results.json"
	}

	if yamlConfig.CliUpdateTick == 0 {
		yamlConfig.CliUpdateTick = 5
	}

	if yamlConfig.ContinueOnError == nil {
		yamlConfig.ContinueOnError = new(bool)
		*yamlConfig.ContinueOnError = false
	}

	if yamlConfig.DBConfig.Host == "" {
		yamlConfig.DBConfig.Host = "localhost"
	}

	if yamlConfig.DBConfig.Port == 0 {
		yamlConfig.DBConfig.Port = 6379
	}

	if yamlConfig.DBConfig.Graph == "" {
		yamlConfig.DBConfig.Graph = "graph"
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
