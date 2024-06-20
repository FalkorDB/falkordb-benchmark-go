package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"
)

func DownloadDataset(url string) error {
	out, err := os.Create("dataset.rdb")
	if err != nil {
		return err
	}
	defer out.Close()

	fmt.Printf("Downloading dataset...")

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check server response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download dataset, server returned bad status: %s", resp.Status)
	}

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	fmt.Println("Downloaded dataset")
	return nil
}

func CopyFile(src string, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destinationFile, err := os.Create(dst)
	if err != nil {
		return err
	}

	defer destinationFile.Close()
	_, err = io.Copy(destinationFile, sourceFile)
	if err != nil {
		return err
	}

	fmt.Println("Copied file")
	return nil
}

func IsURL(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}

func prepareDataset(dataset *string) (err error) {
	if dataset == nil || *dataset == "" {
		return
	}

	if IsURL(*dataset) {
		err = DownloadDataset(*dataset)
		return
	}

	fmt.Println("Copying dataset")
	err = CopyFile(*dataset, "dataset.rdb")
	return
}

func RunFalkorDBContainers(dockerImage string, timeout int, databaseModule string, hasDataset bool) (cancel context.CancelFunc, cmd *exec.Cmd, err error) {
	// Create the command with hardcoded arguments
	ctx, cancel := context.WithCancel(context.Background())

	args := make([]string, 0, 11)
	args = append(args, "run", "--rm", "-i", "-p", "6379:6379", "--name", "falkordb", "-e", "FALKORDB_ARGS=TIMEOUT 0")
	if databaseModule != "" {
		err = CopyFile(databaseModule, "falkordb.so")
		if err != nil {
			cancel()
			log.Panicf("failed to copy database module: %s", err)
		}
		args = append(args, "-v", "falkordb.so:/FalkorDB/bin/src/:ro")
	}
	if hasDataset {
		args = append(args, "-v", "./dataset.rdb:/data/dump.rdb")
	}
	args = append(args, dockerImage)

	cmd = exec.CommandContext(ctx, "docker", args...)

	// Create a pipe for the stdout of the command
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		log.Panicf("failed to get stdout pipe: %s", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		log.Panicf("failed to get stderr pipe: %s", err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		cancel()
		log.Panicf("failed to start command: %s", err)
	}

	// Create a scanner to read the stdout line by line
	scanner := bufio.NewScanner(stdoutPipe)

	done := make(chan bool)
	go func() {
		defer close(done)
		for scanner.Scan() {
			line := scanner.Text()

			// Check if the line contains the specified substring
			if strings.Contains(line, "Ready to accept connections tcp") {
				done <- true
				return
			}
		}
		if err := scanner.Err(); err != nil {
			fmt.Println("Error reading stdout:", err)
		}

		errOut, _ := io.ReadAll(stderrPipe)
		err = fmt.Errorf("process closed: %s", string(errOut))
		done <- false
	}()

	select {
	case found := <-done:
		if !found {
			killDatabase(cmd, cancel, true)
			return
		}
		fmt.Println("Database accepting connections")
	case <-time.After(time.Duration(timeout) * time.Second):
		err = fmt.Errorf("timeout: substring not found within 10 seconds")
		killDatabase(cmd, cancel, true)
		return
	}

	return
}

func RunFalkorDBProcess(databaseModule string, timeout int) (cancel context.CancelFunc, cmd *exec.Cmd, err error) {
	// Create the command with hardcoded arguments
	ctx, cancel := context.WithCancel(context.Background())

	cmd = exec.CommandContext(ctx, "redis-server", "--loadmodule", databaseModule, "--dbfilename", "dataset.rdb")

	// Create a pipe for the stdout of the command
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		log.Panicf("failed to get stdout pipe: %s", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		log.Panicf("failed to get stderr pipe: %s", err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		cancel()
		log.Panicf("failed to start command: %s", err)
	}

	// Create a scanner to read the stdout line by line
	scanner := bufio.NewScanner(stdoutPipe)

	done := make(chan bool)
	go func() {
		defer close(done)
		for scanner.Scan() {
			line := scanner.Text()

			// Check if the line contains the specified substring
			if strings.Contains(line, "Ready to accept connections tcp") {
				done <- true
				return
			}
		}
		if err := scanner.Err(); err != nil {
			fmt.Println("Error reading stdout:", err)
		}

		errOut, _ := io.ReadAll(stderrPipe)
		err = fmt.Errorf("process closed: %s", string(errOut))
		done <- false
	}()

	select {
	case found := <-done:
		if !found {
			killDatabase(cmd, cancel, false)
			return
		}
		fmt.Println("Database accepting connections")
	case <-time.After(time.Duration(timeout) * time.Second):
		err = fmt.Errorf("timeout: substring not found within 10 seconds")
		killDatabase(cmd, cancel, false)
		return
	}

	return
}

func startDatabase(yamlConfig *YamlConfig) (cancelFunc context.CancelFunc, cmd *exec.Cmd, isDocker bool, err error) {
	// Always prefer a docker container if it's available, if a falkordb.so is provided, just use it as a volume
	if yamlConfig.DockerImage != "" {
		cancelFunc, cmd, err = RunFalkorDBContainers(yamlConfig.DockerImage, yamlConfig.DBConfig.DatasetLoadTimeoutSecs, yamlConfig.DatabaseModule, yamlConfig.DBConfig.Dataset != nil)
		isDocker = true
		return
	}

	// If no docker image is provided, use the redis-server command directly
	cancelFunc, cmd, err = RunFalkorDBProcess(yamlConfig.DatabaseModule, yamlConfig.DBConfig.DatasetLoadTimeoutSecs)
	isDocker = false

	return
}

func killDatabase(cmd *exec.Cmd, cancel context.CancelFunc, isDocker bool) {
	fmt.Println("Ensuring FalkorDB is stopped") // Sounds like a threat

	cancel()
	cmd.Process.Kill()
	cmd.Process.Wait()

	if isDocker {
		exec.Command("docker", "rm", "-f", "falkordb").Run()
	}
}
