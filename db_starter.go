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

func CopyDataset(src string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destinationFile, err := os.Create("dataset.rdb")
	if err != nil {
		return err
	}
	defer destinationFile.Close()

	_, err = io.Copy(destinationFile, sourceFile)
	if err != nil {
		return err
	}

	fmt.Println("Copied dataset")
	return nil
}

func IsURL(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}

func RunFalkorDBProcess(dockerImage string, timeout int, hasDataset bool) (cancel context.CancelFunc, cmd *exec.Cmd, err error) {
	// Create the command with hardcoded arguments
	ctx, cancel := context.WithCancel(context.Background())

	if hasDataset {
		cmd = exec.CommandContext(ctx, "docker", "run", "--rm", "-i", "-p", "6379:6379", "--name", "falkordb", "-v", "./dataset.rdb:/data/dump.rdb", "-e", "FALKORDB_ARGS=TIMEOUT 0", dockerImage)
	} else {
		cmd = exec.CommandContext(ctx, "docker", "run", "--rm", "-i", "-p", "6379:6379", "--name", "falkordb", "-e", "FALKORDB_ARGS=TIMEOUT 0", dockerImage)
	}

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
			killDatabase(cmd, cancel)
			return
		}
		fmt.Println("Database accepting connections")
	case <-time.After(time.Duration(timeout) * time.Second):
		err = fmt.Errorf("timeout: substring not found within 10 seconds")
		killDatabase(cmd, cancel)
		return
	}

	return
}

func killDatabase(cmd *exec.Cmd, cancel context.CancelFunc) {
	fmt.Println("Ensuring FalkorDB is stopped") // Sounds like a threat

	cancel()
	cmd.Process.Kill()
	cmd.Process.Wait()

	exec.Command("docker", "rm", "-f", "falkordb").Run()
}
