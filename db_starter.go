package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
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

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check server response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	log.Println("Downloaded dataset")

	return nil
}

func RunFalkorDB() error {
	// Create the command with hardcoded arguments
	cmd := exec.Command("redis-server", "--loadmodule", "./falkordb.so", "--dbfilename", "dataset.rdb")

	// Create a pipe for the stdout of the command
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("failed to get stdout pipe: %s", err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		log.Fatalf("failed to start command: %s", err)
	}

	// Create a scanner to read the stdout line by line
	scanner := bufio.NewScanner(stdoutPipe)

	done := make(chan bool)
	go func() {
		defer close(done)
		for scanner.Scan() {
			line := scanner.Text()
			log.Println("Read line:", line) // Print the line for debugging

			// Check if the line contains the specified substring
			if strings.Contains(line, "Ready to accept connections tcp") {
				done <- true
				return
			}
		}
		if err := scanner.Err(); err != nil {
			fmt.Println("Error reading stdout:", err)
		}
		log.Printf(scanner.Text())
		done <- false
	}()

	select {
	case found := <-done:
		if !found {
			return fmt.Errorf("substring not found")
		}
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timeout: substring not found within 10 seconds")
	}

	// Continue running the process
	go func() {
		if err := cmd.Wait(); err != nil {
			fmt.Println("Command finished with error:", err)
		} else {
			fmt.Println("Command finished successfully")
		}
	}()

	return nil
}