package main

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type TokenResponse struct {
	AccessToken string `json:"access_token"`
	Scope       string `json:"scope"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

var consumerKey = flag.String("consumer-key", "", "consumer key")
var consumerSecret = flag.String("consumer-secret", "", "consumer secret")
var filePath = flag.String("file-path", "", "input file path")
var outputDir = flag.String("output-dir", "", "output directory")
var extractDir = flag.String("extract-dir", "", "extract directory")
var numWorkers = flag.Int("num-workers", 1, "number of parallel workers")
var collection = flag.String("collection", "EO:EUM:DAT:MSG:HRSEVIRI", "eutmetsat collection")
var force = flag.Bool("force", false, "remove existing files")

func main() {
	flag.Parse()

	if *filePath == "" {
		log.Printf("invalid file path")
		return
	}

	if *outputDir == "" {
		*outputDir = "./"
	}

	var response TokenResponse
	err := fetchTokenOnce(&response)
	if err != nil {
		log.Printf("error: %s", err)
		return
	}

	log.Printf("got token = '%s', valid for = %d", response.AccessToken, response.ExpiresIn)

	fileBytes, err := os.ReadFile(*filePath)
	if err != nil {
		log.Printf("error: %s", err)
		return
	}

	fileContent := string(fileBytes)
	files := strings.Split(fileContent, "\n")

	tasksQueue := make(chan string, 0)

	// Start token updater
	go updateToken(&response)

	var wg sync.WaitGroup

	wg.Add(*numWorkers)

	for i := 1; i <= *numWorkers; i++ {
		go worker(i, &wg, tasksQueue, *collection, &response.AccessToken)
	}

	for _, file := range files {
		if file == "" {
			continue
		}
		tasksQueue <- file
	}

	close(tasksQueue)

	wg.Wait()
}

func updateToken(response *TokenResponse) {
	for true {
		time.Sleep(time.Second * time.Duration(response.ExpiresIn-40))
		err := fetchTokenOnce(response)
		if err != nil {
			log.Printf("token updater: error: %s", err)
			return
		}
	}
}

func worker(id int, wg *sync.WaitGroup, tasksQueue <-chan string, collection string, token *string) {
	log.Printf("worker #%d - on", id)

	for file := range tasksQueue {
		log.Printf("worker #%d - downloading %s", id, file)
		for true {
			fileUrl, err := url.Parse(getFileUrl(collection, file))
			if err != nil {
				log.Printf("worker #%d: error: %s", id, err)
				return
			}

			filename := path.Join(*outputDir, path.Base(fileUrl.Path)+".zip")
			if _, err := os.Stat(filename); !errors.Is(err, os.ErrNotExist) {
				if *force {
					_ = os.Remove(filename)
				} else {
					// File exists, skip it
					log.Printf("worker #%d: skipping the download for %s '%s'", id, filename, fileUrl.Path)
					break
				}
			}

			err = fetchFile(fileUrl.String(), filename, token)
			if err != nil {
				log.Printf("worker #%d: error: %s", id, err)
				// Remove file and retry
				_ = os.Remove(filename)
				continue
			}
			log.Printf("worker #%d - downloaded %s", id, file)
			break
		}
	}

	wg.Done()
	log.Printf("worker #%d - off", id)
}

func getFileUrl(collection string, file string) string {
	collection = url.QueryEscape(collection)
	file = url.QueryEscape(file)
	return "https://api.eumetsat.int/data/download/1.0.0/collections/" + collection + "/products/" + file
}

func fetchFile(url string, path string, token *string) error {
	client := http.DefaultClient
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Authorization", "Bearer "+*token)

	res, err := client.Do(req)
	if err != nil {
		return err
	}

	{
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = f.ReadFrom(res.Body)
		if err != nil {
			return err
		}
	}

	// Skip extraction
	if *extractDir == "" {
		return nil
	}

	dirname := strings.Replace(filepath.Base(path), ".zip", "", 1)
	if err = processFile(path, filepath.Join(*extractDir, dirname)); err != nil {
		return err
	}

	if err = os.Remove(path); err != nil {
		return err
	}

	return nil
}

// unzip file to its location
// we can remove the zip file afterwards
func processFile(path, dir string) error {
	reader, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	defer reader.Close()

	// dir := strings.Replace(path, ".zip", "", 1)
	err = os.Mkdir(dir, os.ModePerm)
	if err != nil && errors.Is(err, os.ErrExist) {
		// Dir exists, leave
		return nil
	}
	if err != nil {
		return err
	}

	for _, f := range reader.File {
		if !strings.HasSuffix(f.Name, ".nat") {
			continue
		}

		destinationFile, err := os.OpenFile(filepath.Join(dir, f.Name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}
		defer destinationFile.Close()

		zippedFile, err := f.Open()
		if err != nil {
			return err
		}
		defer zippedFile.Close()

		if _, err := io.Copy(destinationFile, zippedFile); err != nil {
			return err
		}

		break
	}

	return nil
}

func fetchTokenOnce(response *TokenResponse) error {
	reqBody := bytes.NewReader([]byte("grant_type=client_credentials"))

	client := &http.Client{Timeout: 0}
	req, err := http.NewRequest("POST", "https://api.eumetsat.int/token", reqBody)
	if err != nil {
		return err
	}

	if *consumerKey == "" ||
		*consumerSecret == "" {
		return errors.New("invalid credentials")
	}

	basic := base64.RawStdEncoding.EncodeToString([]byte(*consumerKey + ":" + *consumerSecret))
	req.Header.Add("Authorization", "Basic "+basic)

	res, err := client.Do(req)
	if err != nil {
		return err
	}

	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(response)
	if err != nil {
		return err
	}

	return nil
}
