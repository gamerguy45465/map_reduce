package main

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

var mu sync.Mutex

type MapTask struct {
	M, R       int    // total number of map and reduce tasks
	N          int    // map task number, 0-based
	SourceHost string // address of host with map input file
}

type ReduceTask struct {
	M, R        int      // total number of map and reduce tasks
	N           int      // reduce task number, 0-based
	SourceHosts []string // addresses of map workers
}

type Pair struct {
	Key   string
	Value string
}

type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

type Client struct{}

const (
	mapSource = iota
	mapInput
	mapOutput
	reduceInput
	reduceOutput
	reducePartial
	reduceTemp
)

func mapSourceFile(m int) string {
	return fmt.Sprintf("map_%d_source.db", m)
}

func mapInputFile(m int) string {
	return fmt.Sprintf("map_%d_input.db", m)
}

func mapOutputFile(m, r int) string {
	return fmt.Sprintf("map_%d_output_%d.db", m, r)
}

func reduceInputFile(r int) string {
	return fmt.Sprintf("reduce_%d_input.db", r)
}

func reduceOutputFile(r int) string {
	return fmt.Sprintf("reduce_%d_output.db", r)
}

func reducePartialFile(r int) string {
	return fmt.Sprintf("reduce_%d_partial.db", r)
}

func reduceTempFile(r int) string {
	return fmt.Sprintf("reduce_%d_temp.db", r)
}

func makeURL(host, file string) string {
	return fmt.Sprintf("http://%s/data/%s", host, file)
}

func getLocalAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:8080")

	if err != nil {
		log.Fatalf("No, getLocalAddress did not work")
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	localaddress := localAddr.IP.String()

	if localaddress == "" {
		panic("This address just doesn't work for me")
	}
	return localaddress
}

func (c Client) Map(key, value string, output chan<- Pair) error {
	defer close(output)
	lst := strings.Fields(value)
	for _, elt := range lst {
		word := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, elt)
		if len(word) > 0 {
			output <- Pair{Key: word, Value: "1"}
		}
	}
	return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- Pair) error {
	defer close(output)

	count := 0
	fmt.Println("125")
	for v := range values {
		fmt.Println("127")

		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}

	p := Pair{Key: key, Value: strconv.Itoa(count)}

	output <- p

	return nil
}

func createPaths(amount int, typeOfFile int, tmp string) []string {
	i := 0
	var paths []string
	for i < amount {
		switch typeOfFile {
		case mapSource:
			paths = append(paths, filepath.Join(tmp, mapSourceFile(i)))
		case mapInput:
			paths = append(paths, filepath.Join(tmp, mapInputFile(i)))
		case mapOutput:
			//paths = append(paths, filepath.Join(tmp, mapOutputFile(amount, i)))
			paths = append(paths, filepath.Join(tmp, mapOutputFile(i, i)))
		case reduceInput:
			paths = append(paths, filepath.Join(tmp, reduceInputFile(i)))
		case reduceOutput:
			paths = append(paths, filepath.Join(tmp, reduceOutputFile(i)))
		case reducePartial:
			paths = append(paths, filepath.Join(tmp, reducePartialFile(i)))
		case reduceTemp:
			paths = append(paths, filepath.Join(tmp, reduceTempFile(i)))
		}
		i += 1
	}
	return paths
}

func getDatabase(path string) (*sql.DB, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return createDatabase(path)
		}
	}
	return openDatabase(path)
}

func InsertPair(r int, n int, db *sql.DB, pairs []Pair) error {
	for _, pair := range pairs {
		// insert pairs into the output DB
		_, err := db.Exec("INSERT INTO pairs (key, value) VALUES (?, ?)", pair.Key, pair.Value)
		if err != nil {
			db.Close()
			log.Fatalf("InsertPair: error inserting pairs into database: %v", err)
			return err
		}
	}

	return nil
}

func (task *MapTask) Process(path string, client Interface) error {
	// make URL
	sourceFile := mapSourceFile(task.N)
	url := makeURL(task.SourceHost, sourceFile)
	inputFile := filepath.Join(path, mapInputFile(task.N))

	finished := make(chan bool, 1)

	err := download(url, inputFile)
	if err != nil {
		log.Printf("MapTask.Process: error in downloading path %s: %v", path, err)
	}

	var db *sql.DB

	db, err = openDatabase(inputFile)
	if err != nil {
		log.Printf("error in opening inputFile")
		return err
	}

	defer func() {
		db.Close()
		os.Remove(inputFile)
	}()

	outs := make([][]Pair, task.R)
	dbs := []*sql.DB{}
	defer func() {
		if <-finished {
			done_ := make(chan bool, 1)
			go func() {
				for r, elt := range outs {
					if err := InsertPair(r, task.N, dbs[r], elt); err != nil {
						fmt.Printf("MapTask.InsertPair: %v", err)
					}
					dbs[r].Close()
				}
				done_ <- true
			}()
			if <-done_ {
				return
			}
		}
	}()

	// create map output database
	for i := 0; i < task.R; i++ {
		outputDB := mapOutputFile(task.N, i)
		output_database, err := createDatabase(filepath.Join(path, outputDB))
		if err != nil {
			output_database.Close()
			return err
		}
		dbs = append(dbs, output_database)
	}

	rows, err := db.Query("select key, value from pairs")
	if err != nil {
		log.Printf("error in select query from database to get pairs: %v", err)
		return err
	}

	// map process
	defer rows.Close()
	// for key, value from input
	var key string
	var value string
	in_count, out_count := 0, 0

	for rows.Next() {
		if err = rows.Scan(&key, &value); err != nil {
			log.Fatalf("MapTask.Process: error scanning rows: %v", err)
		}

		// call map
		output_ := make(chan Pair)

		// output
		go func() {
			for pair := range output_ {
				hash := fnv.New32()
				hash.Write([]byte(pair.Key))
				r := int(hash.Sum32() % uint32(task.R))
				outs[r] = append(outs[r], pair)
				out_count++
			}
		}()

		err = client.Map(key, value, output_)
		if err != nil {
			log.Printf("Client.Map: %v", err)
		}

		in_count++
	}

	finished <- true

	return err
}

//Process for ReduceTask

func (task *ReduceTask) Process(path string, client Interface) error {
	//func (task *ReduceTask) Process(path string, client Interface, rfile string) error {
	var reduce_temp_files []string
	//fmt.Println(task.M, task.R)
	m := 0
	for m < task.M {
		file := mapOutputFile(m, task.N)
		url := makeURL(task.SourceHosts[m], file)
		reduce_temp_files = append(reduce_temp_files, url)
		m++
	}

	db, err := mergeDatabases(reduce_temp_files, reduceInputFile(task.N), reduceTempFile(task.N))
	if err != nil {
		log.Fatalf("No, merge did not work for some reason %v", err)
		//return err
	}

	// create output file
	reduceOutputFile := reduceOutputFile(task.N)

	//reduceOutputFile = reduceOutputFile

	// create that database
	reduceDB, err := createDatabase(filepath.Join(path, reduceOutputFile))

	if err != nil {
		log.Fatalf("No")
	}

	reduceDB = reduceDB

	var key string
	var value string

	var keys []string
	values := make(chan string)
	//var values <-chan string

	rows, _ := db.Query("select key, value from pairs order by key, value")

	//defer rows.Close()

	//fmt.Println("Ran Here")

	defer rows.Close()

	// variables
	previous := ""

	for rows.Next() {

		if err := rows.Scan(&key, &value); err != nil {
			return err
		}

		if len(previous) == 0 {
			previous = key
		}
		//fmt.Println("Ran")

		output := make(chan Pair)

		go func() {
			//output <- Pair{Value: value, Key: key}
			//new_value := <-values
			//new_value = new_value

			fmt.Println("Went here")

			for pair := range output {
				fmt.Println(pair, "pair")
			}
		}()

		fmt.Println(key, " key/value ", value)

		if key != previous {
			err = client.Reduce(key, values, output)
			if err != nil {
				log.Printf("Client.Reduce: %v", err)
			}
			previous = key
			values = make(chan string)
		} else {
			values <- key
		}

		fmt.Println(key, " key")
		fmt.Println(values, " value")
		fmt.Println(output, " output")

		keys = append(keys, key)

		//p := Pair{Value: value, Key: key}
		/*
			if i != 0 {
				fmt.Println("Ran")
				if keys[i-1] != key {
					fmt.Println("Output")
					output <- Pair{key, value}
				}
			}*/
		//}()

		//err = client.Reduce(key, values, output)
		//if i != 0 {
		//fmt.Println("Ran")
		//	if keys[i-1] != key {
		//		fmt.Println("Output")
		//		output <- Pair{key, value}
		//	}
	}

	db.Close()

	//log.Print("Processed Reduce Tasks")
	return nil

	// everything works above

	/*var urls []string
	m := task.M

	i := 0
	for i < m {
		file := mapOutputFile(i, task.N)
		url := makeURL(getLocalAddress()+":8080", file)
		urls = append(urls, url)
		i++
	}

	temp := createPaths(1, reduceTemp, path)

	source := "austen.db"

	if err := splitDatabase(source, temp); err != nil {
		log.Fatalf("splitting database: %v", err)
	}

	//file := reduceInputFile(task.N)

	fmt.Println(temp[0], "\n\n\n\n\n")

	//for i := 0; i < len(temp);i++

	//new_path := filepath.Join(path, rfile)

	fmt.Println(path)
	db, err := mergeDatabases(urls, rfile, temp[0])

	if err != nil {
		log.Fatalf("No, merge did not work for some reason ", err)
		return err

	} else {
		log.Print("It worked!")
	}
	rows, _ := db.Query("select key, value from pairs order by key, value")

	defer rows.Close()

	// for key, value from input

	return err
	*/
	//TODO: Need to process all pairs in correct order

}

func main() {

	// Introduction
	log.Print("Map Reduce -- Part 1")
	log.Print("By: Jordan Coleman & Hailey Whipple")

	//path := "source.db"
	source := "austen.db"

	number_of_rows, _ := getNumberOfRows(source)
	page_count, _, _ := getDatabaseSize(source)

	var m int = number_of_rows / page_count
	var r int = m / 2

	//m := 11
	//r := 5

	//source := "austin.db"

	tmp := os.TempDir()

	tempdir := filepath.Join(tmp, fmt.Sprintf("mapreduce.%d", os.Getpid()))

	//fmt.Println("Temp Dir ", tempdir)

	if err := os.RemoveAll(tempdir); err != nil {
		log.Fatalf("unable to delete old temp dir: %v", err)
	}
	if err := os.Mkdir(tempdir, 0700); err != nil {
		log.Fatalf("Was unable to make a temp dir")
	}
	defer os.RemoveAll(tempdir)

	log.Printf("splitting %s into %d pieces", source, m)

	var paths []string

	paths = createPaths(m, mapSource, tempdir)

	//for i := 0; i < m; i++ {

	//paths = createPaths(m, mapSource, tempdir)
	//paths_map_input := createPaths(m, mapInput, tempdir)
	//paths_map_output := createPaths(m, mapOutput, tempdir)
	//paths_reduce_input := createPaths(m, reduceInput, tempdir)

	//fmt.Println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n", paths3, "\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
	//}

	/*
		for i := 0; i < m; i++ {
			paths = append(paths, filepath.Join(tempdir, mapSourceFile(i)))
		}
	*/

	if err := splitDatabase(source, paths); err != nil {
		log.Fatalf("splitting database: %v", err)
	}

	/*
		if err := splitDatabase(source, paths_map_input); err != nil {
			log.Fatalf("splitting database: %v", err)
		}
		if err := splitDatabase(source, paths_map_output); err != nil {
			log.Fatalf("splitting database: %v", err)
		}
		if err := splitDatabase(source, paths_reduce_input); err != nil {
			log.Fatalf("splitting database: %v", err)
		}*/

	the_address := net.JoinHostPort(getLocalAddress(), "8080")
	log.Print("Here is a new address that we are starting an http server with and it is ", the_address)

	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))

	listener, err := net.Listen("tcp", the_address)

	if err != nil {
		log.Fatalf("There was a listen error. Here are some things to consider: ", listener, err)
	}
	go func() {
		if err := http.Serve(listener, nil); err != nil {
			log.Fatalf("There was an error with Serve for some reason")
		}

	}()

	var mapTasks []*MapTask

	//defer os.RemoveAll(tempdir)

	// This is where we are building our map tasks
	for i := 0; i < m; i++ {
		task := &MapTask{
			M:          m,
			R:          r,
			N:          i,
			SourceHost: the_address,
		}
		mapTasks = append(mapTasks, task)
	}

	// This is where we are building our reduce tasks

	var reduceTasks []*ReduceTask

	for i := 0; i < r; i++ {
		task := &ReduceTask{
			M:           m,
			R:           r,
			N:           i,
			SourceHosts: make([]string, m),
		}
		reduceTasks = append(reduceTasks, task)
	}

	var client Client

	// This is where we are processing the map tasks
	for i, task := range mapTasks {
		if err := task.Process(tempdir, client); err != nil {
			log.Fatalf("there was an error with processing the maptask: ", i, err)
		}
		for _, reduce := range reduceTasks {
			reduce.SourceHosts[i] = the_address //Question: Why are we passing in the same address here everytime?
		}
	}

	//fmt.Println(tmp)
	//fmt.Println(tempdir)
	log.Println("processed all of map tasks")

	//This is where we are processing the reduce tasks

	//fmt.Println("\n\n\n\n\n\n\n\n\n", len(reduceTasks), "\n\n\n\n\n\n")

	for i, task := range reduceTasks {
		//r_path := filepath.Join(tempdir, paths_reduce_input[i])
		if err := task.Process(tempdir, client); err != nil {
			//if err := task.Process(tempdir, client, paths_reduce_input[i]); err != nil { //
			log.Fatalf("there was an error with processing the reduce task: ", i, err)
		}
	}

	log.Print("Processed all of reduce tasks")

	/* NEXT STEP IS WE NEED TO GATHER OUTPUTS INTO FINAL target.db FILE

	//This is what we wrote last time

	//client := new(Interface)
	//shell(client)

	*/

	//go func() {
	//	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
	//	if err := http.ListenAndServe(the_address, nil); err != nil {
	//		log.Printf("Error in HTTP server for %s: %v", the_address, err)
	//	}
	//}()

}

// go run *.go
