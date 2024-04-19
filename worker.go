package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

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

type Pairs struct {
	pairs []Pair
}

type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

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
	conn, err := net.Dial("udp", "8.8.8.8:80")

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
	for v := range values {
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

func (task *MapTask) Process(path string, client Interface) error {
	db, err := openDatabase(path)
	if err != nil {
		log.Printf("error in op")
		return err
	}
	defer db.Close()

	rows, err := db.Query("select key, value from pairs")
	defer rows.Close()
	if err != nil {
		log.Printf("error in select query from database to get pairs: %v", err)
		return err
	}

	// for key, value from input
	var key string
	var value string

	final_output := new(Pairs)
	Client := new(Client)

	for rows.Next() {
		if err = rows.Scan(&key, &value); err != nil {
			return err
		}

		// map process
		// ... spin up goroutine then call map
		// map(key, value)

		output := make(chan Pair)

		go func() {
			err = Client.Map(key, value, output)
			if err != nil {
				log.Printf("Client.Map: %v", err)
			}
		}()

		for pair := range output {
			final_output.pairs = append(final_output.pairs, pair)
		}

		//hash := fnv.New32() // from the stdlib package hash/fnv
		//hash.Write([]byte(pair.Key))
		//r := int(hash.Sum32() % uint32(task.R))
		task.M++
	}
	rows.Close()

	for pair := range final_output.pairs {
		fmt.Println(pair)
	}

	return err
}

//Process for ReduceTask

func (task *ReduceTask) Process(path string, client Interface) error {
	db, err := openDatabase(path)

	if err != nil {
		log.Fatalf("No")

	}

	//TODO: Need to merge given databases (whatever databases we are talking about, considering the function only has one path string given)
	//TODO: Create an ouput database
	//TODO: Need to process all pairs in correct order

}

func main() {

	// Introduction
	log.Print("Map Reduce -- Part 1")
	log.Print("By: Jordan Coleman & Hailey Whipple")

	m := 10
	r := 5

	source := "source.db"

	tmp := os.TempDir()

	tempdir := filepath.Join(tmp, fmt.Sprintf("mapreduce.%d", os.Getpid()))

	if err := os.RemoveAll(tempdir); err != nil {
		log.Fatalf("unable to delete old temp dir: %v", err)
	}
	if err := os.Mkdir(tempdir, 0700); err != nil {
		log.Fatalf("Was unable to make a temp dir")
	}
	defer os.RemoveAll(tempdir)

	log.Printf("splitting %s into %d pieces", source, m)

	var paths []string

	for i := 0; i < m; i++ {
		paths = append(paths, filepath.Join(tempdir, mapSourceFile(i)))
	}
	if err := splitDatabase(source, paths); err != nil {
		log.Fatalf("splitting database: %v", err)
	}

	the_address := net.JoinHostPort(getLocalAddress(), "8080")
	log.Print("Here is a new address that we are starting an http server with and it is", the_address)

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

	//This is where we are building our reduce tasks

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

	//This is where we are processing the map tasks
	for i, task := range mapTasks {
		if err := task.Process(tempdir, client); err != nil {
			log.Fatalf("there was an error with processing the maptask: ", i, err)
		}
		for _, reduce := range reduceTasks {
			reduce.SourceHosts[i] = the_address //Question: Why are we passing in the same address here everytime?
		}
	}

	//This is where we are processing the reduce tasks

	for i, task := range reduceTasks {
		if err := task.Process(tempdir, client); err != nil {
			log.Fatalf("there was an error with processing the reduce task: ", i, err)
		}
	}

	/* NEXT STEP IS WE NEED TO GATHER OUTPUTS INTO FINAL target.db FILE

	//This is what we wrote last time

	//client := new(Interface)
	//shell(client)

	*/

}
