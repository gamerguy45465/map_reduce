package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

type Client struct{}

func openDatabase(path string) (*sql.DB, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			log.Printf("openDatabase: file [%s] does not exist", path)
			return nil, err
		} else {
			log.Printf("openDatabase: error trying to access source file [%s]: %v", path, err)
			return nil, err
		}
	}

	options :=
		"?" + "_busy_timeout=10000" +
			"&" + "_case_sensitive_like=OFF" +
			"&" + "_foreign_keys=ON" +
			"&" + "_journal_mode=OFF" +
			"&" + "_locking_mode=NORMAL" +
			"&" + "mode=rw" +
			"&" + "_synchronous=OFF"
	db, err := sql.Open("sqlite3", path+options)
	if err != nil {
		log.Printf("error opening database [%s]: %v", path, err)
		return nil, err
	}

	return db, nil
}

func createDatabase(path string) (*sql.DB, error) {
	// delete any existing file
	os.Remove(path)

	options :=
		"?" + "_busy_timeout=10000" +
			"&" + "_case_sensitive_like=OFF" +
			"&" + "_foreign_keys=ON" +
			"&" + "_journal_mode=OFF" +
			"&" + "_locking_mode=NORMAL" +
			"&" + "mode=rw" +
			"&" + "_synchronous=OFF"
	db, err := sql.Open("sqlite3", path+options)
	if err != nil {
		log.Printf("error creating database [%s]: %v", path, err)
		return nil, err
	}
	if _, err = db.Exec("create table pairs (key text, value text)"); err != nil {
		log.Printf("error creating table for database [%s]: %v", path, err)
		db.Close()
		return nil, err
	}

	return db, nil
}

func splitDatabase(source string, paths []string) error {
	db, err := openDatabase(source)
	if err != nil {
		return err
	}
	defer db.Close()

	// create output databases
	var outs []*sql.DB
	var inserts []*sql.Stmt
	defer func() {
		for i, insert := range inserts {
			if insert != nil {
				insert.Close()
			}
			inserts[i] = nil
		}
		for i, db := range outs {
			if db != nil {
				db.Close()
			}
			outs[i] = nil
		}
	}()
	for _, path := range paths {
		out, err := createDatabase(path)
		if err != nil {
			return err
		}
		outs = append(outs, out)
		insert, err := out.Prepare("insert into pairs (key, value) values (?, ?)")
		if err != nil {
			log.Printf("error preparing statement for output database: %v", err)
			return err
		}
		inserts = append(inserts, insert)
	}

	// process input pairs
	dbi := 0
	rows, err := db.Query("select key, value from pairs")
	if err != nil {
		log.Printf("error in select query from database to split: %v", err)
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			log.Printf("error scanning row value: %v", err)
			return err
		}

		// round-robin through the output databases
		insert := inserts[dbi]
		if _, err := insert.Exec(key, value); err != nil {
			log.Printf("db error inserting row to output database: %v", err)
			return err
		}
		dbi = (dbi + 1) % len(inserts)
	}
	if err := rows.Err(); err != nil {
		log.Printf("db error iterating over inputs: %v", err)
		return err
	}
	return nil
}

func mergeDatabases(urls []string, path string, temp string) (*sql.DB, error) {
	// create the output file
	db, err := createDatabase(path)
	if err != nil {
		return nil, err
	}

	// gather them one at a time
	for _, u := range urls {
		if err := download(u, temp); err != nil {
			db.Close()
			return nil, err
		}
		if err := gatherInto(db, temp); err != nil {
			db.Close()
			return nil, err
		}
	}

	return db, nil
}

func download(url, path string) error {
	// issue a GET request to retrieve a file
	res, err := http.Get(url)
	if err != nil {
		log.Printf("error in GET request for %s: %v", url, err)
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		err := fmt.Errorf("GET request returned %s for %s", res.Status, url)
		log.Printf("%v", err)
		return err
	}
	fp, err := os.Create(path)
	if err != nil {
		log.Printf("error creating intermediate file %s for download: %v", path, err)
		return err
	}
	_, err = io.Copy(fp, res.Body)
	fp.Close()
	if err != nil {
		log.Printf("error downloading file %s from %s: %v", path, url, err)
		return err
	}
	return nil
}

func gatherInto(db *sql.DB, path string) error {
	// attach the new file to the open database and merge it in
	if _, err := db.Exec("attach ? as merge", path); err != nil {
		log.Printf("error in attach command: %v", err)
		return err
	}
	if _, err := db.Exec("pragma merge.synchronous = off"); err != nil {
		log.Printf("error disabling synchronous writes for merge database: %v", err)
		return err
	}
	if _, err := db.Exec("pragma merge.journal_mode = off"); err != nil {
		log.Printf("error disabling journaling for merge database: %v", err)
		return err
	}
	if _, err := db.Exec("insert into pairs select key, value from merge.pairs"); err != nil {
		log.Printf("error in merge insert: %v", err)
		return err
	}
	if _, err := db.Exec("detach merge"); err != nil {
		log.Printf("error in detach command: %v", err)
		return err
	}

	// might as well delete it now; might even save some disk writes
	return os.Remove(path)
}

// Part 2

func getNumberOfRows(path string) (int, error) {
	var number_of_rows string
	db, err := openDatabase(path)
	if err != nil {
		log.Printf("error in op")
		return 0, err

	}
	defer db.Close()

	rows, err := db.Query("select count(1) from pairs")
	defer rows.Close()
	if err != nil {

		log.Printf("error in select query from database to count: %v", err)
		return 0, err
	}

	for rows.Next() {
		if err = rows.Scan(&number_of_rows); err != nil {
			return 0, err
		}

		fmt.Println(number_of_rows)
	}

	rows.Close()

	count, er := strconv.Atoi(number_of_rows)
	return count, er
}

func getDatabaseSize(path string) (int, int, error) {
	var page_count string
	var page_size string
	db, err := openDatabase(path)
	if err != nil {
		log.Printf("error in op")
		return 0, 0, err

	}
	defer db.Close()

	rows, err := db.Query("PRAGMA page_count")
	if err != nil {
		log.Printf("error in pragma query from database to page_count: %v", err)
		return 0, 0, err
	}

	defer rows.Close()
	for rows.Next() {
		if err = rows.Scan(&page_count); err != nil {
			return 0, 0, err
		}
	}

	rows, err = db.Query("PRAGMA page_size")
	if err != nil {
		log.Printf("error in pragma query from database to page_size: %v", err)
		return 0, 0, err
	}

	defer rows.Close()
	for rows.Next() {
		if err = rows.Scan(&page_size); err != nil {
			return 0, 0, err
		}
	}

	pg_count, er := strconv.Atoi(page_count)
	pg_size, errr := strconv.Atoi(page_size)
	if er != nil {
		err = er
	}

	if errr != nil {
		err = errr
	}

	return pg_count, pg_size, err
}

func createPaths(amount int) []string {
	i := 1
	var paths []string
	for i < amount+1 {
		paths = append(paths, mapInputFile(i))
		i += 1
	}
	return paths
}

func shell(client *Interface) {
	scanner := bufio.NewScanner(os.Stdin)
	path := "austen.db"
	for scanner.Scan() {
		line := scanner.Text()

		var command string
		var input string

		fmt.Sscanf(line, "%s %s", &command, &input)

		switch command {
		case "quit":
			return

		case "write":
			if input != "" {
				path = input
			}

			number_of_rows, _ := getNumberOfRows(path)
			page_count, _, _ := getDatabaseSize(path)

			PATHS_AMOUNT := number_of_rows / page_count
			PATHS := createPaths(PATHS_AMOUNT)
			err := splitDatabase(path, PATHS)
			if err != nil {
				log.Fatalf("There was an error")
			}

			path_index := 0
			for path_index < len(PATHS) {
				p := PATHS[path_index]

				// make a new map task
				MapWorker := new(MapTask)
				MapWorker.SourceHost = p
				MapWorker.N = path_index
				err = MapWorker.Process(p, *client)
				if err != nil {
					log.Fatalf("MapWorker.Process: error: %v", err)
				}
				path_index += 1
			}

			// make a new reduce task

			break

		case "help":
			fmt.Println("Here are a list of commands to use:\nquit\nwrite\n")
			break

		default:
			log.Print("Error: Unrecognized command. Please type \"help\" for a list of commands.")
		}

	}
}
