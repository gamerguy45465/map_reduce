package main

import "database/sql"

func openDatabase(path string) (*sql.DB, error) {
	// the path to the database--this could be an absolute path
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
		// handle the error here
	} 
}

func createDatabase(path string) (*sql.DB, error) {

}

func splitDataBase(source, paths []string) error {

}

func mergeDatabases(urls []string, path string, temp string) (*sql.DB, error) {

}

func download(url, path string) error {

}

func gatherInfo(db *sql.DB, path string) error {

}

func main() {

}
