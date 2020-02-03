package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"runtime/debug"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// shTime - used to parse non standart time format in Bitergia JSON
type shTime struct {
	time.Time
	Set bool
}

// shProfile - singleprofile data
type shProfile struct {
	Country   string `json:"country"`
	Email     string `json:"email"`
	Gender    string `json:"gender"`
	GenderAcc *int   `json:"gender_acc"`
	IsBot     *bool  `json:"is_bot"`
	Name      string `json:"name"`
	UUID      string `json:"uuid"`
}

// shIdentity - signgle identity data
type shIdentity struct {
	Email        string `json:"email"`
	ID           string `json:"id"`
	Name         string `json:"name"`
	Source       string `json:"source"`
	Username     string `json:"username"`
	UUID         string `json:"uuid"`
	LastModified time.Time
}

// shEnrollment - single company enrollment data
type shEnrollment struct {
	UUID         string `json:"uuid"`
	Organization string `json:"organization"`
	Start        shTime `json:"start"`
	End          shTime `json:"end"`
}

// shUIdentity - single unique identity data
type shUIdentity struct {
	UUID         string         `json:"uuid"`
	Profile      shProfile      `json:"profile"`
	Identities   []shIdentity   `json:"identities"`
	Enrollments  []shEnrollment `json:"enrollments"`
	LastModified time.Time
}

// shData - Bitergia's identities export data format
type shData struct {
	UIdentities map[string]shUIdentity `json:"uidentities"`
}

func fatalOnError(err error) {
	if err != nil {
		tm := time.Now()
		fmt.Printf("Error(time=%+v):\nError: '%s'\nStacktrace:\n%s\n", tm, err.Error(), string(debug.Stack()))
		fmt.Fprintf(os.Stderr, "Error(time=%+v):\nError: '%s'\nStacktrace:\n", tm, err.Error())
		panic("stacktrace")
	}
}

func fatalf(f string, a ...interface{}) {
	fatalOnError(fmt.Errorf(f, a...))
}

func (sht *shTime) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), "\"")
	if s == "null" {
		return
	}
	dtFmt := "2006-01-02T15:04:05"
	sht.Time, err = time.Parse(dtFmt, s)
	if err == nil {
		sht.Set = true
	}
	return
}

func importJSONfiles(db *sql.DB, fileNames []string) error {
	dbg := os.Getenv("DEBUG") != ""
	replace := os.Getenv("REPLACE") != ""
	nFiles := len(fileNames)
	if dbg {
		fmt.Printf("Importing %d files, replace mode: %v\n", nFiles, replace)
	}
	for i, fileName := range fileNames {
		fmt.Printf("Importing %d/%d: %s\n", i+1, nFiles, fileName)
		var data shData
		contents, err := ioutil.ReadFile(fileName)
		fatalOnError(err)
		fatalOnError(json.Unmarshal(contents, &data))
		fmt.Printf("%s: %d records\n", fileName, len(data.UIdentities))
		/*if dbg {
			fmt.Printf("%+v\n", data.UIdentities)
		}*/
	}
	return nil
}

// getConnectString - get MariaDB SH (Sorting Hat) database DSN
// Either provide full DSN via SH_DSN='shuser:shpassword@tcp(shhost:shport)/shdb?charset=utf8&parseTime=true'
// Or use some SH_ variables, only SH_PASS is required
// Defaults are: "shuser:required_pwd@tcp(localhost:3306)/shdb?charset=utf8
// SH_DSN has higher priority; if set no SH_ varaibles are used
func getConnectString(prefix string) string {
	//dsn := "shuser:"+os.Getenv("PASS")+"@/shdb?charset=utf8")
	dsn := os.Getenv(prefix + "DSN")
	if dsn == "" {
		pass := os.Getenv(prefix + "PASS")
		user := os.Getenv(prefix + "USR")
		if user == "" {
			user = os.Getenv(prefix + "USER")
		}
		proto := os.Getenv(prefix + "PROTO")
		if proto == "" {
			proto = "tcp"
		}
		host := os.Getenv(prefix + "HOST")
		if host == "" {
			host = "localhost"
		}
		port := os.Getenv(prefix + "PORT")
		if port == "" {
			port = "3306"
		}
		db := os.Getenv(prefix + "DB")
		if db == "" {
			fatalf("please specify database via %sDB=...", prefix)
		}
		params := os.Getenv(prefix + "PARAMS")
		if params == "" {
			params = "?charset=utf8&parseTime=true"
		}
		if params == "-" {
			params = ""
		}
		dsn = fmt.Sprintf(
			"%s:%s@%s(%s:%s)/%s%s",
			user,
			pass,
			proto,
			host,
			port,
			db,
			params,
		)
	}
	return dsn
}

func main() {
	// Connect to MariaDB
	if len(os.Args) < 2 {
		fmt.Printf("Arguments required: file.json [file2.json [...]]\n")
		return
	}
	dtStart := time.Now()
	var db *sql.DB
	dsn := getConnectString("SH_")
	db, err := sql.Open("mysql", dsn)
	fatalOnError(err)
	defer func() { fatalOnError(db.Close()) }()
	fatalOnError(importJSONfiles(db, os.Args[1:len(os.Args)]))
	dtEnd := time.Now()
	fmt.Printf("Time(%s): %v\n", os.Args[0], dtEnd.Sub(dtStart))
}
