package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// shTime - used to parse non standart time format in Bitergia JSON
type shTime struct {
	time.Time
	Set bool
}

// shCountry - country data
type shCountry struct {
	Alpha3 string `json:"alpha3"`
	Code   string `json:"code"`
	Name   string `json:"name"`
}

// shProfile - singleprofile data
type shProfile struct {
	Country   *shCountry `json:"country"`
	Email     *string    `json:"email"`
	Gender    *string    `json:"gender"`
	GenderAcc *int       `json:"gender_acc"`
	IsBot     *bool      `json:"is_bot"`
	Name      *string    `json:"name"`
	UUID      string     `json:"uuid"`
}

// shIdentity - signgle identity data
type shIdentity struct {
	Email        *string `json:"email"`
	ID           string  `json:"id"`
	Name         *string `json:"name"`
	Source       string  `json:"source"`
	Username     *string `json:"username"`
	UUID         string  `json:"uuid"`
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

// importStats - statistics about added/updated/deleted objects
type importStats struct {
	uidentitiesAdded   int
	uidentitiesFound   int
	profilesAdded      int
	profilesFound      int
	profilesDeleted    int
	identitiesAdded    int
	identitiesFound    int
	identitiesDeleted  int
	enrollmentsAdded   int
	enrollmentsFound   int
	enrollmentsDeleted int
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

func queryOut(query string, args ...interface{}) {
	fmt.Printf("%s\n", query)
	if len(args) > 0 {
		s := ""
		for vi, vv := range args {
			switch v := vv.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, complex64, complex128, string, bool, time.Time:
				s += fmt.Sprintf("%d:%+v ", vi+1, v)
			case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64, *float32, *float64, *complex64, *complex128, *string, *bool, *time.Time:
				s += fmt.Sprintf("%d:%+v ", vi+1, v)
			case nil:
				s += fmt.Sprintf("%d:(null) ", vi+1)
			default:
				s += fmt.Sprintf("%d:%+v ", vi+1, reflect.ValueOf(vv).Elem())
			}
		}
		fmt.Printf("[%s]\n", s)
	}
}

func query(db *sql.DB, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		queryOut(query, args...)
	}
	return rows, err
}

func exec(db *sql.DB, skip, query string, args ...interface{}) (sql.Result, error) {
	res, err := db.Exec(query, args...)
	if err != nil {
		if skip == "" || !strings.Contains(err.Error(), skip) {
			queryOut(query, args...)
		}
	}
	return res, err
}

func addOrganization(db *sql.DB, company string) (int, bool) {
	_, err := exec(db, "Error 1062", "insert into organizations(name) values(?)", stripUnicodeStr(company))
	exists := false
	if err != nil {
		if strings.Contains(err.Error(), "Error 1062") {
			rows, err2 := query(db, "select name from organizations where name = ?", company)
			fatalOnError(err2)
			var existingName string
			for rows.Next() {
				fatalOnError(rows.Scan(&existingName))
			}
			fatalOnError(rows.Err())
			fatalOnError(rows.Close())
			exists = true
		} else {
			fatalOnError(err)
		}
	}
	rows, err := query(db, "select id from organizations where name = ?", company)
	fatalOnError(err)
	var id int
	fetched := false
	for rows.Next() {
		fatalOnError(rows.Scan(&id))
		fetched = true
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if !fetched {
		fatalf("failed to add '%s' company", company)
	}
	return id, exists
}

func addCountry(db *sql.DB, country *shCountry) (exists bool) {
	_, err := exec(
		db,
		"Error 1062",
		"insert into countries(code, alpha3, name) values(?,?,?)",
		country.Code,
		country.Alpha3,
		stripUnicodeStr(country.Name),
	)
	if err != nil {
		if strings.Contains(err.Error(), "Error 1062") {
			exists = true
		} else {
			fatalOnError(err)
		}
	}
	return
}

func getThreadsNum() int {
	// Use environment variable to have singlethreaded version
	st := os.Getenv("ST") != ""
	if st {
		return 1
	}
	nCPUs := 0
	if os.Getenv("NCPUS") != "" {
		n, err := strconv.Atoi(os.Getenv("NCPUS"))
		fatalOnError(err)
		if n > 0 {
			nCPUs = n
		}
	}
	if nCPUs > 0 {
		n := runtime.NumCPU()
		if nCPUs > n {
			nCPUs = n
		}
		runtime.GOMAXPROCS(nCPUs)
		return nCPUs
	}
	nCPUs = runtime.NumCPU()
	runtime.GOMAXPROCS(nCPUs)
	return nCPUs
}

func stripUnicode(pStr *string) *string {
	if pStr == nil {
		return nil
	}
	str := *pStr
	isOk := func(r rune) bool {
		return r < 32 || r >= 127
	}
	t := transform.Chain(norm.NFKD, transform.RemoveFunc(isOk))
	str, _, _ = transform.String(t, str)
	return &str
}

func stripUnicodeStr(str string) string {
	isOk := func(r rune) bool {
		return r < 32 || r >= 127
	}
	t := transform.Chain(norm.NFKD, transform.RemoveFunc(isOk))
	str, _, _ = transform.String(t, str)
	return str
}

func cleanUTF8(str string) string {
	if strings.Contains(str, "\x00") {
		return strings.Replace(str, "\x00", "", -1)
	}
	return str
}

func truncToBytes(str string, size int) string {
	str = cleanUTF8(str)
	length := len(str)
	if length < size {
		return str
	}
	res := ""
	i := 0
	for _, r := range str {
		if len(res+string(r)) > size {
			break
		}
		res += string(r)
		i++
	}
	return res
}

func truncStringOrNil(strPtr *string, maxLen int) interface{} {
	if strPtr == nil {
		return nil
	}
	return truncToBytes(*strPtr, maxLen)
}

func processUIdentity(ch chan struct{}, mtx *sync.RWMutex, db *sql.DB, uidentity shUIdentity, comp2id map[string]int, replace bool, stats *importStats) {
	defer func() {
		if ch != nil {
			ch <- struct{}{}
		}
	}()
	var sts importStats
	rows, err := query(db, "select uuid from uidentities where uuid = ?", uidentity.UUID)
	fatalOnError(err)
	uuid := uidentity.UUID
	fetched := false
	for rows.Next() {
		fatalOnError(rows.Scan(&uuid))
		fetched = true
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if !fetched {
		_, err := exec(
			db,
			"",
			"insert into uidentities(uuid, last_modified) values(?,now())",
			uidentity.UUID,
		)
		fatalOnError(err)
		sts.uidentitiesAdded++
	} else {
		sts.uidentitiesFound++
	}
	rows, err = query(
		db,
		"select uuid from profiles where uuid = ?",
		uidentity.UUID,
	)
	fatalOnError(err)
	fetched = false
	for rows.Next() {
		fatalOnError(rows.Scan(&uuid))
		fetched = true
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if fetched {
		fatalOnError(err)
		sts.profilesFound++
		if replace {
			_, err := exec(db, "", "delete from profiles where uuid = ?", uidentity.UUID)
			fatalOnError(err)
			sts.profilesDeleted++
		}
	}
	if !fetched || (fetched && replace) {
		var cCode *string
		if uidentity.Profile.Country != nil {
			cCode = &uidentity.Profile.Country.Code
		}
		_, err := exec(
			db,
			"",
			"insert into profiles(uuid, name, email, gender, gender_acc, is_bot, country_code) values(?,?,?,?,?,?,?)",
			uidentity.UUID,
			stripUnicode(uidentity.Profile.Name),
			stripUnicode(uidentity.Profile.Email),
			uidentity.Profile.Gender,
			uidentity.Profile.GenderAcc,
			uidentity.Profile.IsBot,
			truncStringOrNil(cCode, 2),
		)
		fatalOnError(err)
		sts.profilesAdded++
	}
	for _, identity := range uidentity.Identities {
		rows, err = query(
			db,
			"select uuid from identities where id = ? or (name = ? and email = ? and username = ? and source = ?)",
			identity.ID,
			stripUnicode(identity.Name),
			stripUnicode(identity.Email),
			stripUnicode(identity.Username),
			identity.Source,
		)
		fatalOnError(err)
		fetched = false
		for rows.Next() {
			fatalOnError(rows.Scan(&uuid))
			fetched = true
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
		if fetched {
			fatalOnError(err)
			sts.identitiesFound++
			if replace {
				_, err := exec(
					db,
					"",
					"delete from identities where id = ? or (name = ? and email = ? and username = ? and source = ?)",
					identity.ID,
					stripUnicode(identity.Name),
					stripUnicode(identity.Email),
					stripUnicode(identity.Username),
					identity.Source,
				)
				fatalOnError(err)
				sts.identitiesDeleted++
			}
		}
		if !fetched || (fetched && replace) {
			_, err := exec(
				db,
				"",
				"insert into identities(uuid, id, source, name, email, username, last_modified) values(?,?,?,?,?,?,now())",
				identity.UUID,
				identity.ID,
				identity.Source,
				stripUnicode(identity.Name),
				stripUnicode(identity.Email),
				stripUnicode(identity.Username),
			)
			fatalOnError(err)
			sts.identitiesAdded++
		}
	}
	rows, err = query(
		db,
		"select uuid from enrollments where uuid = ?",
		uidentity.UUID,
	)
	fatalOnError(err)
	fetched = false
	for rows.Next() {
		fatalOnError(rows.Scan(&uuid))
		fetched = true
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if fetched {
		fatalOnError(err)
		sts.enrollmentsFound++
		if replace {
			_, err := exec(db, "", "delete from enrollments where uuid = ?", uidentity.UUID)
			fatalOnError(err)
			sts.enrollmentsDeleted++
		}
	}
	for _, enrollment := range uidentity.Enrollments {
		if !fetched || (fetched && replace) {
			if mtx != nil {
				mtx.RLock()
			}
			compID, ok := comp2id[enrollment.Organization]
			if mtx != nil {
				mtx.RUnlock()
			}
			if !ok {
				fatalf("organization '%s'not found", enrollment.Organization)
			}
			_, err := exec(
				db,
				"",
				"insert into enrollments(uuid, organization_id, start, end) values(?,?,?,?)",
				enrollment.UUID,
				compID,
				enrollment.Start.Time,
				enrollment.End.Time,
			)
			fatalOnError(err)
			sts.enrollmentsAdded++
		}
	}
	if mtx != nil {
		mtx.Lock()
	}
	stats.uidentitiesAdded += sts.uidentitiesAdded
	stats.uidentitiesFound += sts.uidentitiesFound
	stats.profilesAdded += sts.profilesAdded
	stats.profilesFound += sts.profilesFound
	stats.profilesDeleted += sts.profilesDeleted
	stats.identitiesAdded += sts.identitiesAdded
	stats.identitiesFound += sts.identitiesFound
	stats.identitiesDeleted += sts.identitiesDeleted
	stats.enrollmentsAdded += sts.enrollmentsAdded
	stats.enrollmentsFound += sts.enrollmentsFound
	stats.enrollmentsDeleted += sts.enrollmentsDeleted
	if mtx != nil {
		mtx.Unlock()
	}
}

func importJSONfiles(db *sql.DB, fileNames []string) error {
	dbg := os.Getenv("DEBUG") != ""
	replace := os.Getenv("REPLACE") != ""
	nFiles := len(fileNames)
	if dbg {
		fmt.Printf("Importing %d files, replace mode: %v\n", nFiles, replace)
	}
	uidentitiesAry := []map[string]shUIdentity{}
	orgs := make(map[string]struct{})
	countries := make(map[string]*shCountry)
	for i, fileName := range fileNames {
		fmt.Printf("Importing %d/%d: %s\n", i+1, nFiles, fileName)
		var data shData
		contents, err := ioutil.ReadFile(fileName)
		fatalOnError(err)
		fatalOnError(json.Unmarshal(contents, &data))
		fmt.Printf("%s: %d records\n", fileName, len(data.UIdentities))
		for _, uidentity := range data.UIdentities {
			for _, enrollment := range uidentity.Enrollments {
				orgs[enrollment.Organization] = struct{}{}
			}
			if uidentity.Profile.Country != nil {
				code := uidentity.Profile.Country.Code
				_, ok := countries[code]
				if !ok {
					countries[code] = uidentity.Profile.Country
				}
			}
		}
		uidentitiesAry = append(uidentitiesAry, data.UIdentities)
	}
	comp2id := make(map[string]int)
	orgsAdded := 0
	var exists bool
	for comp := range orgs {
		comp2id[comp], exists = addOrganization(db, comp)
		if !exists {
			orgsAdded++
		}
		if dbg {
			fmt.Printf("Org '%s' -> %d\n", comp, comp2id[comp])
		}
	}
	fmt.Printf("Number of organizations: %d, added new: %d\n", len(comp2id), orgsAdded)
	countriesAdded := 0
	for _, country := range countries {
		exists = addCountry(db, country)
		if !exists {
			countriesAdded++
		}
	}
	fmt.Printf("Number of countries: %d, added new: %d\n", len(countries), countriesAdded)
	thrN := getThreadsNum()
	var mtx *sync.RWMutex
	if thrN > 1 {
		mtx = &sync.RWMutex{}
	}
	stats := &importStats{}
	for _, uidentities := range uidentitiesAry {
		if thrN > 1 {
			ch := make(chan struct{})
			nThreads := 0
			for _, uidentity := range uidentities {
				go processUIdentity(ch, mtx, db, uidentity, comp2id, replace, stats)
				nThreads++
				if nThreads == thrN {
					<-ch
					nThreads--
				}
			}
			for nThreads > 0 {
				<-ch
				nThreads--
			}
		} else {
			for _, uidentity := range uidentities {
				processUIdentity(nil, mtx, db, uidentity, comp2id, replace, stats)
			}
		}
	}
	fmt.Printf("Stats:\n%+v\n", stats)
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
