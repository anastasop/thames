package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	PlayerChannelSize = 30
)

func usage() {
	fmt.Fprintf(os.Stderr, `usage: thames [-r root] [-n N] [--query] [--shuffle] [--mix] queries...

Thames is a browser and player for the BBC Sound Effects collection which
contains sounds from cafes, markets, cars, typewriters, nature etc.
You can browse the collection online at http://thames.acropolis.org.uk/.

Thames creates an index for the collection in an sqlite3 database, makes
full text queries to it and plays the sounds. Each query is an
sqlite3 full text query and is applied verbatim. Usually it is a single term
or a phrase but you can also use NEAR queries.

Some examples

play sounds from cafes

  thames cafe

play sounds from cafes and then from typewriters

  thames cafe typewriter

play sounds from cafes and typewriters interleaved

  thames --shuffle cafe typewriter

mix sounds from cafes and typewriters

  thames --mix cafe typewriter

go out in the wild nature

  thames --mix wind rain water fire

browse sounds from space

  thames --query space

Flags:
`)
	flag.PrintDefaults()
	os.Exit(2)
}

var (
	rootDir   = flag.String("r", "", "Directory of audio files")
	nsounds   = flag.Int("n", 30, "Number of sounds to play for each query")
	onlyQuery = flag.Bool("query", false, "Only query and print the results, don't download, don't play")
	shuffle   = flag.Bool("shuffle", false, "Interleave sounds from queries")
	mix       = flag.Bool("mix", false, "Mix the sounds from queries")

	soundsDir string
)

func soundPath(fname string) string {
	return filepath.Join(soundsDir, fname)
}

// playersRouter routes sounds to sound players, deciding by the query that originated a sound
// When mixing each query gets a dedicated player otherwise there is one player for all
type playersRouter interface {
	// route Returns a buffered channel for a player
	// The channels to players need to be buffered to avoid blocking the downloader
	// The size ideally should be a combination of the download latency and the
	// number of queries but for now we go with an empirical choice
	route(query string) chan sound

	// close Closes the channels of the router
	close()
}

// singlePlayersRouter is a playersRouter that always routes to the same player
type singlePlayersRouter struct {
	c chan sound
}

func newSinglePlayersRouter() *singlePlayersRouter {
	r := new(singlePlayersRouter)
	r.c = make(chan sound, PlayerChannelSize)

	return r
}

func (r *singlePlayersRouter) route(query string) chan sound {
	return r.c
}

func (r *singlePlayersRouter) close() {
	close(r.c)
}

// multiPlayersRouter is a playersRouter supporting many players. Use it when mixing
type multiPlayersRouter struct {
	sync.Mutex

	routes map[string]chan sound
}

func newMultiPlayersRouter() *multiPlayersRouter {
	r := new(multiPlayersRouter)
	r.routes = make(map[string]chan sound)

	return r
}

func (r *multiPlayersRouter) route(query string) chan sound {
	r.Lock()
	defer r.Unlock()

	c, present := r.routes[query]
	if !present {
		c = make(chan sound, PlayerChannelSize)
		r.routes[query] = c
	}

	return c
}

func (r *multiPlayersRouter) close() {
	r.Lock()
	defer r.Unlock()

	for _, c := range r.routes {
		close(c)
	}
}

func main() {
	log.SetPrefix("")
	log.SetFlags(log.Ltime)
	flag.Usage = usage
	flag.Parse()

	soundsDir = filepath.Join(*rootDir, "sounds")
	dbFile := filepath.Join(*rootDir, "sounds.db")
	csvFile := filepath.Join(*rootDir, "BBCSoundEffects.csv")
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		initDatabase(dbFile, csvFile)
	}

	db, err := sql.Open("sqlite3", "file:"+dbFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	stmt, err := db.Prepare(`SELECT location, description, secs FROM sounds WHERE sounds MATCH ? ORDER BY RANDOM() LIMIT ?`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	if *onlyQuery {
		for _, query := range flag.Args() {
			out := make(chan sound)
			go func() {
				for snd := range out {
					if _, err := os.Stat(snd.fpath); err == nil {
						fmt.Printf("%s %s\n", snd.descr, snd.fpath)
					} else {
						fmt.Printf("missing: %s\n", snd.fpath)
					}
				}
			}()
			queryDatabase(stmt, query, *nsounds, out)
		}

		os.Exit(0)
	}

	// a group to track inquirers, downloaders and players
	var wg sync.WaitGroup

	// router to players
	var router playersRouter
	if *mix {
		router = newMultiPlayersRouter()
	} else {
		router = newSinglePlayersRouter()
	}

	// downloader input
	downloadCh := make(chan sound)

	// launch the downloader. Only one for now, BBC seems to have throttling
	wg.Add(1)
	go func() {
		downloader(downloadCh, router)
		wg.Done()
	}()

	// launch the database inquirers. When finish, must close downloadCh
	wg.Add(1)
	go func() {
		if *shuffle || *mix {
			var qwg sync.WaitGroup
			for _, query := range flag.Args() {
				qwg.Add(1)
				go func(q string) {
					queryDatabase(stmt, q, *nsounds, downloadCh)
					qwg.Done()
				}(query)
			}
			qwg.Wait()
		} else {
			for _, query := range flag.Args() {
				queryDatabase(stmt, query, *nsounds, downloadCh)
			}
		}

		close(downloadCh)
		wg.Done()
	}()

	// launch players
	wg.Add(1)
	go func() {
		if !*mix {
			realPlayer(router.route(""))
		} else {
			for _, query := range flag.Args() {
				// players are added to the wait group because they will have stuff to play
				// after inquirers and downloader finish
				wg.Add(1)
				go func(q string) {
					realPlayer(router.route(q))
					wg.Done()
				}(query)
			}
		}

		wg.Done()
	}()

	// at this point we are waiting the players to play all the sounds assigned to them
	wg.Wait()
}

// initDatabase creates the schema in an sqlite3 database and fills the tables with the sounds records from the BBC csv
func initDatabase(dbFile, csvFile string) {
	log.Printf("Initializing database %s", dbFile)

	db, err := sql.Open("sqlite3", "file:"+dbFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	schemaSql := `CREATE VIRTUAL TABLE IF NOT EXISTS sounds USING fts4(
                        location, description, secs, category, CDNumber, CDName, tracknum,

                        tokenize=porter, notindexed=location, notindexed=secs, notindexed=CDNumber, notindexed=tracknum
                      )`
	if _, err := db.Exec(schemaSql); err != nil {
		log.Fatal(err)
	}

	fin, err := os.Open(csvFile)
	if err != nil {
		log.Fatal(err)
	}
	defer fin.Close()

	r := csv.NewReader(fin)
	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	insertSql := `INSERT INTO sounds(location, description, secs, category, CDNumber, CDName, tracknum) VALUES(?, ?, ?, ?, ?, ?, ?);`
	stmt, err := db.Prepare(insertSql)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	for _, record := range records {
		if _, err := stmt.Exec(record[0], record[1], record[2], record[3], record[4], record[5], record[6]); err != nil {
			log.Fatal(err)
		}
	}
}

type sound struct {
	descr string // the description of the sound
	fname string // file name of the sound in the DB index
	fpath string // full path of the sound file constructed by the downloader
	query string // the query for this sound. Used to route to proper player when mixing
	secs  int    // duration in seconds. Useful for logging
}

// queryDatabase sends query string q to database and sends each sound to out
func queryDatabase(stmt *sql.Stmt, query string, nsounds int, out chan<- sound) {
	rows, err := stmt.Query(query, nsounds)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var snd sound
		if err := rows.Scan(&snd.fname, &snd.descr, &snd.secs); err != nil {
			log.Fatal(err)
		}
		snd.query = query
		snd.fpath = soundPath(snd.fname)
		out <- snd
	}
	if rows.Err() != nil {
		log.Fatal(err)
	}
}

// downloader receives sounds from in, downloads the file, fills the path and sends to out (player)
func downloader(in <-chan sound, router playersRouter) {
	defer router.close()

	for snd := range in {
		sp := soundPath(snd.fname)
		exists, err := fileExists(sp)
		if err != nil || !exists {
			log.Printf("Missing File: %s: %v", sp, err)
		} else {
			router.route(snd.query) <- snd
		}
	}
}

// player receives and plays sounds
func player(in <-chan sound, mock bool) {
	for snd := range in {
		log.Printf("Playing: %q %s %s %s", snd.query, snd.descr, time.Duration(snd.secs)*time.Second, snd.fpath)

		if !mock {
			cmd := exec.Command("play", "-q", snd.fpath)
			if err := cmd.Run(); err != nil {
				log.Printf("Error:Play: %v", err)
			}
		}
	}
}

func realPlayer(in <-chan sound) {
	player(in, false)
}

func mockPlayer(in <-chan sound) {
	player(in, true)
}

func fileExists(fpath string) (bool, error) {
	if _, err := os.Stat(fpath); err == nil {
		return true, nil
	} else if !os.IsNotExist(err) {
		return false, err
	}

	return false, nil
}
