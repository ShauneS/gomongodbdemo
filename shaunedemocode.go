package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"net/http"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type HypotenuseValue struct {
	ID          bson.ObjectId `bson:"_id,omitempty"`
	SQLServerID int64
	Hypotenuse  float64
}

type ProgressRecord struct {
	ID               bson.ObjectId `bson:"_id"`
	Process          string
	TotalRecords     int64
	RecordsProcessed int64
	CompletionStatus string
	ProcessingDate   string
}

// Start: Status web server code

func MonitorProgress(w http.ResponseWriter, r *http.Request) {

	// Report progress
	fmt.Fprint(w, GetProcessStatus())
}

func GetProcessStatus() string {

	// Connect to MongoDB
	session, err := mgo.Dial("mongodb://gomongodb:gomongodb@ds061474.mongolab.com:61474/gomongodbdemo")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// Read Progress data
	progressrecords := session.DB("gomongodbdemo").C("ProgressRecords")

	currentprogress := ProgressRecord{}
	err = progressrecords.Find(bson.M{"process": "CalculateHypotenuses"}).One(&currentprogress)
	if err != nil {
		log.Fatal(err)
	}

	// Return it as a string
	return fmt.Sprintf("Progress is good! Process= %s is at %d of %d records! Process is currently %s on %s...",
		currentprogress.Process, currentprogress.RecordsProcessed, currentprogress.TotalRecords,
		currentprogress.CompletionStatus, currentprogress.ProcessingDate)
}

// End: Status web server code

// Start: Data processing code

func ProcessData() {

	var (
		oppositeside         float64
		adjacentside         float64
		sqlserverid          int64
		hypotenuse           float64
		processedrecordcount int64 = 0
		totalrecordcount     int64
	)

	// Connect to SQL Server for reading
	sqldb, sqlerr := sql.Open("mssql", "server=99.198.105.178; user id=gomongodb; password=gomongodb; port=2433; database=GoDemo")
	if sqlerr != nil {
		log.Fatal("Could not open SQL Server connection: ", sqlerr.Error())
	}
	defer sqldb.Close()

	// Connect to MongoDB for writing
	mongodb, merr := mgo.Dial("mongodb://gomongodb:gomongodb@ds061474.mongolab.com:61474/gomongodbdemo")
	if merr != nil {
		log.Fatal("Could not open MongoDB connection: ", merr.Error())
	}
	defer mongodb.Close()
	mongodb.SetMode(mgo.Monotonic, true)
	hypotenusecollection := mongodb.DB("gomongodbdemo").C("HypotenuseValues")
	progressrecordcollection := mongodb.DB("gomongodbdemo").C("ProgressRecords")
	processprogressrecord := ProgressRecord{}
	prgcolrecfinder := bson.M{"process": "CalculateHypotenuses"}
	progerr := progressrecordcollection.Find(prgcolrecfinder).One(&processprogressrecord)
	if progerr != nil {
		log.Fatal("Could not get process progress record: ", progerr.Error())
	}

	// Get Triangle side data
	//tsides, tserr := sqldb.Query("EXEC [dbo].[GetTriangleSidePage]  @pagesize = ?, @pageindex =?", pagesize, pageindex)
	tcount, tcerr := sqldb.Query("SELECT Count(*) FROM TriangleSides")
	if tcerr != nil {
		log.Fatal("Could not retrieve triangle count data: ", tcerr.Error())
	}
	tcount.Next()
	tcscanerr := tcount.Scan(&totalrecordcount)
	if tcscanerr != nil {
		log.Fatal("Could not scan triangle side data count: ", tcscanerr.Error())
	}

	tsides, tserr := sqldb.Query("SELECT TriangleSideID, OppositeSide, AdjacentSide FROM TriangleSides ORDER BY TriangleSideID")
	if tserr != nil {
		log.Fatal("Could not retrieve triangle side data: ", tserr.Error())
	}
	defer tsides.Close()

	// Process SQL Server data
	for tsides.Next() {
		scanerr := tsides.Scan(&sqlserverid, &oppositeside, &adjacentside)
		if scanerr != nil {
			log.Fatal("Could not scan triangle side data: ", scanerr.Error())
		}

		// Calculate hypotenuse data for MongoDB
		hypotenuse = math.Sqrt((oppositeside * oppositeside) + (adjacentside * adjacentside))

		// Write result to MongoDB
		hyperr := hypotenusecollection.Insert(&HypotenuseValue{SQLServerID: sqlserverid, Hypotenuse: hypotenuse})
		if hyperr != nil {
			log.Fatal("Could not insert hypotenuse into MongoDB: ", hyperr.Error())
		}

		// Write processing status to MongoDB.ProgressRecords
		processedrecordcount += 1
		processprogressrecord.RecordsProcessed = processedrecordcount
		processprogressrecord.TotalRecords = totalrecordcount
		processprogressrecord.CompletionStatus = "running"
		processprogressrecord.ProcessingDate = time.Now().Format(time.RFC850)
		prgcolrecfinder := bson.M{"process": "CalculateHypotenuses"}
		progrecerr := progressrecordcollection.Update(prgcolrecfinder, &processprogressrecord)
		if progrecerr != nil {
			log.Fatal("Could not insert progress into MongoDB: ", progrecerr.Error())
		}

		// Insert delay to allow us to observe process as it runs
		time.Sleep(1500 * time.Millisecond)
	}
	tsideserr := tsides.Err()
	if tsideserr != nil {
		log.Fatal("Could not iterate triangle side data: ", tsideserr.Error())
	}

	// Write completion processing status to MongoDB.ProgressRecords
	processprogressrecord.RecordsProcessed = processedrecordcount
	processprogressrecord.CompletionStatus = "completed"
	processprogressrecord.ProcessingDate = time.Now().Format(time.RFC850)
	cprogrecerr := progressrecordcollection.Update(prgcolrecfinder, &processprogressrecord)
	if cprogrecerr != nil {
		log.Fatal("Could not insert progress into MongoDB: ", cprogrecerr.Error())
	}

}

// End: Data processing code

func main() {
	// Start processing SQL Server data into MongoDB
	go ProcessData()

	// Start web server to provide status requests
	http.HandleFunc("/getstatus", MonitorProgress)
	err := http.ListenAndServe("localhost:4000", nil)
	if err != nil {
		log.Fatal("Listener died muttering: ", err)
	}

}
