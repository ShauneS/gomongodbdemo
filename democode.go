package main

import (
	"fmt"
	"log"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Person struct {
	Name  string
	Phone string
}

func main() {
	session, err := mgo.Dial("mongodb://gomongodb:gomongodb@ds061474.mongolab.com:61474/gomongodbdemo")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)

	c := session.DB("gomongodbdemo").C("people")
	err = c.Insert(&Person{"Ale", "+55 53 8116 9639"},
		&Person{"Cla", "+55 53 8402 8510"})
	if err != nil {
		log.Fatal(err)
	}

	result := Person{}
	err = c.Find(bson.M{"name": "Ale"}).One(&result)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Phone:", result.Phone)

	//import (
	//    "fmt"
	//    "log"
	//    "os"

	//    "gopkg.in/mgo.v2"
	//)

	//func main() {
	//    var (
	//        mongoURL        = os.Getenv("mongo_url")
	//        mongoUser       = os.Getenv("mongo_user")
	//        mongoPassword   = os.Getenv("mongo_password")
	//        mongoCollection = os.Getenv("mongo_collection")
	//    )

	//    var (
	//            mongoSession *mgo.Session
	//            database     *mgo.Database
	//            collection   *mgo.Collection
	//            err          error
	//        )

	//    addr := fmt.Sprintf("mongodb://%s:%s@%s", mongoUser, mongoPassword, mongoURL)

	//    // Dial up the server and establish a session
	//    if mongoSession, err = mgo.Dial(addr); err != nil {
	//        log.Fatal(err)
	//    }

	//    // Make sure the connection closes
	//    defer mongoSession.Close()

	//    // This will get the "default" database that the connection string specified
	//    database = mongoSession.DB("")

	//    // Get our collection
	//    collection = database.C(mongoCollection)

	//    // For debuging, print out the collection we found
	//    fmt.Printf("Collection: %+v", collection)

	//    // Close main function

}
