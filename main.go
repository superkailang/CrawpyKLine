package main

import (
	"flag"
	"log"
)

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Println("start application error ", err)
			return
		}
	}()
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	// var (
	// apiPortFlag = flag.Int("port", 6001, "Listener port for the HTTP API connection")
	// )
	flag.Parse()
	//server.ListenAndServe(*apiPortFlag, *initClear)
}
