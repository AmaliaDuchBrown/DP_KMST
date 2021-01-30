package main

import (
	"fmt"
	"io"
	"os"
)

type edge struct {
	x, y string
	w    float64
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	fmt.Println("Welcome to the preprocesor of edges-files of the Kinetic MST system")
	fmt.Println("===================================================================")
	fmt.Println("The file to preprocess must be in the current directory")
	fmt.Println("The file must have the extension .edges")
	fmt.Printf("Enter the name of the edges file: ")
	var edgef string
	fmt.Scanln(&edgef)
	var err error
	rfile, err := os.Open(edgef + ".edges")
	check(err)
	defer rfile.Close()
	wfile, err := os.Create(edgef + ".requests")
	check(err)
	defer wfile.Close()
	for {
		var e edge
		_, err = fmt.Fscanf(rfile, "%s%s%f\n", &e.x, &e.y, &e.w)
		if err == io.EOF {
			break
		}
		_, err = fmt.Fprintln(wfile, "insert", e.x, e.y, e.w)
		check(err)
		//_, err = fmt.Fprintln(wfile, "graph")
		//check(err)
		_, err = fmt.Fprintln(wfile, "kmst")
		check(err)
	}
	_, err = fmt.Fprintln(wfile, "eof")
	check(err)
	wfile.Sync()
	fmt.Println("The output file is: ", edgef+".requests")
}
