////////////////////////////////////////////////////////
//  Copyright Universitat Politecnica de Catalunya    //
//  Programming: A. Duch, E. Passarella, C. Zoltan    //
//  Date: 30/01/2021                                  //
///////////////////////////////////////////////////////

package main

//nodes are represented by int16
//buffered channels (size 12)

import (
	"flag"
	"fmt"
	"io"
//	"log"
	"os"
	"runtime"
//	"runtime/pprof"
//	"runtime/trace"
	"sort"
	//"strconv"

	_ "net/http/pprof"
	"time"
)

const graph = "graph"
const kmst = "kmst"
const eof = "eof"
const insert = "insert"
const update = "update"
const delete = "delete"

type edge struct {
	x, y int16
	w    float64
}

type request struct {
	op string
	e  edge
}

// Utility Functions ---------------------------------

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func normalize(r request) request {
	if r.e.y < r.e.x {
		return request{op: r.op, e: edge{x: r.e.y, y: r.e.x, w: r.e.w}}
	}
	return r
}

func deledg(e edge, ledg []edge) []edge {
	pos := -1
	for i, edg := range ledg {
		if e.x == edg.x && e.y == edg.y {
			pos = i
			break
		}
	}
	if pos != -1 {
		return append(ledg[:pos], ledg[pos+1:]...)
	}
	return ledg
}

func insUp(e edge, ledg []edge) []edge {
	pos := -1
	for i, edg := range ledg {
		if e.x == edg.x && e.y == edg.y {
			pos = i
			break
		}
	}
	if pos != -1 {
		ledg[pos].w = e.w
	}else {
		ledg = append(ledg, e)
	}
	return ledg
}


// MF-Sets Functions ------------------

func father(i int16, id []int16) int16 {
	for i != id[i] {
		id[i] = id[id[i]]
		i = id[i]
	}
	return i
}

func unite(p, q int16, id []int16) {
	var i, j int16
	i = father(p, id)
	j = father(q, id)
	id[i] = j
}

// Sequential kmst

func seqKmst (istream string) {
  // required variables: request and Graph
	var r request
	//var G map[int16][]edge
	G := make(map[int16][]edge)
	//	var err error
	rfile, err := os.Open(istream + ".requests")
	check(err)
	defer rfile.Close()
	check(err)

	for {
		_, err = fmt.Fscanf(rfile, "%s", &r.op)
		if err == io.EOF {
			break
		}
		check(err)
		if r.op == "eof" {
			break
		} else if r.op == kmst {
			var mst3 []edge
			for _ , l := range G {
          mst3 = append(mst3, l...)
			}
			sort.Slice(mst3, func(i, j int) bool {
				if mst3[i].w == mst3[j].w {
					return mst3[i].x < mst3[j].x
				}
				return mst3[i].w < mst3[j].w
			})
			m := make(map[int16]int16)
			var id []int16
			var cc int16
			cc = 0
			var mst2 []edge
			for _, e := range mst3 {
				_, ok1 := m[e.x]
				_, ok2 := m[e.y]
				if !ok1 || !ok2 {
					mst2 = append(mst2, e)
					if !ok1 {
						m[e.x] = cc
						id = append(id, cc)
						cc++
					}
					if !ok2 {
						m[e.y] = cc
						id = append(id, cc)
						cc++
					}
					unite(m[e.x], m[e.y], id)
					m[e.x] = id[m[e.x]]
					m[e.y] = id[m[e.y]]
				} else {
						if father(m[e.x],id) != father(m[e.y],id) {
						mst2 = append(mst2, e)
						unite(m[e.x], m[e.y], id)
						m[e.x] = id[m[e.x]]
						m[e.y] = id[m[e.y]]
					}
				}
			}
			//fmt.Println("KMST: ", mst2)
		} else if r.op == graph  {
			/*fmt.Println("Graph: ")
			for _ , l := range G {
          fmt.Println(l)
			}*/
    } else {
			_, err = fmt.Fscanf(rfile, "%d%d%f\n", &r.e.x, &r.e.y, &r.e.w)
			check(err)
			r := normalize(r)
			if r.op == insert || r.op == update {
				G[r.e.x] = insUp(r.e, G[r.e.x])
			} else {
				G[r.e.x] = deledg(r.e, G[r.e.x])
			}
	  }
	}
	rfile.Close()
}



// Main ---------------------------------
//  Daisy-chain Filter processes.
func main() {
	//Trace code ----------------------------
	//trace.Start(os.Stderr)
	//defer trace.Stop()
	//---------------------------------------
	var istream string
	//Pprof variables -----------------------
	//var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
	//var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
	flag.StringVar(&istream, "file", "", "Specify input file. Default is emptyfile")
	flag.Parse()
	/*
	//Pprof code -----------------------
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}*/
	//----------------------------------
	runtime.GOMAXPROCS(0)
	maxProcs := runtime.GOMAXPROCS(0)
	numCPU := runtime.NumCPU()
	fmt.Println("maxProcs: ", maxProcs, " numCPU: ", numCPU)
	start := time.Now()
  seqKmst(istream)
	t := time.Since(start)
	fmt.Println("TotalExecutionTime,", t, ",", t.Microseconds(), ",", t.Milliseconds(), ",", t.Seconds())
  /*
	//Pprof code ---------------------
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
	*/
	//-------------------------------


}
