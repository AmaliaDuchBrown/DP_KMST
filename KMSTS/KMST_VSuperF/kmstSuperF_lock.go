////////////////////////////////////////////////////////
//  Copyright Universitat Politecnica de Catalunya    //
//  Programming: A. Duch, E. Passarella, C. Zoltan    //
//  Date: 30/01/2021                                  //
///////////////////////////////////////////////////////

package main

//nodes are represented by
//buffered channels (size 12)

import (
	"flag"
	"fmt"
	"io"
//	"log"
	"os"
	"runtime"
	//"runtime/pprof"
	"runtime/trace"
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
	x, y int32
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

func father(i int32, id []int32) int32 {
	for i != id[i] {
		id[i] = id[id[i]]
		i = id[i]
	}
	return i
}

func unite(p, q int32, id []int32) {
	var i, j int32
	i = father(p, id)
	j = father(q, id)
	id[i] = j
}

// Goroutines ---------------------------------

//Input stage
func input(istream string, outr chan<- request, outl chan<- []edge, Fsize int) {

	//	var err error
	rfile, err := os.Open(istream + ".requests")
	check(err)

	defer rfile.Close()
	check(err)
	var r request
	for {
		_, err = fmt.Fscanf(rfile, "%s", &r.op)
		if err == io.EOF {
			break
		}
		check(err)
		if r.op == kmst || r.op == graph || r.op == eof {
			r.e = edge{x: -1, y: -1, w: 0.0}
		} else {
			_, err = fmt.Fscanf(rfile, "%d%d%f\n", &r.e.x, &r.e.y, &r.e.w)
			check(err)
		}
		r := normalize(r)
		outr <- r
		if r.op == "eof" {
			break
		}
		if r.op == kmst || r.op == graph {
			empty := make([]edge, 0)
			outl <- empty
		}
	}
	close(outr)
	close(outl)
	rfile.Close()
}

//Ouput stage
func output(istream string, start time.Time, inr <-chan request,
	inl <-chan []edge, endchan chan<- request) {
	/*ncpu := runtime.NumCPU()
	tnow := time.Now()
	outf, err := os.Create(istream + tnow.Format("20060102150405") + "-" + strconv.Itoa(ncpu) + ".kmst") //kmst's sequence file
	check(err)
	defer outf.Close()
	stf, err := os.Create(istream + tnow.Format("20060102150405") + "-" + strconv.Itoa(ncpu) + ".csv") //statistics file
	check(err)
	defer stf.Close()
	fmt.Fprintln(stf, "Op  NumOp   Num goroutines  GoTime(µs)  MicroSec   MilliSec   Sec")
	numop := 0 //compute the number of operations
	localTime := start*/
	for {
		//ng := runtime.NumGoroutine()
		//numop = numop + 1
		r, ok := <-inr
		//t := time.Since(localTime)
		if ok {
			//fmt.Fprintln(outf, numop, "En output", r.op)
			switch r.op {
			case graph:
				<-inl
				//g, _ := <-inl
				//fmt.Fprintln(stf, "graph,", numop, ",", ng, ",", t, ",", t.Microseconds(), ",",t.Milliseconds(), ",", t.Seconds())
				//fmt.Fprintln(outf, numop, "Graph", g)
                //fmt.Println("Graph", g)
			case kmst:
				<-inl
				//g, _ := <-inl
				//fmt.Fprintln(stf, "kmst,", numop, ",", ng, ",", t, ",", t.Microseconds(), ",",t.Milliseconds(), ",", t.Seconds())
				//fmt.Fprintln(outf, numop, "Kmst", ng)
				//fmt.Println("KMST: ", g)
			case eof:
				//fmt.Fprintln(outf, numop, "Request en output", r)
				endchan <- r
				break
			default:
				//fmt.Fprintln(outf, numop, "Unknown operation in output")
				break
			}
		} else {
			break
		}
		//localTime = time.Now() //here to do not take into account the file writing time
	}
	//t := time.Since(start)
	//fmt.Fprintln(outf, numop-2, ",", "TotalExecutionTime,", t, ",", t.Microseconds(),",", t.Milliseconds(), ",", t.Seconds())
}

// Generator stage
func generator(inr <-chan request, inl <-chan []edge, outr1 chan<- request, outl1 chan<- []edge, Fsize int) {
	for {
		r, ok := <-inr
		if ok {
			switch r.op {
			case insert, update:
				outr := make(chan request, 12) //channel transporting requests
				outl := make(chan []edge, 12)  //channel transporting graph/kmst
				go filter(inr, outr, inl, outl, r.e, Fsize)
				inr = outr
				inl = outl
			case delete:
			case graph, kmst:
				g, _ := <-inl
				outr1 <- r
				outl1 <- g
			case eof:
				outr1 <- r
				break
			default: //something's wrong
				fmt.Println("Unknown operation in generator")
				break
			}
		} else {
			break
		}
	}
	close(outr1)
	close(outl1)
}

//Filter stage
func filter(inr <-chan request, outr chan<- request,
	inl <-chan []edge, outl chan<- []edge, e edge, Fsize int) {
    //filter memory
    var root = make(map[int32][]edge)
		runtime.LockOSThread()
	root[e.x]= append(root[e.x], e) //e.x is inside the filter id-parameters
    //Commented below V8
    //var adjv []int32 //adjacents nodes
	// var adje []edge  //incident edges
	//adjv = append(adjv, e.y)
	//adje = append(adje, e)
	for {
		r, ok := <-inr
		if ok {
			switch r.op {
			case eof:
				outr <- r
				break
			case graph:
				outr <- r
				g, _ := <-inl
                for _, adje := range root{
				    g = append(g, adje...)
                }
				outl <- g
			case kmst:
				outr <- r
				mst1, _ := <-inl
				//--------------------------------------------------
				var mst3 []edge
                mst3 = append(mst3,mst1...)
                for _ , adje := range root{
				    mst3 = append(mst3, adje...)
                }
				sort.Slice(mst3, func(i, j int) bool {
					if mst3[i].w == mst3[j].w {
						return mst3[i].x < mst3[j].x
					}
					return mst3[i].w < mst3[j].w
				})
				m := make(map[int32]int32)
				var id []int32
				var cc int32
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
						if father(m[e.x], id) != father(m[e.y], id) {
							mst2 = append(mst2, e)
							unite(m[e.x], m[e.y], id)
							m[e.x] = id[m[e.x]]
							m[e.y] = id[m[e.y]]
						}
					}
				}
				outl <- mst2
				//end kmst
			default:
				if _ , ok := root[r.e.x]; ok{
                    switch r.op {
					    case insert, update:
						    root[r.e.x] = insUp(r.e, root[r.e.x])
                        case delete:
                            root[r.e.x] = deledg(r.e, root[r.e.x])
                    }
				} else  if len(root) < Fsize {
                    if r.op != delete {
                        root[r.e.x]= append(root[r.e.x],r.e)
                        //fmt.Println("map:", root)
                    } else {
                        outr <- r
                    }
                } else {
					outr <- r
                }
			}
		} else {
            break
        }
    }
	close(outr)
	close(outl)
}

// Main ---------------------------------
//  Daisy-chain Filter processes.
func main() {

	//Trace code ----------------------------
	//trace.Start(os.Stderr)
	ft, err := os.Create("trace.out")
	if err != nil {
		panic(err)
	}
	defer ft.Close()
	err = trace.Start(ft)
	if err != nil {
		panic(err)
	}
	defer trace.Stop()
	//---------------------
	//Trace code ----------------------------can be erased
	//trace.Start(os.Stderr)
	//defer trace.Stop()
	//---------------------------------------
    // Filter size (number of "root nodes" stored by each filter)
    var Fsize int
    fmt.Scan(&Fsize)
    //
    //-----------------------
    //
	var istream string
	//Pprof variables -----------------------
	//var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
	//var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
	flag.StringVar(&istream, "file", "", "Specify input file. Default is emptyfile")
	flag.Parse()
    //
    // ----------------------------
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
	inr := make(chan request, 12)     //channel transporting requests
	inl := make(chan []edge, 12)      //channel transporting sorted lists (graph/kmst)
	outr := make(chan request, 12)    //channel transporting requests
	outl := make(chan []edge, 12)     //channel transporting sorted lists (graph/kmst)
	endchan := make(chan request, 12) //channel transporting sorted lists (graph/kmst)
	start := time.Now()
	go input(istream, inr, inl, Fsize) // Launch Input.
	go generator(inr, inl, outr, outl, Fsize)
	go output(istream, start, outr, outl, endchan)
	<-endchan
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
