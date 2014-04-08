package main

import (
    "fmt"
    "os"
    "peer"
    "strconv"
    "sync"
//    "runtime"
    )



type Graph struct {
    numNodes int
    nodes    []*peer.Node
    edges    map[*peer.Node]*peer.Node
}



var graph Graph

func checkErr(err error) {
    if err != nil {
        fmt.Printf("Fatal error: %s \n", err)
        os.Exit(1)
    }
}

func initGraph() {
    num, _ := strconv.Atoi(os.Args[1])
    graph = Graph{numNodes: num}
}



func main() {
    //Create a centralised monitor

    //Initialise graph
    initGraph()

    //Launch Client goroutines
    all := make([]string, graph.numNodes)
    for i := 0; i < graph.numNodes; i++{
         all[i] = ":" + strconv.Itoa(9000+i)
    }
    
    var byz int
    faults, _ := strconv.Atoi(os.Args[2])
    //conVal := make([]string, graph.numNodes)
    wg_g1:= new(sync.WaitGroup)
    wg_g2:= new(sync.WaitGroup)
    wg_g3:= new(sync.WaitGroup)
    wg_stage1:= new(sync.WaitGroup)
    wg_views := new(sync.WaitGroup)
    wg_tuples:= new(sync.WaitGroup)
    wg_final:= new(sync.WaitGroup)
    wg:= new(sync.WaitGroup)
    wg.Add(graph.numNodes)
    wg_final.Add(graph.numNodes)
    wg_g1.Add(graph.numNodes)
    wg_g2.Add(graph.numNodes)
    wg_g3.Add(graph.numNodes)
    wg_stage1.Add(graph.numNodes)
    wg_views.Add(graph.numNodes)
    wg_tuples.Add(graph.numNodes)
    msgComp := make([]int, graph.numNodes)
    timeComp := make([]int, graph.numNodes)

    //runtime.GOMAXPROCS(runtime.NumCPU())
    for i := 0; i < graph.numNodes; i++{
        port := strconv.Itoa(9000+i)
        nbrs := make([]string, graph.numNodes-1)
        for j, x := 0, 0; j < graph.numNodes; j++ {
            if j != i {
                nbrs[x] = all[j]
                x++
            }
        }
        switch {
            case i < faults: byz = 1
            default: byz = 0
        }
        go peer.Client(port, nbrs, byz, faults, wg_stage1, wg_views, wg_tuples, wg_g1, wg_g2, wg_g3, wg_final, wg, &msgComp[i], &timeComp[i])
    }
    wg.Wait()
    sum := 0
    for _, numMsg := range msgComp {
        sum += numMsg
    }
    time := 0
    for _, numR := range timeComp {
        if time < numR {
            time = numR
        }
    }
    fmt.Println("No. of Msgs sent:", sum)
    fmt.Println("No. of rounds :", time)
    fmt.Printf("Done!")
    os.Exit(0)
}
