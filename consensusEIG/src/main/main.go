package main

import (
    "fmt"
    "os"
    "peer"
    "strconv"
    "sync"
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
    wg := make([]*sync.WaitGroup, 0)
    for i := 0; i <= faults; i++ {
        newWg := new(sync.WaitGroup)
        newWg.Add(graph.numNodes)
        wg = append(wg, newWg)
    }
    wg_done := new(sync.WaitGroup)
    wg_done.Add(graph.numNodes)

    msgComp := make([]int, graph.numNodes)
    timeComp := make([]int, graph.numNodes)
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
        go peer.Client(port, nbrs, byz, faults, wg, wg_done, &msgComp[i], &timeComp[i])
    }
    wg_done.Wait()
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
