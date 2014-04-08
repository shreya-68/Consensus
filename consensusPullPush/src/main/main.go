package main

import (
    "fmt"
    "time"
    "os"
//    "net"
    "peer"
    "strconv"
    "math/rand"
    "math"
    "strings"
    "sync"
    )



type Graph struct {
    numNodes int
    nodes    []*peer.Node
    edges    map[*peer.Node]*peer.Node
}



var graph Graph

var c = 2

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


func getLog(n int) int {
    return int(float64(n)/math.Log(float64(n)))
}

func getCandStrs(len int) []string {
    candStrs := make([]string, 0)
    switch {
    case len == 1 :
        return []string{"1", "0"}
    default:
        strs := getCandStrs(len-1)
        for _, cand := range strs {
            candStrs = append(candStrs, strings.Join([]string{"1", cand}, ""))
            candStrs = append(candStrs, strings.Join([]string{"0", cand}, ""))
        }
        return candStrs
    }
}

func getGstring() string {
    len := c*getLog(graph.numNodes)
    gstring := ""
    for i := 1; i <= len; i++ {
        append := rand.Intn(2)
        gstring += strconv.Itoa(append) 
    }
    return gstring
}



func main() {
    //Create a centralised monitor

    //Initialise graph
    initGraph()

    rand.Seed(time.Now().UTC().UnixNano())
    //Launch Client goroutines
    all := make([]string, graph.numNodes)
    for i := 0; i < graph.numNodes; i++{
         all[i] = ":" + strconv.Itoa(9000+i)
    }

    gstring := getGstring()
    len := c*getLog(graph.numNodes)
    candStrings := getCandStrs(len)
    
    var byz int
    faults, _ := strconv.Atoi(os.Args[2])
    wg_pull := new(sync.WaitGroup)
    wg_pull.Add(graph.numNodes)
    wg := new(sync.WaitGroup)
    wg.Add(graph.numNodes)
    wg_push := new(sync.WaitGroup)
    wg_push.Add(graph.numNodes)
    wg_ans := new(sync.WaitGroup)
    wg_ans.Add(graph.numNodes)
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
        go peer.Client(port, nbrs, byz, faults, gstring, candStrings, wg_push, wg_ans, wg_pull, wg, &msgComp[i], &timeComp[i])
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
