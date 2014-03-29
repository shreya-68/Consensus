
package peer

import (
    "fmt"
    "time"
    "os"
    "net"
    "strconv"
    "math/rand"
    "math"
    "strings"
    )

//Client Node with name, nbr are the first hop neighbours and status is current running status
type Node struct {
    name    string
    nbr     []*net.TCPAddr
    status  string
    addr    *net.TCPAddr
    val     int 
    setVal  []int
    list    []int
    byz     int
    numNodes int
    numFaults int
    c       chan string
}

type Bin struct {
    num int
    list  []BinEntry
}

type BinEntry struct {
    port int
    confidence int
}
    
type Tuple struct {
    from int
    s_j []int
    comm int
    disq int
    comp []int
}

type Comp struct {
    comm int
    s_j []int
    D   []int
    count []int
}

func checkErr(err error) {
    if err != nil {
        fmt.Printf("Fatal error: %s \n", err)
        os.Exit(1)
    }
}

func getPortFromConn(conn net.Conn) int {
    tcpAddr, err := net.ResolveTCPAddr("tcp", conn.LocalAddr().String())
    checkErr(err)
    return tcpAddr.Port
}

func (node *Node) handleClient(conn net.Conn) {
    var buf [256]byte
    n, err := conn.Read(buf[0:])
    checkErr(err)
    msg := string(buf[0:n])
    checkErr(err)
    node.c <- msg
    //node.setVal = append(node.setVal, val)
    //fmt.Println("read")
    //fmt.Println(string(buf[0:n]))
    conn.Close() 
}

func (node *Node) accept(listener *net.TCPListener) {
    for {
            conn, err := listener.Accept()
            if err != nil {
                continue
            }
            go node.handleClient(conn)
            //node.connections[0] = &net.TCPConn(conn)
        }
}

func (node *Node) listen() {
    listener, err := net.ListenTCP("tcp", node.addr)
    checkErr(err)
    go node.accept(listener)
}

func (node *Node) openTCPconn(rcvr *net.TCPAddr) *net.TCPConn {
    conn, err := net.DialTCP("tcp", nil, rcvr)
    checkErr(err)
    return conn
}

func (node *Node) write(msg string, conn *net.TCPConn) {
        //fmt.Printf("Writing %s\n", msg)
        _, err := conn.Write([]byte(msg))
        checkErr(err)
}

func (node *Node) broadcast(msg string) {
    for _, nbr := range node.nbr {
        conn := node.openTCPconn(nbr)
        node.write(msg, conn)
    }
}

func getNumOfComm(n int) int {
    return int(float64(n)/math.Log(float64(n)))
}

func (node *Node) getGradecast1Store() map[int][]int{
    binStore := make(map[int][]int)
    for {
        select {
            case binEntry := <-node.c :
                arr := strings.Split(binEntry, ":")
                port, err := strconv.Atoi(arr[0])
                checkErr(err)
                bin, err := strconv.Atoi(arr[1])
                checkErr(err)
                binStore[bin] = append(binStore[bin], port)
            default:
                    //fmt.Println(binStore)
                    return binStore
        }
    }
}

func getViews(bin Bin) (Bin, Bin) {
    accept := Bin{num: bin.num}
    adopt := Bin{num: bin.num}
    for _, entry := range bin.list {
        switch {
            case (entry.confidence == 2):
                accept.list = append(accept.list, entry)
            case (entry.confidence == 1):
                adopt.list = append(adopt.list, entry)
        }
    }
    return accept, adopt
}


func getLightestBin(binStore map[int][]int) (int, []int) {
    //returns lightest bin
    minVal := -1
    var minKey int
    for key, value := range binStore {
        if (len(value) < minVal) || (minVal == -1) || (len(value) == minVal && key < minKey) {
            minKey = key
            minVal = len(value)
        }
    }
    return minKey, binStore[minKey]
}

func (node *Node) getGradecast2Store() map[int][]int{
    binStore := make(map[int][]int)
    myPort, _ := strconv.Atoi(node.name)
    for {
        select {
            case binEntry := <-node.c :
                bins := strings.Split(binEntry, ",")
                for _, bin := range bins {
                    portVal := strings.Split(bin, ":")
                    ports := strings.Split(portVal[0], ".")
                    val, _ := strconv.Atoi(portVal[1])
                    for _, port := range ports {
                        portNum, _ := strconv.Atoi(port)
                        if myPort != portNum {
                            binStore[portNum] = append(binStore[portNum], val)
                        }
                    }
                }
            default:
                    //fmt.Println(binStore)
                    return binStore
        }
    }
}

func (node *Node) getPortBinStore() map[int][]int{
    portBinStore := make(map[int][]int)
    for {
        select {
            case portEntry := <-node.c :
                ports := strings.Split(portEntry, ",")
                for _, eachport := range ports {
                    portBin := strings.Split(eachport, ":")
                    port, _ := strconv.Atoi(portBin[0])
                    bin, _ := strconv.Atoi(portBin[1])
                    portBinStore[port] = append(portBinStore[port], bin)
                }
            default:
                //fmt.Println(portBinStore)
                return portBinStore
        }
    }
}
            
func (node *Node) getBins(portBinStore map[int][]int, myBin int) []Bin{
    var count map[int]int
    binStore := make(map[int][]BinEntry)
    myPort, _ := strconv.Atoi(node.name)
    binStore[myBin] = append(binStore[myBin], BinEntry{port: myPort, confidence: 2})
    maxBinKey := -1
    for key, value := range portBinStore {
        count = make(map[int]int)
        for _, each := range value {
            count[each] += 1
        }
        if myPort != key {
            port := BinEntry{port: key}
            bin := -1
            for binkey, binvalue := range count {
                switch {
                    //TODO; check this if error occurs
                    case (binvalue >= (2*node.numFaults) + 1) && binkey != -1 :
                        bin = binkey
                        port.confidence = 2
                        break
                    case (2*node.numFaults >= binvalue) && (binvalue >= (node.numFaults) + 1) && binkey != -1 && bin == -1 :
                        bin = binkey
                        port.confidence = 1
                }
                if maxBinKey < binkey {
                    maxBinKey = binkey
                }
            }
            if bin == -1 {
                port.confidence = 0
            }
            binStore[bin] = append(binStore[bin], port)
        }
    }
    delete(binStore, -1)
    fmt.Printf("My name is %d\n", myPort)
    //fmt.Println(binStore)
    bins := make([]Bin, 0)
    for i := 0; i <= maxBinKey; i++ {
        list, ok := binStore[i]
        if ok {
            bins = append(bins, Bin{num: i, list: list})
        }
    }
    return bins
}


func (node *Node) gradecastStep1(value int) map[int][]int {
    msg := node.name + ":" + strconv.Itoa(value)
    node.broadcast(msg)
    time.Sleep(200*time.Millisecond) 
    return node.getGradecast1Store()
}

func (node *Node) gradecastStep2(binStore map[int][]int) map[int]int {
    //returns mapping of port to bin number if mu of all other players voted for that bin number for that player else player mapped to -1.
    msg := ""
    for key, value := range binStore {
        for _, port := range value {
            msg += strconv.Itoa(port) + "."
        }
        msg = strings.TrimRight(msg, ".")
        msg += ":" + strconv.Itoa(key) + ","
    }
    msg = strings.TrimRight(msg, ",")
    node.broadcast(msg)
    time.Sleep(200*time.Millisecond) 
    portStore := node.getGradecast2Store()
    for key, value := range binStore {
        for _, port := range value {
            portStore[port] = append(portStore[port], key)
        }
    }
    var count map[int]int
    portBin := make(map[int]int)
    for key, value := range portStore {
        count = make(map[int]int)
        for _, each := range value {
            count[each] += 1
        }
        for binkey, num := range count {
            if num >= (node.numNodes - node.numFaults - 1){
                portBin[key] = binkey
            }
        }
        _, ok := portBin[key]
        if !ok {
            portBin[key] = -1
        }
    }
    //fmt.Println(portBin)
    return portBin
}

func (node *Node) gradecastStep3(portStore map[int]int, myBin int) []Bin {
    msg := ""
    for key, value := range portStore {
        msg += strconv.Itoa(key) + ":" + strconv.Itoa(value) + ","
    }
    msg = strings.TrimRight(msg, ",")
    node.broadcast(msg)
    time.Sleep(200*time.Millisecond) 
    portBinStore := node.getPortBinStore()
    for key, value := range portStore {
            portBinStore[key] = append(portBinStore[key], value)
    }
    bins := node.getBins(portBinStore, myBin)
    //fmt.Println("After gradecast Step 3")
    //fmt.Println(bins)
    //bin := getLightestBin(bins)
    //accept, adopt := getViews(bin)
    //fmt.Println(accept)
    //fmt.Println(adopt)
    return bins
}

func (node *Node) implementGradecast(myVal int) []Bin {
    binStore := node.gradecastStep1(myVal)
    fmt.Println("GradeCast 1 done")
    portBin := node.gradecastStep2(binStore)
    fmt.Println("GradeCast 2 done")
    bins := node.gradecastStep3(portBin, myVal)
    fmt.Println("GradeCast 3 done")
    fmt.Println(bins)
    return bins
}

func (node *Node) selectiveBroadcast(msg string, list []int) {
    myPort, _ := strconv.Atoi(node.name)
    for _, val := range list {
        if myPort != val {
            port := ":" + strconv.Itoa(val)
            addr, err := net.ResolveTCPAddr("tcp", port)
            checkErr(err)
            conn := node.openTCPconn(addr)
            node.write(msg, conn)
        }
    }
}

func (node *Node) getTuples() []Tuple{
    entries := make([]Tuple, 0)
    for {
        select {
            case entry := <-node.c :
                msg := strings.Split(entry, ",")
                for _, tuple := range msg {
                    arr := strings.Split(tuple, ".")
                    if(len(arr) == 5) {
                        from, _ := strconv.Atoi(arr[0])
                        s_j := make([]int, 0)
                        subset := strings.Split(arr[1], ":")
                        for _, value := range subset {
                            port, _ := strconv.Atoi(value)
                            s_j = append(s_j, port)
                        }
                        comm, _ := strconv.Atoi(arr[2])
                        disq, _ := strconv.Atoi(arr[3])
                        comp := make([]int, 0)
                        accepts := strings.Split(arr[4], ":")
                        for _, value := range accepts {
                            accept, _ := strconv.Atoi(value)
                            comp = append(comp, accept)
                        }
                        entries = append(entries, Tuple{from: from, s_j: s_j, comm: comm, disq: disq, comp: comp})
                    }
                }
            default:
                return entries
        }
    }
}

func (node *Node) getIntersection() map[int][]int {
    adoptInter := make(map[int][]int)
    for {
        select {
            case entry := <-node.c :
                comms := strings.Split(entry, ",")
                for _, comm := range comms {
                    arr := strings.Split(comm, ":")
                    num, _ := strconv.Atoi(arr[0])
                    members := strings.Split(arr[1], ".")
                    for _, member := range members {
                        port, err := strconv.Atoi(member)
                        checkErr(err)
                        found := 0
                        for _, each := range adoptInter[num] {
                            if each == port {
                                found = 1
                                break
                            }
                        }
                        if found == 0 {
                            adoptInter[num] = append(adoptInter[num], port)
                        }
                    }
                }
            default:
                return adoptInter
        }
    }
}

func (node *Node) getComposition(acceptee map[int][]int, list []int, numComm int) map[int][]int {
    msg := ""
    for num, list := range acceptee {
        msg += strconv.Itoa(num) + ":"
        for _, accepted := range list {
            msg += strconv.Itoa(accepted) + "."
        }
        msg = strings.TrimRight(msg, ".")
        msg += ","
    }
    msg = strings.TrimRight(msg, ",")
    node.selectiveBroadcast(msg, list)
    time.Sleep(200*time.Millisecond) 
    accepts := node.getIntersection()
    time.Sleep(200*time.Millisecond) 
    return accepts
}

func (node *Node) subprotocol(adoptee map[int][]int, acceptee map[int][]int, list []int, numComm int) {
    adopts := node.getComposition(adoptee, list, numComm)
    accepts := node.getComposition(acceptee, list, numComm)
    disq := make(map[int]int)
    tupleMsg := make([]Tuple, 0)
    for key, value := range adopts {
        newTuple := Tuple{s_j: list, comm: key}
        switch {
            case len(value) > numComm :
                disq[key] = 1
                newTuple.disq = 1
                tupleMsg = append(tupleMsg, newTuple)
        }
    }
    for key, value := range accepts {
        newTuple := Tuple{s_j: list, comm: key}
        _, ok := disq[key]
        switch {
            case !ok :
                disq[key] = 1
                newTuple.disq = 0
                newTuple.comp = value
                tupleMsg = append(tupleMsg, newTuple)
        }
    }
    msg := "" 
    for _, tuple := range tupleMsg {
        msg = node.name + "."
        for _, val := range tuple.s_j {
            msg += strconv.Itoa(val) + ":"
        }
        msg = strings.TrimRight(msg, ":")
        msg += "." + strconv.Itoa(tuple.comm) + "." + strconv.Itoa(tuple.disq) + "." 
        for _, accept := range tuple.comp {
            msg += strconv.Itoa(accept) + ":"
        }
        msg = strings.TrimRight(msg, ":")
        msg += ","
    }
    msg = strings.TrimRight(msg, ",")
    node.broadcast(msg)

}

    
func getCombination(list []int, k int) [][]int {
    ret := make([][]int, 0)
    first := list[0]
    if k == 0 {
        return ret
    }
    if len(list) == k {
        ret = append(ret, list)
        return ret
    }
    keep := getCombination(list[1:], k-1)
    noKeep := getCombination(list[1:], k)
    if len(keep) == 0 {
        newRow := []int{first} 
        ret = append(ret, newRow)
    }
    for _, row := range keep {
        newRow := make([]int, 0)
        newRow = append(newRow, first)
        newRow = append(newRow, row...)
        ret = append(ret, newRow)
    }
    for _, row := range noKeep {
        ret = append(ret, row)
    }
    return ret
}

func find(list []int, num int) int {
    for _, value := range list {
        if value == num {
            return 1
        }
    }
    return 0
}

func subset(sub []int, super []int) int {
    for _, elem := range sub {
        if find(super, elem) == 0 {
            return 0
        }
    }
    return 1
}

func equal(a []int, b []int) int {
    if len(a) != len(b) {
        return 0
    }
    for _, elem := range a {
        if find(b, elem) == 0 {
            return 0
        }
    }
    return 1
}

func getFinalComp(comps []Comp) []int {
    maxLen := -1
    maxIndex := -1
    for index, comp := range comps {
        if maxLen < len(comp.D) {
            maxLen = len(comp.D)
            maxIndex = index
        }
    }
    return comps[maxIndex].D
}


func (node *Node) initRound(roundNum int) {
    var numComm int
    numComm = getNumOfComm(node.numNodes)
    fmt.Printf("the num of committees for %d nodes are %d\n", node.numNodes, numComm)
    myComm := getRandComm(numComm)
    fmt.Printf("I am %s. Setval: %d\n", node.name, myComm)
    var s_j int 
    s_j = numComm*3/4
    myPort, _ := strconv.Atoi(node.name)

    //Stage 1: Getting all the committees
    bins := node.implementGradecast(myComm)
    views_accept := make(map[int][]int)
    views_adopt := make(map[int][]int)
    for _, bin := range bins {
        accept, adopt := getViews(bin)
        for _, entry := range accept.list {
            views_accept[bin.num] = append(views_accept[bin.num], entry.port)
        }
        for _, entry := range adopt.list {
            views_adopt[bin.num] = append(views_adopt[bin.num], entry.port)
        }
    }
    fmt.Println(views_accept)
    fmt.Println("Got all views")


    //Stage 2: Agreeing on the composition of the smallest
    combos := getCombination(node.list, s_j)
    fmt.Println("Combinations")
    fmt.Println(combos)
    fmt.Println(myPort)
    for _, subcomm := range combos {
        fmt.Println(subcomm)
        participate := find(subcomm, myPort)
                fmt.Println("Running subprotocol")
        switch {
            case participate == 1 :
                node.subprotocol(views_adopt, views_accept, subcomm, numComm)
                time.Sleep(200*time.Millisecond) 
            default:
                time.Sleep(700*time.Millisecond) 
        }
    }
    tuples := node.getTuples()
    fmt.Println(tuples)
    //disqMap := make(map[int]int)
    //map_comm_tuple := make(map[int][]Tuple)
    //for _, tuple := range tuples {
    //    if find(tuple.s_j, tuple.from) == 1 && subset(tuple.s_j, views_accept[tuple.comm]) == 1 && (tuple.disq == 1) {
    //        disqMap[tuple.comm] += 1
    //    }
    //    if (tuple.disq == 0) && find(tuple.s_j, tuple.from) == 1 {
    //        map_comm_tuple[tuple.comm] = append(map_comm_tuple[tuple.comm], tuple)
    //    }
    //}
    //for key, value := range disqMap {
    //    if value >= numComm/2 {
    //        disqMap[key] = 1
    //    }
    //}
    //committees := make(map[int][]int)
    //for comm, values := range map_comm_tuple {
    //    comm_sj := make([]Comp, 0)
    //    for i := 0; i < len(values); i++ {
    //        newRow := Comp{comm: comm, s_j: values[i].s_j, D: []int{-1}}
    //        D := make([][]int, 0)
    //        D = append(D, values[i].comp)
    //        for j := i+1; j < len(values);  {
    //            switch {
    //            case (equal(values[i].s_j, values[j].s_j) == 1) :
    //                found := 0
    //                for index, comp := range D {
    //                    if equal(comp, values[j].comp) == 1 {
    //                        newRow.count[index] += 1
    //                        found = 1
    //                        break
    //                    }
    //                }
    //                if found == 0 {
    //                    D = append(D, values[j].comp)
    //                    newRow.count = append(newRow.count, 1)
    //                }
    //                first := values[:j]
    //                last := values[j+1:]
    //                values = append(first, last...)
    //            default: 
    //                j++
    //            }
    //        }
    //        for index, num := range newRow.count {
    //            if num >= numComm/2 {
    //                newRow.D = D[index]
    //            }
    //        }
    //        comm_sj = append(comm_sj, newRow)
    //    }
    //    committees[comm] = getFinalComp(comm_sj)
    //}
    //binNum, binMem := getLightestBin(committees)
    //fmt.Println(binNum, binMem)
}


func (node *Node) initConsensus() {
    node.initRound(1)
    time.Sleep(600*time.Millisecond) 
    //fmt.Printf("Hi, my port is %s. The set of values I have received are: \n", node.name)
    //val := node.getConsensus()
    //fmt.Println("The consensus is value ", val)
}

func (node *Node) getConsensus() int {
    count := make(map[int]int)
    for _, val := range node.setVal {
        count[val] += 1
    }
    maxVal := 0
    var maxKey int
    for key, _ := range count {
        if count[key] > maxVal {
            maxKey = key
            maxVal = count[key]
        }
    }
    return maxKey
}

func getRandComm(numComm int) int {
    return rand.Intn(numComm)
}

func initVal() int{
    return rand.Intn(2)
}


func Client(port string, nbrs []string, byz int, faults int) {
    node := Node{name: port, status: "Init", byz: byz, numFaults: faults}
    var err error
    port = ":" + port
    node.addr, err = net.ResolveTCPAddr("tcp", port)
    checkErr(err)
    tcpAddrNbr := make([]*net.TCPAddr, len(nbrs))
    for i, val := range nbrs {
        addr, err := net.ResolveTCPAddr("tcp", val)
        checkErr(err)
        tcpAddrNbr[i] = addr
    }
    node.nbr = tcpAddrNbr 
    //fmt.Printf("Hi my name is %s\n", node.name)
    rand.Seed(time.Now().UTC().UnixNano())
    node.val = initVal()
    for _, nbr := range node.nbr {
       node.list = append(node.list, nbr.Port)
    }
    node.list = append(node.list, node.addr.Port)
    node.numNodes = len(node.list)
    node.setVal = append(node.setVal, node.val)
    msg := "My (" + strconv.Itoa(node.addr.Port) + ") initial value is " + strconv.Itoa(node.val)
    node.c = make(chan string)
    fmt.Println(msg)
    node.listen()
    time.Sleep(200*time.Millisecond) 
    node.initConsensus()
}
