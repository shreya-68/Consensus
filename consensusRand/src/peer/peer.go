
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
    "sync"
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
    wg_s *sync.WaitGroup
    wg_v *sync.WaitGroup
    wg_t *sync.WaitGroup
    wg_g1 *sync.WaitGroup
    wg_g2 *sync.WaitGroup
    wg_g3 *sync.WaitGroup
    wg_final *sync.WaitGroup
}

var a = 2

type Bin struct {
    num int
    list  []BinEntry
}

type BinEntry struct {
    port int
    confidence int
}

type View struct {
    typ string
    subcomm []int
    commNum int
    myView []int
    disq int
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
    count int
}

func checkErr(err error) {
    if err != nil {
        fmt.Printf("Error: %s \n", err)
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
    conn.Close() 
}

func (node *Node) accept(listener *net.TCPListener, k *int) {
    for {
            conn, err := listener.Accept()
            if err != nil {
                continue
            }
            *k = *k + 1
            fmt.Println(*k)
            go node.handleClient(conn)
        }
}

func (node *Node) listen(k *int) {
    listener, err := net.ListenTCP("tcp", node.addr)
    checkErr(err)
    go node.accept(listener, k)
}

func (node *Node) openTCPconn(rcvr *net.TCPAddr) *net.TCPConn {
    conn, err := net.DialTCP("tcp", nil, rcvr)
    checkErr(err)
    return conn
}

func (node *Node) write(msg string, conn *net.TCPConn) {
        _, err := conn.Write([]byte(msg))
        checkErr(err)
}

func (node *Node) broadcast(msg string) {
    for _, nbr := range node.nbr {
        conn := node.openTCPconn(nbr)
        node.write(msg, conn)
        conn.Close()
    }
}

func getNumOfComm(n int) int {
    return int(float64(n)/math.Log(float64(n)))
}

func getLog(n int) int {
    return int(math.Log(float64(n)))
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
    time.Sleep(20*time.Millisecond) 
    node.wg_g1.Done()
    node.wg_g1.Wait()
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
    time.Sleep(20*time.Millisecond) 
    node.wg_g2.Done()
    node.wg_g2.Wait()
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
    time.Sleep(20*time.Millisecond) 
    node.wg_g3.Done()
    node.wg_g3.Wait()
    portBinStore := node.getPortBinStore()
    for key, value := range portStore {
            portBinStore[key] = append(portBinStore[key], value)
    }
    bins := node.getBins(portBinStore, myBin)
    return bins
}

func (node *Node) implementGradecast(myVal int) []Bin {
    binStore := node.gradecastStep1(myVal)
    portBin := node.gradecastStep2(binStore)
    bins := node.gradecastStep3(portBin, myVal)
    //fmt.Println(bins)
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
            conn.Close()
        }
    }
}

func (node *Node) getTuples() []Tuple{
    entries := make([]Tuple, 0)
    for {
        select {
            case entry := <-node.c :
                tuples := strings.Split(entry, "/")
                for _, tuple := range tuples {
                    msg := strings.Split(tuple, ",")
                    if len(msg) == 5 {
                        //msg[0] is FROM
                        //msg[1] is subcomm
                        //msg[2] is numcomm
                        //msg[3] is disq
                        //msg[4] is compos
                        from, _ := strconv.Atoi(msg[0])
                        s_j := make([]int, 0)
                        subcomm := strings.Split(msg[1], ".")
                        for _, value := range subcomm {
                            port, _ := strconv.Atoi(value)
                            s_j = append(s_j, port)
                        }
                        comm, _ := strconv.Atoi(msg[2])
                        disq, _ := strconv.Atoi(msg[3])
                        comp := make([]int, 0)
                        accepts := strings.Split(msg[4], ".")
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

func (node *Node) getIntersection() []View {
    allViews := make([]View, 0)
    for {
        select {
            case entry := <-node.c :
                comms := strings.Split(entry, ",")
                if len(comms) == 4 {
                    //comms[0] is TYPE
                    //comms[1] is from
                    //comms[2] is subcomm
                    //comms[3] is commNum:view.view
                    subcomm := make([]int, 0)
                    mems := strings.Split(comms[2], ".")
                    for _, mem := range mems {
                        x, _ := strconv.Atoi(mem)
                        subcomm = append(subcomm, x)
                    }
                    arr := strings.Split(comms[3], ":")
                    num, _ := strconv.Atoi(arr[0])
                    members := strings.Split(arr[1], ".")
                    found := 0
                    for i, eachView := range allViews {
                        if (equal(eachView.subcomm, subcomm) == 1) && (eachView.commNum == num) && (eachView.typ == comms[0]) {
                            found = 1
                            for _, member := range members {
                                port, err := strconv.Atoi(member)
                                checkErr(err)
                                if (find(eachView.myView, port) == 0) {
                                    allViews[i].myView = append(allViews[i].myView, port)
                                }
                            }
                            break
                        }
                    }
                    if found == 0 {
                        newView := View{typ: comms[0], subcomm:subcomm, commNum: num }
                        for _, member := range members {
                            port, _ := strconv.Atoi(member)
                            newView.myView = append(newView.myView, port)
                        }
                        allViews = append(allViews, newView)
                    }
                }
            default:
                return allViews
        }
    }
}

func (node *Node) sendComposition(typ string, acceptee map[int][]int, subcomm []int, numComm int){
    //ADOPT, from, subcom.sfd.,NUM:skjdf.afa
    submsg := typ + "," + node.name + ","
    for _, commMem := range subcomm {
        submsg += strconv.Itoa(commMem) + "."
    }
    submsg = strings.TrimRight(submsg, ".")
    submsg += ","
    for num, list := range acceptee {
        msg := submsg
        msg += strconv.Itoa(num) + ":"
        for _, accepted := range list {
            msg += strconv.Itoa(accepted) + "."
        }
        msg = strings.TrimRight(msg, ".")
        //fmt.Println(msg, subcomm)
        node.selectiveBroadcast(msg, subcomm)
    }
}

func (node *Node) subprotocol(adoptee map[int][]int, acceptee map[int][]int, subcomm []int, numComm int) {
    node.sendComposition("ADOPT", adoptee, subcomm, numComm)
    node.sendComposition("ACCEPT", acceptee, subcomm, numComm)
}

func (node *Node) decide(numComm int) []Tuple {
    allViews := node.getIntersection()
    //fmt.Println("PRINTING all views", allViews)
    allTuples := make([]Tuple, 0)
    allAccepts := make([]View, 0)
    allAdopts := make([]View, 0)
    for _, eachView := range allViews {
        if eachView.typ == "ADOPT" {
            allAdopts = append(allAdopts, eachView)
        }
        if eachView.typ == "ACCEPT" {
            allAccepts = append(allAccepts, eachView)
        }
    }
    //fmt.Println(allAdopts)
    //fmt.Println(allAccepts)
    log := getLog(len(node.list))
    for i, eachView := range allAdopts{
        if len(eachView.myView) > log {
            fmt.Println("DISQUALIFIED")
            allViews[i].disq = 1
            newTuple := Tuple{s_j: eachView.subcomm, comm: eachView.commNum, disq: 1}
            allTuples = append(allTuples, newTuple)
        }
    }
    for _, eachAccept := range allAccepts {
        found := 0
        for _, eachTuple := range allTuples {
            if (equal(eachTuple.s_j, eachAccept.subcomm) == 1) && (eachTuple.comm == eachAccept.commNum) {
                found = 1
                break
            }
        }
        if found == 0 {
            newTuple := Tuple{s_j: eachAccept.subcomm, comm: eachAccept.commNum, disq: 0, comp: eachAccept.myView}
            allTuples = append(allTuples, newTuple)
        }
    }
    return allTuples
}

//FROM,SJ.SJ.SJ,CI,
//SENDING TUPLES

func (node *Node) sendTuples(allTuples []Tuple) {
    msg := "" 
    for _, tuple := range allTuples {
        msg = node.name + ","
        for _, val := range tuple.s_j {
            msg += strconv.Itoa(val) + "."
        }
        msg = strings.TrimRight(msg, ".")
        msg += "," + strconv.Itoa(tuple.comm) + "," + strconv.Itoa(tuple.disq) + "," 
        found := 1
        for _, accept := range tuple.comp {
            found = 0
            msg += strconv.Itoa(accept) + "."
        }
        switch {
            case found == 0:
                msg = strings.TrimRight(msg, ".")
            case found == 1:
                msg = strings.TrimRight(msg, ",")
        }
        msg += "/"
    }
    msg = strings.TrimRight(msg, "/")
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

func (node *Node) check() {
    for {
        select {
            case left := <-node.c:
                fmt.Println("STILL LEFT TO READ", node.name, left)
            default: return
        }
    }
}

func (node *Node) getHighest() int {
    high := -1
    for {
        select {
            case entry := <-node.c:
                num, _ := strconv.Atoi(entry)
                if high < num || high == -1 {
                    high = num
                }
            default: return high
        }
    }
}

func (node *Node) initConsensus() {
    var numComm int
    numComm = getNumOfComm(node.numNodes)
    fmt.Printf("the num of committees for %d nodes are %d\n", node.numNodes, numComm)
    myComm := getRandComm(numComm)
    fmt.Printf("I am %s. Setval: %d\n", node.name, myComm)
    var s_j int 
    s_j = numComm*3/4
    //if len(node.list) > 16 {
    //    s_j = numComm*3/8
    //}
    fmt.Println("SJ:", s_j, numComm)
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
    //fmt.Println(views_accept)
    //fmt.Println("Got all views")
    node.wg_s.Done()
    node.wg_s.Wait()


    //Stage 2: Agreeing on the composition of the smallest
    combos := getCombination(node.list, s_j)
    fmt.Println(len(combos))
    fmt.Println("CAME here")
    for _, subcomm := range combos {
        if find(subcomm, myPort) == 1 {
            node.subprotocol(views_adopt, views_accept, subcomm, numComm)
        }
        //fmt.Println("INDEX:", i)
    }
    node.wg_v.Done()
    node.wg_v.Wait()

    tuples := node.decide(numComm)
    //fmt.Println("PRINTING TUPLES", tuples)
    node.sendTuples(tuples)
    time.Sleep(20*time.Millisecond) 
    node.wg_t.Done()
    node.wg_t.Wait()
    tuples = node.getTuples()
    disqMap := make(map[int]int)
    map_comm_tuple := make(map[int][]Tuple)
    for _, tuple := range tuples {
        if find(tuple.s_j, tuple.from) == 1 && subset(tuple.s_j, views_accept[tuple.comm]) == 1 && (tuple.disq == 1) {
            disqMap[tuple.comm] += 1
        }
        if (tuple.disq == 0) && find(tuple.s_j, tuple.from) == 1 {
            map_comm_tuple[tuple.comm] = append(map_comm_tuple[tuple.comm], tuple)
        }
    }
    //fmt.Println("PRINTING MAPCOMM", map_comm_tuple)
    log := getLog(len(node.list))
    for key, value := range disqMap {
        if value >= log/2 {
            disqMap[key] = 1
        }
    }
    committees := make(map[int][]int)
    for comm, values := range map_comm_tuple {
        comm_sj := make([]Comp, 0)
        for _, eachVal := range values {
            found := 0
            for index, eachComp := range comm_sj {
                if (equal(eachComp.s_j, eachVal.s_j) == 1) && (equal(eachComp.D, eachVal.comp) == 1) {
                    comm_sj[index].count += 1
                    found = 1
                    break
                }
            }
            if found == 0 {
                newRow := Comp{comm: comm, s_j: eachVal.s_j, D: eachVal.comp, count: 1}
                comm_sj = append(comm_sj, newRow)
            }
        }
        //fmt.Println("PRINTING COMM_SJ", comm_sj)
        final_comm_sj := make([]Comp, 0)
        for _, eachComp := range comm_sj {
            if eachComp.count >= log/2 {
                final_comm_sj = append(final_comm_sj, eachComp)
            }
        }
        committees[comm] = getFinalComp(final_comm_sj)
    }
    //fmt.Println("PRINTING COMMITTEES", committees)
    binNum, binMem := getLightestBin(committees)
    fmt.Println(binNum, binMem)
    get := 0
    if find(binMem, myPort) == 1 {
        num := rand.Intn(100)
        msg := strconv.Itoa(num)
        fmt.Println("My rand is", node.name, msg)
        node.selectiveBroadcast(msg, binMem)
        time.Sleep(200*time.Millisecond) 
        high := node.getHighest()
        fmt.Println(high)
        if high == num || num > high {
            msg := strconv.Itoa(node.val)
            node.broadcast(msg)
            get = 1
        }
    }
    node.wg_final.Done()
    node.wg_final.Wait()
    var consensus int
    switch {
        case get == 1:
            consensus = node.val
        default:
            consensus, _ = strconv.Atoi(<-node.c)
    }
    fmt.Println("My consensus value:", node.name, consensus)

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


func Client(port string, nbrs []string, byz int, faults int, wg_stage1 *sync.WaitGroup, wg_views *sync.WaitGroup, wg_tuples *sync.WaitGroup, wg_g1 *sync.WaitGroup, wg_g2 *sync.WaitGroup, wg_g3 *sync.WaitGroup, wg_final *sync.WaitGroup, wg *sync.WaitGroup, k *int) {
    node := Node{name: port, status: "Init", byz: byz, numFaults: faults, wg_s: wg_stage1, wg_v: wg_views, wg_t: wg_tuples, wg_g1: wg_g1, wg_g2: wg_g2, wg_g3: wg_g3, wg_final: wg_final}
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
    rand.Seed(time.Now().UTC().UnixNano())
    node.val = initVal()
    for _, nbr := range node.nbr {
       node.list = append(node.list, nbr.Port)
    }
    node.list = append(node.list, node.addr.Port)
    node.numNodes = len(node.list)
    node.setVal = append(node.setVal, node.val)
    msg := "My (" + strconv.Itoa(node.addr.Port) + ") initial value is " + strconv.Itoa(node.val)
    node.c = make(chan string, 500000)
    fmt.Println(msg)
    node.listen(k)
    time.Sleep(200*time.Millisecond) 
    node.initConsensus()
    wg.Done()
}
