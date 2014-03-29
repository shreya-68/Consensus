

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
    val     string 
    setVal  []string
    list    []int
    byz     int
    candStrs []string
    c       chan string
}

var c = 2

type PullInfo struct {
    typ string
    from int
    origFrom int
    gstr string
    randstr string
    to int
}

type Counter struct {
    gstr string
    x int
    count int
}

func checkErr(err error) {
    if err != nil {
        fmt.Printf("Fatal error: %s \n", err)
        os.Exit(1)
    }
}

func initVal() int{
    return rand.Intn(2)
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

func (node *Node) getPushReqs() map[string][]int {
    portCand := make(map[string][]int)
    for {
        select {
            case entry := <-node.c :
                arr := strings.Split(entry, ":")
                port, err := strconv.Atoi(arr[0])
                checkErr(err)
                cand := arr[1]
                checkErr(err)
                portCand[cand] = append(portCand[cand], port)
            default:
                return portCand
        }
    }
}

//func (node *Node) getPullReqs() []PullInfo {
//    pulls := make([]PullInfo, 0)
//    for {
//        select {
//            case entry := <-node.c :
//                arr := strings.Split(entry, ":")
//                port, err := strconv.Atoi(arr[1])
//                checkErr(err)
//                if (arr[0] == "PULL") || (arr[0] == "POLL") {
//                    newPull := PullInfo{typ: arr[0], from: port, gstr: arr[2], randstr: arr[3]}
//                }
//                if arr[0] == "POLL1" {
//                    orig, err := strconv.Atoi(arr[2])
//                    to, err := strconv.Atoi(arr[5])
//                    newPull := PullInfo{typ: arr[0], from: port, origFrom: orig, gstr: arr[3], randstr: arr[4], to: to}
//                }
//                pulls = append(pulls, newPull)
//            default:
//                return pulls
//        }
//    }
//}

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
        conn.Close()
        
    }
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

//Message format ---> int1.int2.int3.currRoot:val,int1.int2.int3.currRoot:val, ...,
func (node *Node) pushPhase() {
    msg := node.name + ":" + node.val
    node.broadcast(msg)
    time.Sleep(200*time.Millisecond) 
    recvd := node.getPushReqs()
    for s_x, nodes := range recvd {
        quorum := getPushQuorum(s_x, node.addr.Port, node.list)
        fmt.Println(quorum)
        count := 0
        for _, x := range nodes {
            if find(quorum, x) == 1 {
                count++
            }
        }
        if count > len(quorum)/2 {
            if s_x != node.setVal[0] {
                node.setVal = append(node.setVal, s_x)
            }
        }
    }
    fmt.Println(node.setVal)
}

func getGstring(n int) string {
    len := c*getLog(n)
    gstring := ""
    for i := 1; i <= len; i++ {
        append := rand.Intn(2)
        gstring += strconv.Itoa(append) 
    }
    return gstring
}

func (node *Node) sendPullReqs() {
    for _, s_x := range node.setVal {
        rsx := getGstring(len(node.list))
        pollList := getPollList(rsx, node.addr.Port, node.list)
        pullQuorum := getPullQuorum(s_x, node.addr.Port, node.list)
        msgPoll := "POLL:" + node.name + ":" + s_x + ":" + rsx
        msgPull := "PULL:" + node.name + ":" + s_x + ":" + rsx
        node.selectiveBroadcast(msgPoll, pollList)
        node.selectiveBroadcast(msgPull, pullQuorum)
    }
}

func (node *Node) routingPullReqs(c chan PullInfo) {
    //pulls := node.getPullReqs()
    counter := make([]Counter, 0)
    for {
        select {
            case entry := <-node.c :
                arr := strings.Split(entry, ":")
                port, err := strconv.Atoi(arr[1])
                checkErr(err)
                var pullReq PullInfo
                if (arr[0] == "PULL") || (arr[0] == "POLL") {
                    pullReq = PullInfo{typ: arr[0], from: port, origFrom: port, gstr: arr[2], randstr: arr[3]}
                }
                if arr[0] == "POLL1" {
                    orig, err := strconv.Atoi(arr[2])
                    checkErr(err)
                    to, err := strconv.Atoi(arr[5])
                    pullReq = PullInfo{typ: arr[0], from: port, origFrom: orig, gstr: arr[3], randstr: arr[4], to: to}
                }
                if arr[0] == "POLL2" {
                    orig, err := strconv.Atoi(arr[2])
                    checkErr(err)
                    pullReq = PullInfo{typ: arr[0], from: port, origFrom: orig, gstr: arr[3], randstr: arr[4]}
                }
                switch {
                    case pullReq.typ == "PULL" :
                        pullQuorum := getPullQuorum(pullReq.gstr, pullReq.from, node.list)
                        if (pullReq.gstr == node.val) && (find(pullQuorum, node.addr.Port) == 1) {
                            pollList := getPollList(pullReq.randstr, pullReq.from, node.list)
                            for _, w := range pollList {
                                pullQuoW := getPullQuorum(pullReq.gstr, w, node.list)
                                msgPoll := "POLL1:" + node.name + ":" + strconv.Itoa(pullReq.from) + ":" + pullReq.gstr + ":" + pullReq.randstr + ":" + strconv.Itoa(w)
                                node.selectiveBroadcast(msgPoll, pullQuoW)
                            }
                        }
                    case pullReq.typ == "POLL1" :
                        pullQuoW := getPullQuorum(pullReq.gstr, pullReq.to, node.list)
                        pullQuoX := getPullQuorum(pullReq.gstr, pullReq.origFrom, node.list)
                        pollList := getPollList(pullReq.randstr, pullReq.origFrom, node.list)
                        if (find(pullQuoX, pullReq.from) == 1) && (find(pullQuoW, node.addr.Port) == 1) && (find(pollList, pullReq.to) == 1) {
                            found := 0
                            var index int
                            for i, count := range counter {
                                if (count.gstr == pullReq.gstr) && (count.x == pullReq. origFrom) {
                                    count.count += 1
                                    found = 1
                                    index = i
                                    break
                                }
                            }
                            if found == 0 {
                                newCount := Counter{gstr: pullReq.gstr, x: pullReq.origFrom, count: 1}
                                counter = append(counter, newCount)
                                index = len(counter)-1
                            }
                            if counter[index].count > len(pullQuoX)/2 {
                                msgPoll := "POLL2:" + node.name + ":" + strconv.Itoa(pullReq.origFrom) + ":" + pullReq.gstr + ":" + pullReq.randstr
                                node.selectiveBroadcast(msgPoll, []int{pullReq.to})
                                counter[index].count = -10000
                            }

                        }
                    default: c <- pullReq
                }
        }
    }
}

func (node *Node) answeringPullReqs(c chan PullInfo) {
    counts := make(map[string]int)
    counter := make([]Counter, 0)
    polled := make([]Counter, 0)
    for {
        select {
            case pullReq := <-c :
                switch {
                    case pullReq.typ == "POLL2": 
                        pullQuo := getPullQuorum(pullReq.gstr, node.addr.Port, node.list)
                        pollList := getPollList(pullReq.randstr, pullReq.origFrom, node.list)
                        if (pullReq.gstr == node.val) && (find(pollList, node.addr.Port) == 1) && (find(pullQuo, pullReq.from) == 1) {
                            found := 0
                            var index int
                            for i, count := range counter {
                                if (count.gstr == pullReq.gstr) && (count.x == pullReq. origFrom) {
                                    count.count += 1
                                    found = 1
                                    index = i
                                    break
                                }
                            }
                            if found == 0 {
                                newCount := Counter{gstr: pullReq.gstr, x: pullReq.origFrom, count: 1}
                                counter = append(counter, newCount)
                                index = len(counter)-1
                            }
                            found = 0
                            for _, pair := range polled {
                                if (pair.gstr == pullReq.gstr) && (pair.x == pullReq. origFrom) {
                                    found = 1
                                    break
                                }
                            }
                            if (counter[index].count > len(pullQuo)/2) && (found == 1) {
                                counts[pullReq.gstr] += 1
                                msgPoll := "ANSWER:" + node.name + ":" + pullReq.gstr
                                node.selectiveBroadcast(msgPoll, []int{pullReq.origFrom})
                                counter[index].count = -10000
                            }

                        }
                    case pullReq.typ == "POLL" : 
                        pollList := getPollList(pullReq.randstr, pullReq.origFrom, node.list)
                        pullQuo := getPullQuorum(pullReq.gstr, node.addr.Port, node.list)
                        if find(pollList, node.addr.Port) == 1 {
                            found := 0
                            for _, pair := range polled {
                                if (pair.gstr == pullReq.gstr) && (pair.x == pullReq. origFrom) {
                                    found = 1
                                    break
                                }
                            }
                            if found == 0 {
                                newPoll := Counter{gstr: pullReq.gstr, x: pullReq.origFrom}
                                polled = append(polled, newPoll)
                            }
                            found = 0
                            var index int
                            for i, count := range counter {
                                if (count.gstr == pullReq.gstr) && (count.x == pullReq. origFrom) {
                                    found = 1
                                    index = i
                                    break
                                }
                            }
                            if found == 1 {
                                if (counter[index].count > len(pullQuo)/2) {
                                    counts[pullReq.gstr] += 1
                                    msgPoll := "ANSWER:" + node.name + ":" + pullReq.gstr
                                    node.selectiveBroadcast(msgPoll, []int{pullReq.origFrom})
                                    counter[index].count = -10000

                                }
                            }


                        }
                        
                }
        }
    }
                
}

func (node *Node) pullPhase() {
    node.sendPullReqs()
    c := make(chan PullInfo)
    go node.routingPullReqs(c)
    node.answeringPullReqs(c)
}

func (node *Node) getConsensus() {
    //count := make(map[int]int)
    //for _, val := range node.setVal {
    //    count[val] += 1
    //    //fmt.Printf("I am %s. Setval: %d\n", node.name, val)
    //}
    //maxVal := 0
    //var maxKey int
    //for key, _ := range count {
    //    if count[key] > maxVal {
    //        maxKey = key
    //        maxVal = count[key]
    //    }
    //}
    //fmt.Println("The consensus is value ", maxKey)
}

func (node *Node) initConsensus(faults int){
    node.pushPhase()
    time.Sleep(300*time.Millisecond) 
    node.pullPhase()
    //node.getConsensus()

}

func getLog(n int) int {
    return int(float64(n)/math.Log(float64(n)))
}

func getDecimal(num string) int {
    mult := 1
    sum := 0
    for i := len(num) - 1; i >= 0 ; i-- {
        last, _ := strconv.Atoi(string(num[i]))
        sum += mult*last
        mult *= 2
    }
    return sum
}

func getPushQuorum(cand string, n int, list []int) []int {
    size := getLog(len(list))
    dec := getDecimal(cand)
    quorum := make([]int, 0)
    for i := 0; i < size; i++ {
        num := (((n - 9000) + 1 + dec + i) % len(list)) + 9000
        quorum = append(quorum, num)
    }
    return quorum
}

func getPullQuorum(cand string, n int, list []int) []int {
    size := getLog(len(list))
    dec := getDecimal(cand)
    quorum := make([]int, 0)
    for i := 0; i < size; i++ {
        num := (((n - 9000) + 1 + dec + i + 3) % len(list)) + 9000
        quorum = append(quorum, num)
    }
    return quorum
}

func getPollList(cand string, n int, list []int) []int {
    size := getLog(len(list))
    dec := getDecimal(cand)
    quorum := make([]int, 0)
    for i := 0; i < size; i++ {
        num := (((n - 9000) + 1 + dec + i + 2) % len(list)) + 9000
        quorum = append(quorum, num)
    }
    return quorum
}


func Client(port string, nbrs []string, byz int, faults int, gstring string, candStrs []string) {
    node := Node{name: port, status: "Init", byz: byz, val: gstring, candStrs: candStrs}
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
    node.setVal = append(node.setVal, node.val)
    msg := "My (" + strconv.Itoa(node.addr.Port) + ") initial value is " + node.val
    fmt.Println(msg)
    for _, nbr := range node.nbr {
       node.list = append(node.list, nbr.Port)
    }
    node.list = append(node.list, node.addr.Port)
    node.c = make(chan string)
    node.listen()
    time.Sleep(400*time.Millisecond) 
    node.initConsensus(faults)
    //fmt.Printf("Hi, my port is %s. The set of values I have received are: \n", node.name)
    //node.getConsensus()
}
