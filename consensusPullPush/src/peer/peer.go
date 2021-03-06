

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
    val     string 
    setVal  []string
    list    []int
    byz     int
    candStrs []string
    c       chan string
    pullChan       chan PullInfo
    ansChan       chan PullInfo
    wg_push *sync.WaitGroup
    wg_pull *sync.WaitGroup
    wg_ans *sync.WaitGroup
    wg *sync.WaitGroup
}

var c = 1

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
    var buf [1024]byte
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

func (node *Node) accept(listener *net.TCPListener, k *int) {
    for {
            conn, err := listener.Accept()
            if err != nil {
                continue
            }
            *k = *k + 1
            if node.name == "9000" {
                fmt.Println(*k)
            }
            go node.handleClient(conn)
            //node.connections[0] = &net.TCPConn(conn)
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
            conn.Close()
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
    //var val string
    //switch {
    //    case node.byz == 1 :
    //        val = getGstring(len(node.list))
    //    default:
    //        val = node.val
    //}
    //msg := node.name + ":" + val
    msg := node.name + ":" + node.val
    node.broadcast(msg)
    node.wg_push.Done()
    node.wg_push.Wait()
    recvd := node.getPushReqs()
    for s_x, nodes := range recvd {
        quorum := getPushQuorum(s_x, node.addr.Port, node.list)
        //fmt.Println(quorum)
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

func (node *Node) sendPullReqs(t *int) map[string][]int {
    polls := make(map[string][]int)
    for _, s_x := range node.setVal {
        rsx := getGstring(len(node.list))
        pollList := getPollList(rsx, node.addr.Port, node.list)
        pullQuorum := getPullQuorum(s_x, node.addr.Port, node.list)
        msgPoll := "POLL:" + node.name + ":" + s_x + ":" + rsx
        msgPull := "PULL:" + node.name + ":" + s_x + ":" + rsx
        node.selectiveBroadcast(msgPoll, pollList)
        node.selectiveBroadcast(msgPull, pullQuorum)
        *t = *t + 2
        polls[s_x] = pollList
    }
    return polls
}

func (node *Node) routingPullReqs() {
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
                if (arr[0] == "ANSWER") {
                    pullReq = PullInfo{typ: arr[0], from: port, gstr: arr[2]}
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
                //fmt.Println(pullReq)
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
                                    counter[i].count += 1
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
                            //fmt.Println("PRINTING COUN AND LEN OF QUORUM")
                            //fmt.Printf("I am %d, GSTRING IS %s, X is %d\n", node.addr.Port, pullReq.gstr, pullReq.origFrom)
                            //fmt.Printf("count %d, len/2 %d\n", counter[index].count, len(pullQuoX)/2)
                            if counter[index].count > (len(pullQuoX)/2 - 1) {
                                //if 1 == 1 {
                                msgPoll := "POLL2:" + node.name + ":" + strconv.Itoa(pullReq.origFrom) + ":" + pullReq.gstr + ":" + pullReq.randstr
                                node.selectiveBroadcast(msgPoll, []int{pullReq.to})
                                counter[index].count = -10000
                            }

                        }
                    case pullReq.typ == "ANSWER" : node.ansChan <- pullReq
                    default: node.pullChan <- pullReq
                }
        }
    }
}

func (node *Node) answeringPullReqs() {
    counts := make(map[string]int)
    counter := make([]Counter, 0)
    polled := make([]Counter, 0)
    for {
        select {
            case pullReq := <- node.pullChan :
                switch {
                    case pullReq.typ == "POLL2": 
                        pullQuo := getPullQuorum(pullReq.gstr, node.addr.Port, node.list)
                        pollList := getPollList(pullReq.randstr, pullReq.origFrom, node.list)
                        if (pullReq.gstr == node.val) && (find(pollList, node.addr.Port) == 1) && (find(pullQuo, pullReq.from) == 1) {
                            found := 0
                            var index int
                            for i, count := range counter {
                                if (count.gstr == pullReq.gstr) && (count.x == pullReq. origFrom) {
                                    counter[i].count += 1
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
                            if (counter[index].count > len(pullQuo)/2 - 1) && (found == 1) {
                                counts[pullReq.gstr] += 1
                                msgPoll := "ANSWER:" + node.name + ":" + pullReq.gstr
                                fmt.Println("Sending Answer")
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
                                if (counter[index].count > len(pullQuo)/2 - 1) {
                                    counts[pullReq.gstr] += 1
                                    fmt.Println("Sending Answer")
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

func (node *Node) processAnswers(polls map[string][]int) string {
    counter := make(map[string]int)
    timeout := make(chan bool, 1)
    go func() {
            time.Sleep(14 * time.Second)
                timeout <- true
    }()
    for {
        select {
            case answer := <- node.ansChan :
                fmt.Println(node.name, "Received ANSWER")
                pollList, ok := polls[answer.gstr]
                if ok {
                    counter[answer.gstr] += 1
                    if counter[answer.gstr] > len(pollList)/2 -1 {
                        return answer.gstr
                    }
                }
            case <-timeout:
                return "-2"
                }
        }
}

func (node *Node) pullPhase(t *int) string {
    polls := node.sendPullReqs(t)
    fmt.Println("Sent Pull Reqs for:")
    node.wg_pull.Done()
    node.wg_pull.Wait()
    //fmt.Println(polls)
    go node.routingPullReqs()
    time.Sleep(10*time.Second) 
    go node.answeringPullReqs()
    *t = *t + 2
    time.Sleep(10*time.Second) 
    return node.processAnswers(polls)
}


func (node *Node) initConsensus(faults int, t *int){
    fmt.Println("Starting Push phase.")
    node.pushPhase()
    *t = *t + 1
    fmt.Println("Starting Pull phase.")
    conVal :=  node.pullPhase(t)
    fmt.Printf("My %s consensus value is %s\n", node.name, conVal)
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
    j := 0
    for i := 0; i < size + j; i++ {
        num := (((n - 9000) + 1 + dec + i) % len(list)) + 9000
        switch {
            case num == n:
                j++
            default:
                quorum = append(quorum, num)
        }
    }
    return quorum
}

func getPullQuorum(cand string, n int, list []int) []int {
    size := getLog(len(list))
    dec := getDecimal(cand)
    quorum := make([]int, 0)
    j := 0
    for i := 0; i < size + j; i++ {
        num := (((n - 9000) + 1 + dec + i + 3) % len(list)) + 9000
        switch {
            case num == n:
                j++
            default:
                quorum = append(quorum, num)
        }
    }
    return quorum
}

func getPollList(cand string, n int, list []int) []int {
    size := getLog(len(list))
    dec := getDecimal(cand)
    quorum := make([]int, 0)
    j := 0
    for i := 0; i < size + j; i++ {
        num := (((n - 9000) + 1 + dec + i + 2) % len(list)) + 9000
        switch {
            case num == n:
                j++
            default:
                quorum = append(quorum, num)
        }
    }
    return quorum
}


func Client(port string, nbrs []string, byz int, faults int, gstring string, candStrs []string, wg_push *sync.WaitGroup, wg_ans *sync.WaitGroup, wg_pull *sync.WaitGroup, wg *sync.WaitGroup, k *int, t *int) {
    node := Node{name: port, status: "Init", byz: byz, val: gstring, candStrs: candStrs, wg_push: wg_push, wg_ans: wg_ans, wg_pull: wg_pull, wg: wg}
    var err error
    port = ":" + port
    node.pullChan = make(chan PullInfo, 1000000)
    node.ansChan = make(chan PullInfo, 1000000)
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
    for _, nbr := range node.nbr {
       node.list = append(node.list, nbr.Port)
    }
    node.list = append(node.list, node.addr.Port)
    if node.byz == 1 {
            //node.val = "11111111"
            node.val = getGstring(len(node.list))
    }
    msg := "My (" + strconv.Itoa(node.addr.Port) + ") initial value is " + node.val
    fmt.Println(msg)
    node.c = make(chan string, 1000000)
    node.listen(k)
    time.Sleep(400*time.Millisecond) 
    node.initConsensus(faults, t)
    wg.Done()
}
