
package peer

import (
    "fmt"
    "time"
    "os"
    "net"
    "strconv"
    "math/rand"
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
    root    *EIGNode
    c       chan string
    wg []*sync.WaitGroup
}

func checkErr(err error) {
    if err != nil {
        fmt.Printf("Fatal error: %s \n", err)
        os.Exit(1)
    }
}

func checkWriteErr(err error) {
    if err != nil {
        fmt.Printf("Fatal write error: %s \n", err)
        os.Exit(1)
    }
}

type EIGNode struct {
    level int
    path  []int
    val   int
    child []*EIGNode
    newval int
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

func (node *Node) accept(listener *net.TCPListener, k *int) {
    for {
            conn, err := listener.Accept()
            if err != nil {
                continue
            }
            *k = *k + 1
            fmt.Println(node.name, *k)
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
        //fmt.Printf("Writing %s\n", msg)
        n, err := conn.Write([]byte(msg))
        if n == len(msg) {
            checkWriteErr(err)
        }
}

func (node *Node) broadcast(msg string) {
    for _, nbr := range node.nbr {
        conn := node.openTCPconn(nbr)
        node.write(msg, conn)
        conn.Close()
        
    }
}

func (node *Node) setChildVals(l *int) {
    for {
        select {
            case entry := <-node.c:
                eachNode := strings.Split(entry, ",")
                *l = *l + len(eachNode)
                for _, each := range eachNode {
                    pathVal := strings.Split(each, ":")
                    if len(pathVal) == 2 {
                        pathStr := strings.Split(pathVal[0], ".")
                        path := make([]int, 0)
                        for _, eachInt := range pathStr {
                            x, _ := strconv.Atoi(eachInt)
                            path = append(path, x)
                        }
                        value, _  := strconv.Atoi(pathVal[1])
                        depth := len(path)
                        curr := node.root
                        for i := 0; i < depth; i++ {
                            for _, child := range curr.child {
                                if len(child.path) >= i+1 {
                                    if child.path[i] == path[i] {
                                        curr = child
                                        break
                                    }
                                }
                            }
                        }
                        curr.val = value
                    }
                }
            default: return
        }
    }
}

func (node *Node) createChildren(eigNode *EIGNode) {
    list := node.list
    for _, i := range list {
        found := 0
        for _, j := range eigNode.path {
            if i == j {
                found = 1
                break
            }
        }
        if found == 0 {
            newChild := &EIGNode{level:eigNode.level+1}
            newChild.path = make([]int, eigNode.level)
            copy(newChild.path, eigNode.path)
            newChild.path = append(newChild.path, i)
            if i == node.addr.Port {
                newChild.val = eigNode.val
            }
            eigNode.child = append(eigNode.child, newChild)
        }
    }
}

func (node *Node) traverseEIG(eigNode *EIGNode, depth int) []*EIGNode {
    if depth == 0 {
        node.createChildren(eigNode)
        if node.root == eigNode {
            return []*EIGNode{eigNode}
        }
        found := 0
        for _, j := range eigNode.path {
            if node.addr.Port == j {
                found = 1
                break
            }
        }
        //fmt.Println(eigNode)
        if found == 0 {
            //fmt.Println("To send: ", eigNode.val)
            return []*EIGNode{eigNode}
        }
        return []*EIGNode{}
    }
    leaves := []*EIGNode{}
    for _, child := range eigNode.child {
        subLeaf := node.traverseEIG(child, depth-1)
        leaves = append(leaves, subLeaf...)
    }
    return leaves
}

//Message format ---> int1.int2.int3.currRoot:val,int1.int2.int3.currRoot:val, ...,
func (node *Node) initRound(roundNum int, t *int) {
    sendThis := node.traverseEIG(node.root, roundNum)

    msg := ""
    for _, each := range sendThis {
        for _, pathInt := range each.path {
            msg += strconv.Itoa(pathInt) + "."
        }
        val := strconv.Itoa(each.val)
        if node.byz == 1{
            val = strconv.Itoa(rand.Intn(2))
        }
        msg += node.name + ":" + val + ","
    }
    msg = strings.TrimRight(msg, ",")
    //msg = strconv.Itoa(node.val)
    *t = *t + 1
    node.broadcast(msg)
}

func (node *Node) getConsensus(level int) {
    curr := node.root
    stack := []*EIGNode{}
    queue := []*EIGNode{curr}
    for len(queue) > 0{
        x := queue[0] 
        queue = queue[1:]
        if x.level <= level {
            for _, child := range x.child {
                queue = append(queue, child)
            }
        }
        stack = append(stack, x)
    }
    for len(stack) > 0 {
        x := stack[len(stack)-1]
        stack = stack[:len(stack)-1]
        switch {
            case x.level == level+1: x.newval = x.val
            default: 
                count := make(map[int]int)
                for _, child := range x.child {
                    count[child.val] += 1
                }
                maxVal := 0
                var maxKey int
                for key, _ := range count {
                    if count[key] > maxVal {
                        maxKey = key
                        maxVal = count[key]
                    }
                }
                x.newval = maxKey
        }
    }
}

func (node *Node) initConsensus(faults int, t *int, l *int){
    defer func() {
        if r := recover(); r != nil {
            fmt.Println("Recovered", r)
        }
    }()
    for i := 0; i <= faults ; i++ {
        node.initRound(i, t)
        node.wg[i].Done()
        node.wg[i].Wait()
        node.setChildVals(l)
        fmt.Println("This round complete", i)
    }
    node.getConsensus(faults)
    fmt.Printf("My %s final value is %d \n", node.name, node.root.newval)
}

func Client(port string, nbrs []string, byz int, faults int, wg []*sync.WaitGroup, wg_done *sync.WaitGroup, k *int, t *int) {
    node := Node{name: port, status: "Init", byz: byz, wg: wg}
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
    node.setVal = append(node.setVal, node.val)
    msg := "My (" + strconv.Itoa(node.addr.Port) + ") initial value is " + strconv.Itoa(node.val)
    fmt.Println(msg)
    node.root = &EIGNode{level: 0, val: node.val}
    for _, nbr := range node.nbr {
       node.list = append(node.list, nbr.Port)
    }
    node.list = append(node.list, node.addr.Port)
    node.c = make(chan string, 10000000)
    node.listen(k)
    time.Sleep(200*time.Millisecond) 
    var l int
    node.initConsensus(faults, t, &l)
    fmt.Println(l)
    *k = *k + l
    wg_done.Done()
}
