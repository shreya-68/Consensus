
#include <assert.h>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <ctime>
#include <cstdio>
#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <utility>
#include <unistd.h>

using namespace std;

# include "mpi.h"

// stores the value log(size)
int quorum_size;
// Variables to store it's own process ID "rank" and the total number of
// nodes "size"
int rank;
int size;
// variable to store string of bits
int myvalue = 0;

int has_decided = 0;
int num_failures;

int default_value = 1;



// TAGS: 1 - Push message
//       2 - pull query
//       3 - Fw1
//       4 - Fw2
//       5 - Poll
//       6 - Answer

class Node {
 public:
    int level;
    vector<Node*> children;
    int* label;
    Node* parent;
    int val;
    int newval;
    int cval;
    int count;

    int* getParentLabel() {
        return parent->label;
    }
};

Node* root;

set<int> byz_set;
map<int, set<int> > my_suspect_set;
map<int, map<int, set<int> > > echo_suspects;

int **temp_suspect_set;

// stores the gstring - the value agreed upon by majority of nodes
int setMyValue() {
    sleep(rank*2);
    srand(time(NULL));
    myvalue = (rand() % 4) + 1;
    return 1;
}

int initFirstRound() {
    MPI_Request req;
    for(int i = 0; i < size; i++) {
        if(i != rank) {
          MPI_Isend(&myvalue, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &req);
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    int recd_msg = -1;
    for(int i = 0; i < size; i++) {
        Node* newChild = new Node();
        newChild->level  = 1;
        newChild->label = new int[1];
        newChild->parent = root;
        *(newChild->label) = i;
        root->children.push_back(newChild);
        MPI_Irecv(&recd_msg, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &req);
        if (i == rank)
            newChild->val = myvalue;
        else
            newChild->val = recd_msg;
    }
    return 1;
}

int initSecondRound() {
    MPI_Request req;
    int *message = new int[size];
    for(vector<Node*>::iterator it = root->children.begin();
            it != root->children.end(); it++) {
        message[*((*it)->label)] = (*it)->val;
    }

    for(int i = 0; i < size; i++) {
        if(i != rank) {
          MPI_Isend(message, size, MPI_INT, i, 2, MPI_COMM_WORLD, &req);
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);


    int *recd_msg;
    for(int i = 0; i < size; i++) {
        if(i != rank) {
            recd_msg = new int[size];
            for(int j = 0; j < size; j++) {
                recd_msg[j] = -1;
            }
            MPI_Irecv(recd_msg, size, MPI_INT, i, 2, MPI_COMM_WORLD, &req);
        } else {
            recd_msg = message;
        }
        int next_recd_val = 0;
        for(vector<Node*>::iterator it = root->children.begin();
                it != root->children.end() && next_recd_val < size;
                it++, next_recd_val++) {
            if (*((*it)->label) != i) {
                assert(*((*it)->label) == next_recd_val);
                Node* newChild = new Node();
                newChild->level  = 2;
                newChild->label = new int[2];
                *(newChild->label) = *((*it)->label);
                *(newChild->label + 1) = i; 
                newChild->parent = *it;
                (*it)->children.push_back(newChild);
                newChild->cval = recd_msg[next_recd_val];
            }
        }
    }
    int count = 0;
    for(vector<Node*>::iterator it_parent = root->children.begin();
            it_parent != root->children.end(); it_parent++) {
        count = 0;
        for(vector<Node*>::iterator it_child = (*it_parent)->children.begin();
                it_child != (*it_parent)->children.end(); it_child++) {
            if ((*it_parent)->val == (*it_child)->cval) {
                count++;
            }
        }
        if (count < size - num_failures) {
            byz_set.insert(*((*it_parent)->label));
        }
    }
    return 1;
}

int addToSuspects(int reporter, int suspect_set[]) {
    for(int i = 0; i < size; i++) {
        if(suspect_set[i] == 1) {
            my_suspect_set[reporter].insert(i);
        }
    }
    return 1;
}

int addToEchoesCheck(int reporter, int reportee, int suspect_set[]) {
    int equality = 1;
    for(int i = 0; i < size; i++) {
        if(suspect_set[i] != temp_suspect_set[reportee][i])
            equality = 0;
        if(suspect_set[i] == 1) {
            echo_suspects[reporter][reportee].insert(i);
        }
    }
    return equality; 
}

int *convertSetToArray(set<int> setter) {
    int *convertee = new int[setter.size()];
    for(set<int>::iterator node = setter.begin(); node != setter.end(); node++) {
        convertee[*node] = 1;
    }
    return convertee;
}

int initThirdRound() {
    MPI_Request req;
    int **message;
    message = new int*[size+1];
    int node_num = 0;
    for(vector<Node*>::iterator it_parent = root->children.begin();
            it_parent != root->children.end(); it_parent++, node_num++) {
        node_num = *((*it_parent)->label) + 1;
        message[node_num] = new int[size];
        for(vector<Node*>::iterator it_child = (*it_parent)->children.begin();
                it_child != (*it_parent)->children.end(); it_child++) {
            message[node_num][*((*it_child)->label + 1)] = (*it_child)->cval;
        }
    }

    message[0] = convertSetToArray(byz_set);

    for(int i = 0; i < size; i++) {
        if(i != rank) {
          MPI_Isend(message, (size+1)*size, MPI_INT, i, 3, MPI_COMM_WORLD, &req);
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);


    int **recd_msg;
    recd_msg = new int*[size+1];
    for(int i = 0; i < size; i++) {
        if(i != rank) {
            MPI_Irecv(recd_msg, (size+1)*size, MPI_INT, i, 3, MPI_COMM_WORLD, &req);
        } else {
            recd_msg = message;
        }
        int parent_num = 0;
        int child_num = 0;
        for(vector<Node*>::iterator it_parent = root->children.begin();
                it_parent != root->children.end(); it_parent++, node_num++) {
            for(vector<Node*>::iterator it_child = (*it_parent)->children.begin();
                    it_child != (*it_parent)->children.end(); it_child++) {
                parent_num = *((*it_child)->label);
                child_num = *((*it_child)->label + 1);
                if (parent_num != i || child_num != i) {
                    Node* newChild = new Node();
                    newChild->level  = 3;
                    newChild->label = new int[3];
                    *(newChild->label) = parent_num;
                    *(newChild->label + 1) = child_num; 
                    *(newChild->label + 2) = i; 
                    newChild->parent = *it_child;
                    (*it_child)->children.push_back(newChild);
                    newChild->cval = recd_msg[parent_num+1][child_num];
                    if (newChild->cval == (*it_child)->cval)
                        (*it_child)->count++;
                }
            }
        }
        temp_suspect_set[i] = recd_msg[0];
        addToSuspects(i, temp_suspect_set[i]);
    }
    for(vector<Node*>::iterator it_parent = root->children.begin();
            it_parent != root->children.end(); it_parent++) {
        for(vector<Node*>::iterator it_child = (*it_parent)->children.begin();
                it_child != (*it_parent)->children.end(); it_child++) {
            if ((*it_child)->count < size - num_failures) {
                byz_set.insert(*((*it_child)->label + 1));
            }
        }
    }
    return 1;
}

int initRoundR(int round) {
    MPI_Request req;
    int **message;
    message = new int*[size+1];
    message = temp_suspect_set;
    message[size+1] = convertSetToArray(byz_set);

    for(int i = 0; i < size; i++) {
        if(i != rank) {
          MPI_Isend(message, (size+1)*size, MPI_INT, i, round, MPI_COMM_WORLD, &req);
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);


    int **new_suspect_set;
    new_suspect_set = new int*[size];
    map<int, int > reported;
    int **recd_msg;
    recd_msg = new int*[size+1];
    for(int i = 0; i < size; i++) {
        if(i != rank) {
            MPI_Irecv(recd_msg, (size+1)*size, MPI_INT, i, round, MPI_COMM_WORLD, &req);
        } else {
            recd_msg = message;
        }
        int equality;
        for (int reporting_node = 0; reporting_node < size; reporting_node++) {
            equality = addToEchoesCheck(i, reporting_node, recd_msg[reporting_node]);
            if(equality) {
                reported[reporting_node]++;
            }
        }
        new_suspect_set[i] = recd_msg[size];
        addToSuspects(i, new_suspect_set[i]);
    }
    temp_suspect_set = new_suspect_set;

    for(map<int, int>::iterator it_reported = reported.begin(); it_reported != reported.end(); it_reported++) {
        if(it_reported->second < size - num_failures) {
                byz_set.insert(it_reported->first);
        }
    }
    return 1;
}
int find(int i, vector<int> label) {
    for(vector<int>::iterator it_label = label.begin(); it_label != label.end(); it_label++) {
        if(*it_label == i)
            return 0;
    }
    return 1;
}

int getCVal(vector<int> label, int level) {
    if(level == 2) {
        // TODO
    } else {
        int j = label[label.size()-3];
        int k = label[label.size()-2];
        int l = label[label.size()-1];
        if(echo_suspects[l][k].find(j) != echo_suspects[l][k].end())
            return -1;
    }
    return 1;
}

int getNewVal(vector<int> label, int level) {
    if(level == num_failures + 1) {
        // TODO
    }
    int count = 0;
    map<int, int> cval_count;
    for (int i = 0; i < size; i++) {
        vector<int> childLabel = label;
        if(!find(i, label)) {
                childLabel.push_back(i);
            if(getNewVal(childLabel, level+1) == 1) {
                cval_count[getCVal(childLabel, level+1)]++;
                count++;
            }
        }
    }
    if(count >= size - num_failures - level){
        int cval = -1;
        for(map<int, int>::iterator it_cval = cval_count.begin(); it_cval != cval_count.end(); it_cval++) {
            if(it_cval->second > count/2)
                cval = it_cval->first;
        }
        return cval;
    }
    return -1;
}

int extractDecision() {
    int newval;
    map<int, int> newValCount;
    for (int i = 0; i < size; i++) {
        vector<int> newLabel;
        newLabel.push_back(i);
        newval = getNewVal(newLabel, 1);
        newValCount[newval]++;
    }
    for(map<int, int>::iterator count = newValCount.begin(); count != newValCount.end(); count++) {
        if(count->second > size/2) {
            return count->first;
        }
    }
    return default_value;
}

int convertToSet() {
    for(int i = 0; i < size; i++) {
        set<int> mapped_set;
        for(int j = 0; j < size; j++) {
            if (temp_suspect_set[i][j] == 1)
                mapped_set.insert(j);
        }
        my_suspect_set[i] = mapped_set;
    }
    return 1;
}

int main(int argc, char *argv[]) {

    // Initialize MPI
    MPI_Init(&argc, &argv);
    // Get MPI rank
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    // Get MPI size
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // quorum_size is log of total number of nodes
    quorum_size = log2(size);

    num_failures = (size/3) - 1;


    // assigns to gstring one of the two argument gstrings passed randomly
    setMyValue();
    printf("Hello, world! "
            "from process %d of %d, my value is %d\n", rank, size, myvalue);
    root = new Node();
    root->level = 0;
    root->val = -1;

    //----------------FIRST ROUND----------------------
    initFirstRound();

    // To ensure all nodes have completed the push phase
    MPI_Barrier(MPI_COMM_WORLD);

    //----------------SECOND ROUND----------------------
    initSecondRound();

    temp_suspect_set = new int*[size];

    initThirdRound();

    convertToSet();

    for(int round = 4; round <= num_failures + 1; round++) {
        initRoundR(round);
    }

    MPI_Finalize();

    return(0);
}
