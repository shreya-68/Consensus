
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
int gstring = 0;

int has_decided = 0;

map<int, int> rand_strings;

// TAGS: 1 - Push message
//       2 - pull query
//       3 - Fw1
//       4 - Fw2
//       5 - Poll
//       6 - Answer

// stores the gstring - the value agreed upon by majority of nodes
int getRandBit() {
    sleep(rank*2);
    srand(time(NULL));
    return rand() % 2;
}

// generates all bit strings of length 'size'
vector<int> generateAllStrings(int size) {
    vector<int> all_strings;
    int prev_length = 1;
    if (size >= 1)
        all_strings.push_back(1);
    for (int length = 2; length <= size; length++) {
        for (int it = 0; it < prev_length; it++) {
            all_strings.push_back(all_strings[it]*10 + 1);
            all_strings[it] = all_strings[it]*10;
        }
        prev_length = all_strings.size();
    }
    if (rank == 0) {
        printf("All strings are size:%d\n", all_strings.size());
        for(int i = 0; i < prev_length; i++) {
            printf("%d\n", all_strings[i]);
        }
    }
    return all_strings;
}


// returns the pull quorum determined by a function H
set<int>* getPullQuorum(int candString, int id, int size) {
    set<int>* pullQuorum = new set<int>;
    int i, member;
    for (i = 0; i < quorum_size; i++) {
        member = ((candString*id) + (size*2/3) + i) % size;
        if (member != id) {
            pullQuorum->insert(member);
        }
    }
    return pullQuorum;
}

// returns the push quorum determined by a function I
set<int>* getPushQuorum(int candString, int id, int size) {
    set<int>* pushQuorum = new set<int>;
    int i, member;
    for (i = 0; i < quorum_size; i++) {
        member = ((candString*id) + (size*2/3) + i) % size;
        if (member != id) {
            pushQuorum->insert(member);
        }
    }
    return pushQuorum;
}

// returns the push quorum determined by a function J
set<int>* getPollList(int candString, int id, int size) {
    set<int>* pollList = new set<int>;
    int i, member;
    for (i = 0; i < quorum_size; i++) {
        member = ((candString*id) + (size*2/3) + i) % size;
        if (member != id) {
            pollList->insert(member);
        }
    }
    return pollList;
}

// Implements the Push phase in two parts. 1) Broadcast of gstring, 2)
// collecting of candidate strings. Returns an array of candidates
vector<int>* pushPhase() {
    MPI_Request req;
    set<int>* pushQuorum;
    int i;

    // Broadcast gstring to a node x if this node is in PushQuorum(gstring, x)
    for (i = 0; i < size; i++) {
        pushQuorum = getPushQuorum(gstring, i, size);
        if (pushQuorum->find(rank) != pushQuorum->end()) {
          printf("I %d am SENDING to %d\n", rank, i);
          MPI_Isend(&gstring, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &req);
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    // Recieve push messages from a node y for string s only if y is an element
    // of I(s, rank)
    vector<int>* candidates = new vector<int>;
    //int all_strings[pow(2, quorum_size)][1];
    //int num_strings = pow(2, quorum_size);
    map<int, int> all_strings;
    for (i = 0; i < size; i++) {
        int cand_string = 0;
        MPI_Irecv(&cand_string, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &req);
        while (cand_string != 0) {
            printf("%d RECIEVED from process %d, string is %d\n", rank, i, cand_string);
            pushQuorum = getPushQuorum(cand_string, rank, size);
            if (pushQuorum->find(i) != pushQuorum->end()) {
                all_strings[cand_string]++;
            }
            cand_string = 0;
            MPI_Irecv(&cand_string, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &req);
        }
    }

    // gets a list of all candidate strings that were pushed by more than half
    // the nodes in its push quorum
    for (map<int, int>::iterator mapIt = all_strings.begin();
            mapIt != all_strings.end(); mapIt++) {
        if (mapIt->second > quorum_size/2) {
            candidates->push_back(mapIt->first);
        }
    }
    return candidates;
} // pushPhase function ends

// Function to send pull requests
int sendPullRequests(vector<int> *candidates) {
    int r, rxs;
    vector<int> all_strings = generateAllStrings(quorum_size);
    int num = all_strings.size();
    set<int>* pollList;
    set<int>* pullQuorum;
    MPI_Request req;
    for (vector<int>::iterator candIt = candidates->begin();
            candIt != candidates->end(); candIt++) {
        r = rand() % num;
        rxs = all_strings[r];
        rand_strings[*candIt] = rxs;
        pollList = getPollList(rxs, rank, size);
        int message[2] = {*candIt, rxs};
        for (set<int>::iterator pollIt = pollList->begin();
                pollIt != pollList->end(); pollIt++) {
            MPI_Isend(message, 2, MPI_INT, *pollIt, 5, MPI_COMM_WORLD, &req);
            printf("PULL PHASE POLL: I %d am SENDING to %d: message %d,%d\n", rank, *pollIt, *candIt, rxs);
        }
        pullQuorum = getPullQuorum(*candIt, rank, size);
        for (set<int>::iterator pullIt = pullQuorum->begin();
                pullIt != pullQuorum->end(); pullIt++) {
            MPI_Isend(message, 2, MPI_INT, *pullIt, 2, MPI_COMM_WORLD, &req);
            printf("PULL PHASE PULL: I %d am SENDING to %d: message %d,%d\n", rank, *pullIt, *candIt,rxs);
        }
    }
    return 1;
}

int routingPullRequests() {
    MPI_Request req;
    int i;
    int *recd_string;
    set<int> *pullQuorum;
    set<int> *pollList;
    for (i = 0; i < size; i++) {
        recd_string = new int[2];
        recd_string[0] = -1;
        MPI_Irecv(recd_string, 2, MPI_INT, i, 2, MPI_COMM_WORLD, &req);
        while (recd_string[0] != -1) {
            pullQuorum = getPullQuorum(gstring, i, size);
            printf("PULL PHASE routing: I %d am RECEIVING from %d: message %d,%d\n", rank, i, recd_string[0], recd_string[1]);
            if (recd_string[0] == gstring && pullQuorum->find(rank) != pullQuorum->end()) {
                printf("PULL PHASE routing AFTER: I %d am RECEIVING from %d: message %d\n", rank, i, recd_string[0]);
                pollList = getPollList(recd_string[1], i, size);
                set<int> *pullQW;
                for (set<int>::iterator pollIt = pollList->begin();
                        pollIt != pollList->end(); pollIt++) {
                    pullQW = getPullQuorum(gstring, *pollIt, size);
                    int message[4] = {i, gstring, recd_string[1], *pollIt};
                    for (set<int>::iterator pullIt = pullQW->begin();
                            pullIt != pullQW->end(); pullIt++) {
                        MPI_Isend(message, 4, MPI_INT, *pullIt, 3, MPI_COMM_WORLD, &req);
                    }
                }
            }
            recd_string = new int[2];
            recd_string[0] = -1;
            MPI_Irecv(recd_string, 2, MPI_INT, i, 2, MPI_COMM_WORLD, &req);
        }
    }

    // To ensure all nodes have completed processing pull requests
    MPI_Barrier(MPI_COMM_WORLD);

    map< pair<int, int>, int> fwcount;
    set<int> *pullQW;
    set<int> *pullQX;
    set<int> *pollQX;
    for (i = 0; i < size; i++) {
        recd_string = new int[4];
        recd_string[0] = -1;
        MPI_Irecv(recd_string, 4, MPI_INT, i, 3, MPI_COMM_WORLD, &req);
        while (recd_string[0] != -1) {
            pullQW = getPullQuorum(gstring, recd_string[3], size);
            pullQX = getPullQuorum(gstring, recd_string[0], size);
            pollQX = getPollList(recd_string[2], recd_string[0], size);
            if (recd_string[1] == gstring && pullQW->find(rank) != pullQW->end() &&
                    pullQX->find(i) != pullQX->end() &&
                    pollQX->find(recd_string[3]) != pollQX->end()) {
                fwcount[make_pair(gstring, recd_string[0])]++;
                if (fwcount[make_pair(gstring, recd_string[0])] > pullQX->size()/2) {
                    int message[3] = {recd_string[0], recd_string[1], recd_string[2]};
                    MPI_Isend(message, 3, MPI_INT, recd_string[3], 4, MPI_COMM_WORLD, &req);
                    fwcount[make_pair(gstring, recd_string[0])] = -100000;

                }
            }
            recd_string = new int[4];
            recd_string[0] = -1;
            MPI_Irecv(recd_string, 4, MPI_INT, i, 3, MPI_COMM_WORLD, &req);
        }
    }
    return 1;
}

int answerPullRequests() {
    MPI_Request req;
    int i;
    int *recd_msg;
    map<int, int> count;
    set< pair<int, int> > polled;
    map< pair<int, int>, int> fw2count;
    set<int> *pullQuorum;
    set<int> *pollQX;

    for (i = 0; i < size; i++) {
        recd_msg = new int[3];
        recd_msg[0] = -1;
        MPI_Irecv(recd_msg, 3, MPI_INT, i, 4, MPI_COMM_WORLD, &req);
        while (recd_msg[0] != -1) {
            if (count.find(recd_msg[1]) != count.end()) {
                if (count[recd_msg[1]] > log2(size)*log2(size)) {
                    //wait for has_decided
                }
            }
            pullQuorum = getPullQuorum(gstring, rank, size);
            pollQX = getPollList(recd_msg[2], recd_msg[0], size);
            if (recd_msg[1] == gstring && pullQuorum->find(i) != pullQuorum->end() &&
                    pollQX->find(rank) != pollQX->end()) {
                fw2count[make_pair(gstring, recd_msg[0])]++;
                if (fw2count[make_pair(gstring, recd_msg[0])] > pullQuorum->size()/2 &&
                        polled.find(make_pair(recd_msg[0], gstring)) != polled.end()) {
                    count[recd_msg[1]]++;
                    MPI_Isend(&gstring, 1, MPI_INT, recd_msg[0], 6, MPI_COMM_WORLD, &req);
                    fw2count[make_pair(gstring, recd_msg[0])] = -100000;
                }
            }
            recd_msg = new int[3];
            recd_msg[0] = -1;
            MPI_Irecv(recd_msg, 3, MPI_INT, i, 4, MPI_COMM_WORLD, &req);
        }
    }
    for (i = 0; i < size; i++) {
        recd_msg = new int[2];
        recd_msg[0] = -1;
        MPI_Irecv(recd_msg, 2, MPI_INT, i, 5, MPI_COMM_WORLD, &req);
        while (recd_msg[0] != -1) {
            pullQuorum = getPullQuorum(recd_msg[0], rank, size);
            pollQX = getPollList(recd_msg[1], i, size);
            if (pollQX->find(rank) != pollQX->end()) {
                polled.insert(make_pair(i, recd_msg[0]));
                if (fw2count[make_pair(recd_msg[0], i)] > pullQuorum->size()/2) {
                    count[recd_msg[0]]++;
                    MPI_Isend(&recd_msg[0], 1, MPI_INT, i, 6, MPI_COMM_WORLD, &req);
                    fw2count[make_pair(recd_msg[0], i)] = -100000;
                }
            }
            recd_msg = new int[2];
            recd_msg[0] = -1;
            MPI_Irecv(recd_msg, 2, MPI_INT, i, 5, MPI_COMM_WORLD, &req);
        }
    }
    return 1;
}

int processAnswers() {
    int recd_msg;
    MPI_Request req;
    int i;
    map<int, int> count;
    set<int> *pollQX;
    map< int, set<int> > sentAnswer;
    for (i = 0; i < size; i++) {
        recd_msg = -1;
        MPI_Irecv(&recd_msg, 1, MPI_INT, i, 6, MPI_COMM_WORLD, &req);
        while (recd_msg != -1) {
            pollQX = getPollList(rand_strings[recd_msg], rank, size);
            if (pollQX->find(i) != pollQX->end() && sentAnswer[i].find(recd_msg) == sentAnswer[i].end()) {
                count[recd_msg]++;
                if(count[recd_msg] > pollQX->size()/2) {
                    has_decided = 1;
                    gstring = recd_msg;
                    return recd_msg;
                }
            }
            recd_msg = -1;
            MPI_Irecv(&recd_msg, 2, MPI_INT, i, 6, MPI_COMM_WORLD, &req);
        }
    }
    return gstring;
}

int pullPhase(vector<int> *candidates) {
    sendPullRequests(candidates);
    // To ensure all nodes have completed sending pull requests
    MPI_Barrier(MPI_COMM_WORLD);
    routingPullRequests();
    // To ensure all nodes have completed routing pull requests
    MPI_Barrier(MPI_COMM_WORLD);
    answerPullRequests();
    //// To ensure all nodes have completed routing pull requests
    MPI_Barrier(MPI_COMM_WORLD);
    return processAnswers();
}

int main(int argc, char *argv[]) {

    int i;

    // Initialize MPI
    MPI_Init(&argc, &argv);
    // Get MPI rank
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    // Get MPI size
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // quorum_size is log of total number of nodes
    quorum_size = log2(size);

    // assigns to gstring one of the two argument gstrings passed randomly
    if (getRandBit()) {
        for (i = 0; i < quorum_size; i++) {
            gstring = gstring*10 + (argv[1][i] - '0');
        }
    } else {
        for (i = 0; i < quorum_size; i++) {
            gstring = gstring*10 + (argv[2][i] - '0');
        }
    }
    printf("Hello, world! "
            "from process %d of %d, my gstring is %d\n", rank, size, gstring);

    //----------------PUSH PHASE----------------------
    vector<int> *candidates = pushPhase();

    // To ensure all nodes have completed the push phase
    MPI_Barrier(MPI_COMM_WORLD);

    //----------------PULL PHASE----------------------
    int final_string = pullPhase(candidates);
    printf("My %d final value is %d\n", rank, final_string);


    MPI_Finalize();

    return(0);
}
