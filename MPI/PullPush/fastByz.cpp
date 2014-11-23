
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
int number_of_messages;
int number_of_bits;
int num_failures;
int byzantine;
int all_size = 0;

map<int, int> rand_strings;

// TAGS: 1 - Push message
//       2 - pull query
//       3 - Fw1
//       4 - Fw2
//       5 - Poll
//       6 - Answer

// stores the gstring - the value agreed upon by majority of nodes
int getRandBit(int num) {
    sleep(rank*2);
    srand(time(NULL));
    return rand() % num;
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
    //if (rank == 0) {
    //    printf("All strings are size:%d\n", int(all_strings.size()));
    //    for(int i = 0; i < prev_length; i++) {
    //        printf("%d\n", all_strings[i]);
    //    }
    //}
    return all_strings;
}


// returns the pull quorum determined by a function H
set<int>* getPullQuorum(int candString, int id, int size) {
    set<int>* pullQuorum = new set<int>;
    int i, member = (candString*id) + (size*2/3) - 1;
    for (i = 0; i < quorum_size; i++) {
        member = (member + 1) % size;
        if (member != id) {
            pullQuorum->insert(member);
        } else {
            member = (member + 1) % size;
            if (member != id)
                pullQuorum->insert(member);
        }
    }
    return pullQuorum;
}

// returns the push quorum determined by a function I
set<int>* getPushQuorum(int candString, int id, int size) {
    set<int>* pushQuorum = new set<int>;
    int i, member = (candString*id) + (size*2/3) - 1;
    for (i = 0; i < quorum_size; i++) {
        member = (member + 1) % size;
        if (member != id) {
            pushQuorum->insert(member);
        } else {
            member = (member + 1) % size;
            if (member != id)
                pushQuorum->insert(member);
        }
    }
    return pushQuorum;
}

// returns the push quorum determined by a function J
set<int>* getPollList(int candString, int id, int size) {
    set<int>* pollList = new set<int>;
    int i, member = (candString*id) + (size*2/3) - 1;
    for (i = 0; i < quorum_size; i++) {
        member = (member + 1) % size;
        if (member != id) {
            pollList->insert(member);
        } else {
            member = (member + 1) % size;
            if (member != id)
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
    MPI_Status stats;
    for (i = 0; i < size; i++) {
        pushQuorum = getPushQuorum(gstring, i, size);
        if (pushQuorum->find(rank) != pushQuorum->end()) {
          //printf("I %d am SENDING to %d\n", rank, i);
            MPI_Send(&gstring, 1, MPI_INT, i, 1, MPI_COMM_WORLD);
        }
	    delete pushQuorum;
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
            //if(rank == 0)
            //    printf("%d RECIEVED from process %d, string is %d\n", rank, i, cand_string);
            pushQuorum = getPushQuorum(cand_string, rank, size);
            if (pushQuorum->find(i) != pushQuorum->end()) {
	  	        number_of_messages++;
	  	        number_of_bits++;
                all_strings[cand_string]++;
            }
            cand_string = 0;
            MPI_Irecv(&cand_string, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &req);
	        delete pushQuorum;
        }
    }

    // gets a list of all candidate strings that were pushed by more than half
    // the nodes in its push quorum
    for (map<int, int>::iterator mapIt = all_strings.begin();
            mapIt != all_strings.end(); mapIt++) {
        if (mapIt->second > quorum_size/2) {
            candidates->push_back(mapIt->first);
            //if(rank == 0)
            //    printf("%d's candidate list includes %d\n", rank, mapIt->first);
        }
    }
    return candidates;
} // pushPhase function ends

// Function to send pull requests
int sendPullRequests(vector<int> *candidates) {
    int r, rxs;
    vector<int> all_strings = generateAllStrings(quorum_size);
    all_size = all_strings.size();
    set<int>* pollList;
    set<int>* pullQuorum;
    MPI_Request req;
    MPI_Status stats;
    for (vector<int>::iterator candIt = candidates->begin();
            candIt != candidates->end(); candIt++) {
        r = getRandBit(all_size);
        rxs = all_strings[r];
        rand_strings[*candIt] = rxs;
        pollList = getPollList(rxs, rank, size);
        int message[2] = {*candIt, rxs};
        if(byzantine) {
	        number_of_messages += 2*quorum_size*all_size/size;
	        number_of_bits += 4*quorum_size*all_size/size;
        }
        for (set<int>::iterator pollIt = pollList->begin();
                pollIt != pollList->end(); pollIt++) {
            MPI_Send(message, 2, MPI_INT, *pollIt, 5, MPI_COMM_WORLD);
            if(!byzantine) {
	            number_of_messages++;
	            number_of_bits += 2;
            }
            //if(*pollIt == 0)
            //    printf("PULL PHASE POLL: I %d am SENDING to %d: message %d,%d\n", rank, *pollIt, *candIt, rxs);
        }
        delete pollList;
        pullQuorum = getPullQuorum(*candIt, rank, size);
        for (set<int>::iterator pullIt = pullQuorum->begin();
                pullIt != pullQuorum->end(); pullIt++) {
            MPI_Send(message, 2, MPI_INT, *pullIt, 2, MPI_COMM_WORLD);
            if(!byzantine) {
	            number_of_messages++;
	            number_of_bits += 2;
            }
            //if(*pullIt == 0)
            //    printf("PULL PHASE PULL: I %d am SENDING to %d: message %d,%d\n", rank, *pullIt, *candIt,rxs);
        }
	    delete pullQuorum;
    }
    return 1;
}

int routingPullRequests() {
    MPI_Request req;
    MPI_Status stats;
    int i;
    int *recd_string;
    set<int> *pullQuorum;
    set<int> *pollList;
    for (i = 0; i < size; i++) {
        if(byzantine) {
	        number_of_messages += quorum_size*quorum_size*all_size/size;
	        number_of_bits += 4*quorum_size*quorum_size*all_size/size;
        }
        recd_string = new int[2];
        recd_string[0] = -1;
        MPI_Irecv(recd_string, 2, MPI_INT, i, 2, MPI_COMM_WORLD, &req);
        while (recd_string[0] != -1) {
            pullQuorum = getPullQuorum(gstring, i, size);
            //if(rank == 0)
            //    printf("PULL PHASE routing: I %d am RECEIVING from %d: message %d,%d\n", rank, i, recd_string[0], recd_string[1]);
            if (recd_string[0] == gstring && pullQuorum->find(rank) != pullQuorum->end()) {
                //if(rank == 0)
                //    printf("PULL PHASE routing AFTER: I %d am RECEIVING from %d: message %d, %d\n", rank, i, recd_string[0], recd_string[1]);
                pollList = getPollList(recd_string[1], i, size);
                set<int> *pullQW;
                for (set<int>::iterator pollIt = pollList->begin();
                        pollIt != pollList->end(); pollIt++) {
                    pullQW = getPullQuorum(gstring, *pollIt, size);
                    int message[4] = {i, gstring, recd_string[1], *pollIt};
                    for (set<int>::iterator pullIt = pullQW->begin();
                            pullIt != pullQW->end(); pullIt++) {
                        MPI_Send(message, 4, MPI_INT, *pullIt, 3, MPI_COMM_WORLD);
                        if(!byzantine) {
	    		            number_of_messages++;
	    		            number_of_bits += 4;
                        }
                        //if(*pullIt == 0)
                        //    printf("PULL PHASE sending FW1: I %d am sending to %d: message %d, %d, %d, %d\n", rank, *pullIt, i, gstring, recd_string[1], *pollIt);
                    }
                }
                delete pollList;
	            delete pullQuorum;
            }
            delete[] recd_string;
            recd_string = new int[2];
            recd_string[0] = -1;
            MPI_Irecv(recd_string, 2, MPI_INT, i, 2, MPI_COMM_WORLD, &req);
        }
        delete[] recd_string;
    }

    // To ensure all nodes have completed processing pull requests
    MPI_Barrier(MPI_COMM_WORLD);

    map< pair<int, int>, int> fwcount;
    set<int> *pullQW;
    set<int> *pullQX;
    set<int> *pollQX;
    for (i = 0; i < size; i++) {
        if(byzantine) {
	        number_of_messages += quorum_size*all_size/size;
	        number_of_bits += 3*quorum_size*all_size/size;
        }
        recd_string = new int[4];
        recd_string[0] = -1;
        MPI_Irecv(recd_string, 4, MPI_INT, i, 3, MPI_COMM_WORLD, &req);
        while (recd_string[0] != -1) {
            //if(rank == 0)
            //    printf("PULL PHASE RECEIVING FW1: I %d am RECEIVING from %d: message %d,%d, %d, %d\n", rank, i, recd_string[0], recd_string[1], recd_string[2], recd_string[3]);
            pullQW = getPullQuorum(gstring, recd_string[3], size);
            pullQX = getPullQuorum(gstring, recd_string[0], size);
            pollQX = getPollList(recd_string[2], recd_string[0], size);
            if (recd_string[1] == gstring && pullQW->find(rank) != pullQW->end() &&
                    pullQX->find(i) != pullQX->end() &&
                    pollQX->find(recd_string[3]) != pollQX->end()) {
                fwcount[make_pair(gstring, recd_string[0])]++;
                if (fwcount[make_pair(gstring, recd_string[0])] > pullQX->size()/2) {
                    int message[3] = {recd_string[0], recd_string[1], recd_string[2]};
                    MPI_Send(message, 3, MPI_INT, recd_string[3], 4, MPI_COMM_WORLD);
                    if(!byzantine) {
	    	            number_of_messages++;
	    	            number_of_bits += 3;
                    }
                    //if(recd_string[3] == 0)
                    //    printf("PULL PHASE sending FW2: I %d am sending to %d: message %d, %d, %d\n", rank, recd_string[3], message[0], message[1], message[2]);
                    fwcount[make_pair(gstring, recd_string[0])] = -100000;

                }
            }
	        delete pullQW;
	        delete pullQX;
	        delete pollQX;
            delete[] recd_string;
            recd_string = new int[4];
            recd_string[0] = -1;
            MPI_Irecv(recd_string, 4, MPI_INT, i, 3, MPI_COMM_WORLD, &req);
        }
        delete[] recd_string;
    }
    return 1;
}

int answerPullRequests() {
    MPI_Request req;
    MPI_Status stats;
    int i;
    int *recd_msg;
    map<int, int> count;
    set< pair<int, int> > polled;
    map< pair<int, int>, int> fw2count;
    set<int> *pullQuorum;
    set<int> *pollQX;

    for (i = 0; i < size; i++) {
        if(byzantine) {
	        number_of_messages += 2*quorum_size*all_size/size;
	        number_of_bits += 2*quorum_size*all_size/size;
        }
        recd_msg = new int[3];
        recd_msg[0] = -1;
        MPI_Irecv(recd_msg, 3, MPI_INT, i, 4, MPI_COMM_WORLD, &req);
        while (recd_msg[0] != -1) {
            //if(rank == 0)
            //    printf("PULL PHASE RECEIVING FW2: I %d am RECEIVING from %d: message %d,%d, %d\n", rank, i, recd_msg[0], recd_msg[1], recd_msg[2]);
            if (count.find(recd_msg[1]) != count.end()) {
                if (count[recd_msg[1]] > log2(size)*log2(size)) {
                    //wait for has_decided
                    while(!has_decided);
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
                    MPI_Send(&gstring, 1, MPI_INT, recd_msg[0], 6, MPI_COMM_WORLD);
                    if(!byzantine) {
	    	            number_of_messages++;
	    	            number_of_bits += 1;
                    }
                    if(recd_msg[0] == 0)
                        printf("PULL PHASE sending ANSWER: I %d am sending to %d: message %d\n", rank, recd_msg[0], gstring);
                    fw2count[make_pair(gstring, recd_msg[0])] = -100000;
                }
            }
            delete pollQX;
	        delete pullQuorum;
            delete[] recd_msg;
            recd_msg = new int[3];
            recd_msg[0] = -1;
            MPI_Irecv(recd_msg, 3, MPI_INT, i, 4, MPI_COMM_WORLD, &req);
        }
        delete[] recd_msg;
    }
    for (i = 0; i < size; i++) {
        recd_msg = new int[2];
        recd_msg[0] = -1;
        MPI_Irecv(recd_msg, 2, MPI_INT, i, 5, MPI_COMM_WORLD, &req);
        while (recd_msg[0] != -1) {
            //if(rank == 0)
            //    printf("PULL PHASE ANSWERING POLL: I %d am RECEIVING from %d: message %d,%d\n", rank, i, recd_msg[0], recd_msg[1]);
            pullQuorum = getPullQuorum(recd_msg[0], rank, size);
            pollQX = getPollList(recd_msg[1], i, size);
            if (pollQX->find(rank) != pollQX->end()) {
                polled.insert(make_pair(i, recd_msg[0]));
                if (fw2count[make_pair(recd_msg[0], i)] > pullQuorum->size()/2) {
                    count[recd_msg[0]]++;
                    MPI_Send(&recd_msg[0], 1, MPI_INT, i, 6, MPI_COMM_WORLD);
                    if(!byzantine) {
	    	            number_of_messages++;
	    	            number_of_bits += 1;
                    }
                    if(i == 0)
                        printf("PULL PHASE sending ANSWER: I %d am sending to %d: message %d\n", rank, i, recd_msg[0]);
                    fw2count[make_pair(recd_msg[0], i)] = -100000;
                }
            }
            delete pollQX;
            delete pullQuorum;
            delete[] recd_msg;
            recd_msg = new int[2];
            recd_msg[0] = -1;
            MPI_Irecv(recd_msg, 2, MPI_INT, i, 5, MPI_COMM_WORLD, &req);
        }
        delete[] recd_msg;
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
            if(rank == 0)
                printf("PULL PHASE RECEIVING ANSWER: I %d am receiving from %d: message %d\n", rank, i, recd_msg);
            if (pollQX->find(i) != pollQX->end() && sentAnswer[i].find(recd_msg) == sentAnswer[i].end()) {
                count[recd_msg]++;
                if(count[recd_msg] > pollQX->size()/2) {
		    printf("I am deciding\n");
                    has_decided = 1;
                    gstring = recd_msg;
                    return recd_msg;
                }
            }
            delete pollQX;
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

    // Initialize MPI
    MPI_Init(&argc, &argv);
    // Get MPI rank
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    // Get MPI size
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // quorum_size is log of total number of nodes
    quorum_size = log2(size);

    number_of_messages = 0;
    number_of_bits = 0;
    int value1 = 0;
    int value2 = 0;

    num_failures = (size/3) - 1;
    num_failures = num_failures > 0? num_failures:0;


    for (int i = 0; i < quorum_size; i++) {
        value1 = value1*10 + (argv[1][i] - '0');
    }
    for (int i = 0; i < quorum_size; i++) {
        value2 = value2*10 + (argv[2][i] - '0');
    }
    // assigns to gstring one of the two argument gstrings passed randomly
    if((rank % (size/num_failures) == 0) && rank != 0) {
        byzantine = 1;
        gstring = value1;
    } else {
        gstring = value2;
    }
    //if (getRandBit(1)) {
    //    gstring = value1;
    //} else {
    //    gstring = value2;
    //}
    printf("Hello, world! "
            "from process %d of %d, my gstring is %d\n", rank, size, gstring);

    //----------------PUSH PHASE----------------------
    vector<int> *candidates = pushPhase();

    // To ensure all nodes have completed the push phase
    MPI_Barrier(MPI_COMM_WORLD);

    //----------------PULL PHASE----------------------
    int final_string = pullPhase(candidates);
    printf("My %d final value is %d\n", rank, final_string);

    int *msg_per_node;
    int *bit_per_node;
    int *value_per_node;
    int *final_per_node;
    if(rank == 0) {
	    msg_per_node = new int[size];
	    bit_per_node = new int[size];
	    value_per_node = new int[size];
	    final_per_node = new int[size];
    }
    printf("%d's total messages = %d\n", rank, number_of_messages);
    MPI_Gather(&number_of_messages, 1, MPI_INT, msg_per_node, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Gather(&number_of_bits, 1, MPI_INT, bit_per_node, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Gather(&gstring, 1, MPI_INT, value_per_node, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Gather(&final_string, 1, MPI_INT, final_per_node, 1, MPI_INT, 0, MPI_COMM_WORLD);
    int total_messages = 0;
    int total_bits = 0;
    int total_value1 = 0;
    int total_value2 = 0;
    int final_value1 = 0;
    int final_value2 = 0;
    if(rank == 0) {
	    for(int i = 0; i < size; i++) {
	        total_messages += msg_per_node[i];
	        total_bits += bit_per_node[i];
	        if(value_per_node[i] == value1)
	    	total_value1++;
	        else
	    	total_value2++;
	        if(final_per_node[i] == value1)
	    	final_value1++;
	        else
	    	final_value2++;
	    }
	    printf("Total number of messages = %d \n Total number of bits = %d\n", total_messages, total_bits);
	    printf("Total number of %d = %d \n Total number of %d = %d\n", value1, total_value1, value2, total_value2);
	    printf("Total number of final %d = %d \n Total number of final %d = %d\n", value1, final_value1, value2, final_value2);
    }
	delete[] msg_per_node;
	delete[] bit_per_node;
	delete[] value_per_node;
	delete[] final_per_node;

    MPI_Finalize();

    return(1);
}
