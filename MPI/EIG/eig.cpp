
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
#include "mpi.h"
#include <iostream>

#define MAX 2

using namespace std;

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

int byzantine;
int second_value;
int number_of_messages;
int number_of_bits;

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
    myvalue = (rand() % MAX) + 3;
    return 1;
}

int initFirstRound() {
    //cout<<"Initiating first round\n";
    MPI_Request req;
    MPI_Status stats;
    for(int i = 0; i < size; i++) {
        if(i != rank) {
            if(!byzantine) {
                MPI_Send(&myvalue, 1, MPI_INT, i, 1, MPI_COMM_WORLD);
            } else if(i <= size/2) {
                MPI_Send(&myvalue, 1, MPI_INT, i, 1, MPI_COMM_WORLD);
            } else {
                MPI_Send(&second_value, 1, MPI_INT, i, 1, MPI_COMM_WORLD);
            }
	    	number_of_messages++;
	    	number_of_bits += 1;
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
        printf("I %d received value %d from process %d\n", rank, newChild->val, i);
    }
    cout<<"Completing first round\n";
    return 1;
}

int initSecondRound() {
    cout<<"Initiating second round\n";
    MPI_Request req;
    int *message = new int[size];
    for(vector<Node*>::iterator it = root->children.begin();
            it != root->children.end(); it++) {
        message[*((*it)->label)] = (*it)->val;
        if(rank == 0)
            printf("sending value %d from process %d, %d\n", (*it)->val, *((*it)->label), rank);
    }

    MPI_Status stats;
    for(int i = 0; i < size; i++) {
        if(i != rank) {
            MPI_Send(message, size, MPI_INT, i, 2, MPI_COMM_WORLD);
	    	number_of_messages++;
	    	number_of_bits += size;
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
                //if(rank == 1 && i == 0)
                //    printf("I %d setting value %d received for child %d,%d\n", rank, recd_msg[next_recd_val], *((*it)->label), i);
            }
        }
	delete[] recd_msg;
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
            if(rank == 0)
                printf("My byz set includes %d\n", *((*it_parent)->label));
        }
    }
    cout<<"Completing second round\n";
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
    int *convertee = new int[size];
    for(set<int>::iterator node = setter.begin(); node != setter.end(); node++) {
        convertee[*node] = 1;
    }
    return convertee;
}

int initThirdRound() {
    cout<<"Initiating third round\n";
    MPI_Request req;
    int **msg = new int*[size];
    int *message;
    int node_num = 0;
    MPI_Status stats;
    for(vector<Node*>::iterator it_parent = root->children.begin();
            it_parent != root->children.end(); it_parent++) {
        node_num = *((*it_parent)->label);
        message = new int[size];
        for(vector<Node*>::iterator it_child = (*it_parent)->children.begin();
                it_child != (*it_parent)->children.end(); it_child++) {
            message[*((*it_child)->label + 1)] = (*it_child)->cval;
        }
        msg[node_num] = message;
        for(int i = 0; i < size; i++) {
            if(i != rank) {
                MPI_Send(message, size, MPI_INT, i, node_num, MPI_COMM_WORLD);
	    	    number_of_messages++;
	    	    number_of_bits += size;
            }
        }
    }

    message = convertSetToArray(byz_set);
    if(rank == 0) {
        for(int i = 0; i < size; i++) {
            if(message[i] == 1)
                printf("My byz array has: %d\n", i);
        }
    }

    for(int i = 0; i < size; i++) {
        if(i != rank) {
            MPI_Send(message, size, MPI_INT, i, size+1, MPI_COMM_WORLD);
	    	number_of_messages++;
	    	number_of_bits += size;
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);


    int *recd_msg;
    int child_num = 0;
    for(vector<Node*>::iterator it_parent = root->children.begin();
            it_parent != root->children.end(); it_parent++) {
        node_num = *((*it_parent)->label);
        for(int i = 0; i < size; i++) {
            if(i != rank) {
                recd_msg = new int[size];
                MPI_Irecv(recd_msg, size, MPI_INT, i, node_num, MPI_COMM_WORLD, &req);
            } else {
                recd_msg = msg[node_num];
            }
            for(vector<Node*>::iterator it_child = (*it_parent)->children.begin();
                    it_child != (*it_parent)->children.end(); it_child++) {
                child_num = *((*it_child)->label + 1);
                if (node_num != i || child_num != i) {
                    Node* newChild = new Node();
                    newChild->level  = 3;
                    newChild->label = new int[3];
                    *(newChild->label) = node_num;
                    *(newChild->label + 1) = child_num; 
                    *(newChild->label + 2) = i; 
                    newChild->parent = *it_child;
                    (*it_child)->children.push_back(newChild);
                    newChild->cval = recd_msg[child_num];
                    //if(i == 1 && rank == 2 && child_num == 0) {
                    //    printf("Value is %d, for node %d, %d, %d\n", recd_msg[child_num], node_num, child_num, i);
                    //}
                    if (newChild->cval == (*it_child)->cval)
                        (*it_child)->count++;
                }
            }
	    delete[] recd_msg;
        }
    }
    for(int i = 0; i < size; i++) {
        if(i != rank) {
            recd_msg = new int[size];
            MPI_Irecv(recd_msg, size, MPI_INT, i, size+1, MPI_COMM_WORLD, &req);
        } else {
            recd_msg = message;
        }
	delete[] temp_suspect_set[i];
        temp_suspect_set[i] = recd_msg;
        addToSuspects(i, temp_suspect_set[i]);
    }
    for(vector<Node*>::iterator it_parent = root->children.begin();
            it_parent != root->children.end(); it_parent++) {
        for(vector<Node*>::iterator it_child = (*it_parent)->children.begin();
                it_child != (*it_parent)->children.end(); it_child++) {
            if ((*it_child)->count < size - num_failures) {
                byz_set.insert(*((*it_child)->label + 1));
                if(rank == 0)
                    printf("My byz set includes %d\n", *((*it_child)->label + 1));
            }
        }
    }
    //printf("Completing third round\n");
    return 1;
}

int initRoundR(int round) {
    MPI_Request req;
    printf("Initiating round %d\n", round);
    int *message;

    MPI_Status stats;
    for(int suspector = 0; suspector < size; suspector++) {
	if(rank != suspector) {
            for(int j = 0; j < size; j++) {
                if(j != rank && suspector != j) {
       	            MPI_Send(temp_suspect_set[suspector], size, MPI_INT, j, suspector, MPI_COMM_WORLD);
		            number_of_messages++;
		            number_of_bits += size;
        	    }
            }
	}
    }

    message = convertSetToArray(byz_set);
    for(int i = 0; i < size; i++) {
        if(i != rank) {
            MPI_Send(message, size, MPI_INT, i, size+1, MPI_COMM_WORLD);
	    	number_of_messages++;
	    	number_of_bits += size;
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);


    map<int, int > reported;
    int *recd_msg;
    for (int reporting_node = 0; reporting_node < size; reporting_node++) {
	if(rank != reporting_node) {
            for(int i = 0; i < size; i++) {
                if(i != rank) {
                    recd_msg = new int[size];
                    MPI_Irecv(recd_msg, size, MPI_INT, i, reporting_node, MPI_COMM_WORLD, &req);
                } else {
                    recd_msg = temp_suspect_set[reporting_node];
                }
                int equality = addToEchoesCheck(i, reporting_node, recd_msg);
                if(equality) {
                    reported[reporting_node]++;
                }
	        delete[] recd_msg;
            }
	}
    }
    for(int i = 0; i < size; i++) {
        if(i != rank) {
            recd_msg = new int[size];
            MPI_Irecv(recd_msg, size, MPI_INT, i, size+1, MPI_COMM_WORLD, &req);
        } else {
            recd_msg = message;
        }
	delete[] temp_suspect_set[i];
        temp_suspect_set[i] = recd_msg;
        addToSuspects(i, temp_suspect_set[i]);
    }

    for(map<int, int>::iterator it_reported = reported.begin(); it_reported != reported.end(); it_reported++) {
        if(it_reported->second < size - num_failures) {
                byz_set.insert(it_reported->first);
        }
    }
    printf("Completing round %d\n", round);
    return 1;
}
int find(int i, vector<int> label) {
    for(vector<int>::iterator it_label = label.begin(); it_label != label.end(); it_label++) {
        if(*it_label == i)
            return 1;
    }
    return 0;
}

int getCVal(vector<int> label, int level) {
    if(level == 2) {
        Node *parent = root->children[label[0]];
        if(label[1] > label[0])
            return parent->children[label[1]-1]->cval;
        else
            return parent->children[label[1]]->cval;
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
        if(num_failures == 0)
            return root->children[label[0]]->val;
        if(num_failures == 1) {
            return 1;
        }
        if (label.size() >= 2) {
            int l = label[label.size()-1];
            int k = label[label.size()-2];
            if(temp_suspect_set[l][k] == 1) {
                return -1;
            } else {
                return 1;
            }
        }
    } else {
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
                childLabel.pop_back();
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
        set<int> *mapped_set = new set<int>;
        for(int j = 0; j < size; j++) {
            if (temp_suspect_set[i][j] == 1)
                mapped_set->insert(j);
        }
        my_suspect_set[i] = *mapped_set;
    }
    return 1;
}

void pretty_print() {
    if (rank == 0) {
        printf("My byzantine suspects are: \n");
        for(set<int>::iterator it=byz_set.begin(); it != byz_set.end(); it++)
                printf("%d\n", *it);
    }
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
    num_failures = num_failures > 0? num_failures:0;

    number_of_messages = 0;
    number_of_bits = 0;

    // assigns to gstring one of the two argument gstrings passed randomly
    setMyValue();
    root = new Node();
    root->level = 0;
    root->val = -1;

    if((rank % (size/num_failures) == 0) && rank != 0) {
        byzantine = 1;
        second_value = (myvalue + 1) % MAX;
    } else {
        byzantine = 0;
    }

    printf("Hello, world! "
            "from process %d of %d, my value is %d and I am %d byzantine\n", rank, size, myvalue, byzantine);


    //----------------FIRST ROUND----------------------
    initFirstRound();

    //----------------SECOND ROUND----------------------
    if (num_failures + 1 >= 2) {
        initSecondRound();
    }

    temp_suspect_set = new int*[size];
    for(int i = 0; i < size; i++) {
        temp_suspect_set[i] = new int[size];
    }

    if (num_failures + 1 >= 3) {
        initThirdRound();
        convertToSet();
    }

    for(int round = 4; round <= num_failures + 1; round++) {
        initRoundR(round);
    }

    pretty_print();

    int final_value = extractDecision();
    printf("My %d final value is %d\n", rank, final_value);

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
    MPI_Gather(&myvalue, 1, MPI_INT, value_per_node, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Gather(&final_value, 1, MPI_INT, final_per_node, 1, MPI_INT, 0, MPI_COMM_WORLD);
    int total_messages = 0;
    int total_bits = 0;
    int total_value1 = 0;
    int total_value2 = 0;
    int final_value1 = 0;
    int final_value2 = 0;
    int value1 = 3, value2 = 4;
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

    return(0);
}
