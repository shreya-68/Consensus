
#include <algorithm>
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
using std::vector;
using std::ostream;

# include "mpi.h"

// stores the value log(size)
int quorum_size;
// Variables to store it's own process ID "rank" and the total number of
// nodes "size"
int rank;
int size;

map<int, set<int> > view;
map<int, set<int> > extView;

int final_value;
int num_failures;
int numOfBins;
int number_of_messages;
int number_of_bits;
int byzantine;

map<int, int> rand_strings;

vector<int> ids;
vector<int> combination;
vector<vector<int> > combinations;
set<int> elementof;


template<typename T>
ostream& operator<< (ostream& out, const vector<T>& v) {
    out << "[";
    size_t last = v.size() - 1;
    for(size_t i = 0; i < v.size(); ++i) {
                out << v[i];
                        if (i != last) 
                                        out << ", ";
                            }
        out << "]";
            return out;
}

template<typename T>
ostream& operator<< (ostream& out, const set<T>& v) {
    out << "[";
    size_t last = v.size() - 1;
    for(set<int>::const_iterator it = v.begin(); it != v.end(); it++) {
                out << *it;
    }
    out << "]";
            return out;
}

void intersection(set<int> set1, set<int>* set2) {
    for(set<int>::iterator it = set2->begin(); it != set2->end();) {
        if(set1.find(*it) != set1.end())
            ++it;
        else
            set2->erase(it++);
    }
}

// stores the gstring - the value agreed upon by majority of nodes
int getRandBit(int num) {
    sleep(rank);
    srand(time(NULL));
    return (rand() % num) + 1;
}

void go(int offset, int k, int in) {
  if (k == 0) {
    combinations.push_back(combination);
    //cout<<combination<<endl;
    if(in) {
        elementof.insert(combinations.size() - 1);
    }
    return;
  }
  for (int i = offset; i <= size - k; ++i) {
    combination.push_back(i);
    if(i == rank)
        go(i+1, k-1, 1);
    else
        go(i+1, k-1, in);
    combination.pop_back();
  }
}

int getAllCommittees() {

  go(0, 0.75*quorum_size, 0);

  return 0;
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

map<int, map<int, int> >* gradecast(int v) {
    MPI_Request req;
    MPI_Status stats;
    map<int, map<int, int> > step3;
    int step2[size][2];
    map<int, map<int, int> > step4;
    map<int, map<int, int> > *output = new map<int, map<int, int> >();

    int i;

    //---------------STEP1-----------------------
    for (i = 0; i < size; i++) {
        if (rank != i) {
            if(rank == 0)
                printf("I %d am SENDING to %d\n", rank, i);
            MPI_Send(&v, 1, MPI_INT, i, 1, MPI_COMM_WORLD);
	    	number_of_messages++;
	    	number_of_bits += 1;
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    //--------------STEP2----------------------------
    int rcvd_v;
    for (i = 0; i < size; i++) {
        rcvd_v = -1;
        if (rank != i) {
            MPI_Irecv(&rcvd_v, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &req);
        } else {
            rcvd_v = v;
        }
        if(rank == 0)
            printf("RECEIVED from %d - %d\n", i, rcvd_v);
        step2[i][0] = i;
        step2[i][1] = rcvd_v;
        step3[i][v]++;
    }

    for (i = 0; i < size; i++) {
        if (rank != i) {
            for (int j = 0; j < size; j++) {
                MPI_Send(step2[j], 2, MPI_INT, i, 2, MPI_COMM_WORLD);
	    	    number_of_messages++;
	    	    number_of_bits += 2;
            }
        }
    }
    //delete[] step2;

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    //--------------STEP3----------------------------
    int *recd_vij;
    for (i = 0; i < size; i++) {
        if(rank != i) {
            recd_vij = new int[2];
            recd_vij[0] = -1;
            MPI_Irecv(recd_vij, 2, MPI_INT, i, 2, MPI_COMM_WORLD, &req);
            while (recd_vij[0] != -1) {
                step3[recd_vij[0]][recd_vij[1]]++;
                delete[] recd_vij;
                recd_vij = new int[2];
                recd_vij[0] = -1;
                MPI_Irecv(recd_vij, 2, MPI_INT, i, 2, MPI_COMM_WORLD, &req);
            }
        }
    }

    for (i = 0; i < size; i++) {
        for(map<int, int>::iterator it_vals = step3[i].begin(); it_vals != step3[i].end(); it_vals++) {
            if (it_vals->second >= size - num_failures) {
                step4[i][it_vals->first] = 1;
                int val = it_vals->first;
                for (int j = 0; j < size; j++) {
                    if (rank != j) {
                        if(rank == 9 && j == 0)
                            printf("SENDING to %d this %d,%d\n", j, i, it_vals->first);
                        MPI_Send(&val, 1, MPI_INT, j, i, MPI_COMM_WORLD);
	    	            number_of_messages++;
	    	            number_of_bits += 1;
                    }
                }
                break;
            }
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    //--------------STEP4----------------------------
    int *recd_mu;
    int mu;
    for (i = 0; i < size; i++) {
        if(rank != i) {
            for(int j = 0; j < size; j++) {
                mu = -1;
                MPI_Irecv(&mu, 1, MPI_INT, i, j, MPI_COMM_WORLD, &req);
                while (mu != -1) {
                    if(rank == 0 && i == 9)
                        printf("RECEIVED from %d - %d, %d\n", i, j, mu);
                    step4[j][mu]++;
                    mu = -1;
                    MPI_Irecv(&mu, 1, MPI_INT, i, j, MPI_COMM_WORLD, &req);
                }
            }
        }
    }
    for (i = 0; i < size; i++) {
        int set = 1;
        for(map<int, int>::iterator it_vals = step4[i].begin(); it_vals != step4[i].end(); it_vals++) {
            if (it_vals->second >= num_failures*2 + 1) {
                (*output)[i][it_vals->first] = 2;
                if(rank == 0)
                    printf("SETTING for %d this %d,%d\n", i, it_vals->first, 2);
            } else if (it_vals->second >= num_failures + 1) {
                (*output)[i][it_vals->first] = 1;
                if(rank == 0)
                    printf("SETTING for %d this %d,%d\n", i, it_vals->first, 1);
            } else {
                set = 0;
            }
        }
        if (!set) {
            (*output)[i][-1] = 0;
            if(rank == 0)
                printf("SETTING for %d this %d,%d\n", i, -1, 0);
        }
    }
    return output;
}

int stage1() {

    int bin_number = getRandBit(numOfBins);
    printf("My %d bin number is %d \n", rank, bin_number);
    map<int, map<int, int> > *result = gradecast(bin_number);
    for (int i = 0; i < size; i++) {
        if(rank == i) {
            view[bin_number].insert(i);
            extView[bin_number].insert(i);
        } else {
            for(map<int, int>::iterator it_vals = (*result)[i].begin(); it_vals != (*result)[i].end(); it_vals++) {
                if (it_vals->second == 2) {
                    if(rank == 0)
                        printf("In my view of bin %d inserting %d\n", it_vals->first, i);
                    view[it_vals->first].insert(i);
                    extView[it_vals->first].insert(i);
                } else if (it_vals->second == 1) {
                    if(rank == 0)
                        printf("In my Extended view of bin %d inserting %d\n", it_vals->first, i);
                    extView[it_vals->first].insert(i);
                }
            }
        }
    }
    if(rank == 0)
        printf("Finished stage 1\n");
    return 1;

}

int find(int i, const vector<int>& comp) {
    for(vector<int>::const_iterator it = comp.begin(); it != comp.end(); it++) {
        if(i == *it)
            return 1;
    }
    return 0;
}

int subset(const vector<int>& sub, const set<int>& big) {
    for(vector<int>::const_iterator it = sub.begin(); it != sub.end(); it++) {
        if(big.find(*it) == big.end())
            return 0;
    }
    return 1;
}

set<int>* stage2() {
//void stage2() {
    MPI_Request req;
    MPI_Status stats;
    if(rank == 0) {
        printf("My external view is:\n");
    }

    //---------Send external view to all nodes----------
    for(map<int, set<int> >::iterator it_bins = extView.begin(); it_bins != extView.end(); it_bins++) {
        int *members = new int[size];
        for(set<int>::iterator in = it_bins->second.begin(); in != it_bins->second.end(); in++) {
            members[*in] = 1;
        }
        for (int i = 0; i < size; i++) {
            if (rank != i) {
                MPI_Send(members, size, MPI_INT, i, it_bins->first + size + 10, MPI_COMM_WORLD);
	    	    number_of_messages++;
	    	    number_of_bits += size;
            }
        }
        delete[] members;
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    map<int, map<int, set<int> > > collection;
    for(int bin = 1; bin <= numOfBins; bin++) {
        for (int i = 0; i < size; i++) {
            if (rank != i) {
                int flag;
                MPI_Iprobe(i, bin+size+10, MPI_COMM_WORLD, &flag, &stats);
                if(flag == true) {
                    int *members = new int[size];
                    MPI_Irecv(members, size, MPI_INT, i, size+bin+10, MPI_COMM_WORLD, &req);
                    for (int j = 0; j < size; j++) {
                        if(members[j] == 1)
                            collection[bin][i].insert(j);
                    }
                    delete[] members;
                }
            } else {
                collection[bin][i] = extView[bin];
            }
        }
    }

    //------------------Disqualify committees for each subset----------------
    
    getAllCommittees();
    if(rank == 0) {
        cout<<"I am in committees "<<elementof<<endl;
    }

    // for each subset -> maps to each bin with disqualification value 0 or 1
    map<int, map<int, int> > disq;
    map<int, vector<int> > notDisq;
    for(int bin = 1; bin <= numOfBins; bin++) {
        for(set<int>::iterator it_sj = elementof.begin(); it_sj != elementof.end(); it_sj++) {
            //For each subset we create an external view of all members of the
            //committee
            vector<int>::iterator it_member = combinations[*it_sj].begin();
            set<int> extOthers = collection[bin][*it_member];
            it_member++;
            for(;it_member != combinations[*it_sj].end(); it_member++) {
                intersection(collection[bin][*it_member], &extOthers);
            }
            //if(rank == 0) {
            //    cout<<"For the committee"<<combinations[*it_sj]<<" the external view is "<<extOthers<<" for bin "<<bin<<"Quorum size is"<<quorum_size<<endl;
            //}
            if (extOthers.size() > quorum_size) {
                disq[*it_sj][bin] = 1;
            } else {
                disq[*it_sj][bin] = 0;
                notDisq[*it_sj].push_back(bin);
            }
        }
        int *members = new int[size];
        if(rank == 0) {
            printf("Bin:%d\n", bin);
        }
        for(set<int>::iterator in = view[bin].begin(); in != view[bin].end(); in++) {
            members[*in] = 1;
            if(rank == 0) {
                printf("Member:%d\n", *in);
            }
        }
        for (int i = 0; i < size; i++) {
            if(rank != i) {
                if(rank == 10)
                    cout<<"sending "<<bin<<" members "<<endl;
                MPI_Send(members, size, MPI_INT, i, bin+size+20, MPI_COMM_WORLD);
	    	    number_of_messages++;
	    	    number_of_bits += size;
            }
        }
        delete[] members;
    }

    //// To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    map<int, map<int, set<int> > > viewCollection;
    for(int bin = 1; bin <= numOfBins; bin++) {
        for (int i = 0; i < size; i++) {
            if (rank != i) {
                int flag;
                MPI_Iprobe(i, bin+size+20, MPI_COMM_WORLD, &flag, &stats);
                if(flag == true) {
                    if (rank == 0)
                        cout<<"receiving members"<<endl;
                    int *members = new int[size];
                    MPI_Irecv(members, size, MPI_INT, i, bin+size+20, MPI_COMM_WORLD, &req);
                    for (int j = 0; j < size; j++) {
                        if(members[j] == 1){
                            viewCollection[bin][i].insert(j);
                            if(rank == 0)
                                cout<<"Member "<<i<<"has member "<<j<<" in bin "<<bin<<endl;
                        }
                    }
                    delete[] members;
                    if(rank == 0)
                        cout<<viewCollection[bin][i]<< " : "<<bin<<":"<<i<<endl;
                }
            } else {
                viewCollection[bin][i] = view[bin];
            }
        }
    }

    map<int, map<int, set<int> > > composition;
    for(map<int, vector<int> >::iterator it_view = notDisq.begin(); it_view != notDisq.end(); it_view++) {
        for(vector<int>::iterator it_bins = it_view->second.begin(); it_bins != it_view->second.end(); it_bins++) {
            vector<int>::iterator it_member = combinations[it_view->first].begin();
            set<int> viewOthers = viewCollection[*it_bins][*it_member];
            it_member++;
            for(;it_member != combinations[it_view->first].end(); it_member++) {
                if(rank == 0) {
                    cout<<"For member "<<*it_member<<" collection is"<<viewCollection[*it_bins][*it_member]<<endl;
                }
                intersection(viewCollection[*it_bins][*it_member], &viewOthers);
            }
            composition[*it_bins][it_view->first] = viewOthers;
        }
    }

    ////---------------------------------------------------------------------
    for(int bin = 1; bin <= numOfBins; bin++) {
        for(set<int>::iterator it_sj = elementof.begin(); it_sj != elementof.end(); it_sj++) {
            int *message = new int[size+3];
            message[0] = *it_sj;
            message[1] = bin;
            if(disq[*it_sj][bin] == 1) {
                message[2] = 1;
            } else {
                message[2] = 0;
                for(set<int>::iterator in = composition[bin][*it_sj].begin(); in != composition[bin][*it_sj].end(); in++) {
                    message[*in+3] = 1;
                }
            }
            for (int i = 0; i < size; i++) {
                if (rank != i) {
                    MPI_Send(message, size+3, MPI_INT, i, 300+bin, MPI_COMM_WORLD);
	    	        number_of_messages++;
	    	        number_of_bits += size+3;
                }
            }
            delete[] message;
        }
    }

    //// To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    ////---------------------------------------------------------------------
    map<int, set<int> > final_composition;
    set<int> final_disq;
    for(int bin = 1; bin <= numOfBins; bin++) {
        int disqCount = 0;
        map<int, map<set<int>, int> > comp;
        set<int> final_set;
        int final_set_size = 0;
        for (int i = 0; i < size; i++) {
            int *rcvd_msg;
            if(rank != i) {
                int flag = false;
                //MPI_Iprobe(i, 300+bin, MPI_COMM_WORLD, &flag, &stats);
                //while(flag == true) {
                rcvd_msg = new int[size+3];
                rcvd_msg[0] = -1;
                MPI_Irecv(rcvd_msg, size+3, MPI_INT, i, 300+bin, MPI_COMM_WORLD, &req);
                while(rcvd_msg[0] != -1) {
                    if(find(i, combinations[rcvd_msg[0]]) && subset(combinations[rcvd_msg[0]], view[rcvd_msg[1]])) {
                        //if(rank == 3) {
                        //    cout<<"View : "<<view[rcvd_msg[1]]<<"combination :"<<combinations[rcvd_msg[0]]<<endl;
                        //    cout<<"From "<<i<<"Yellow!"<<rcvd_msg[0]<<"::"<<rcvd_msg[1]<<":::"<<rcvd_msg[2]<<endl;
                        //}
                        if(rcvd_msg[2] == 1) {
                            ++disqCount;
                            //if(rank == 3) {
                            //    cout<<"Received disqualification message for :"<<bin<<"Count is "<<disqCount<<endl;
                            //}
                            if(disqCount >= quorum_size/2)
                                final_disq.insert(bin);
                        } else {
                            set<int> D;
                            for(int j = 3; j < size + 3; j++) {
                                if(rcvd_msg[j] == 1)
                                    D.insert(j-3);
                            }
                            ++comp[rcvd_msg[0]][D];
                            if(comp[rcvd_msg[0]][D] >= quorum_size/2) {
                                if(rank == 0)
                                    cout<<"comp is "<<D<<"count is"<<comp[rcvd_msg[0]][D]<<endl;
                                if(D.size() > final_set_size) {
                                    final_set = D;
                                    final_set_size = D.size();
                                }
                            }
                        }
                    }
                    delete[] rcvd_msg;
                    rcvd_msg = new int[size+3];
                    rcvd_msg[0] = -1;
                    MPI_Irecv(rcvd_msg, size+3, MPI_INT, i, 300+bin, MPI_COMM_WORLD, &req);
                }
                delete[] rcvd_msg;
            }
        }
        final_composition[bin] = final_set;
        if(rank == 3){
            cout<<"Final disq "<<final_disq<<endl;
            cout<<"Bin: "<<bin<<"Final composition "<<final_composition[bin]<<endl;
        }
    }
    
    for(set<int>::iterator it_bins = final_disq.begin(); it_bins != final_disq.end(); it_bins++) {
        final_composition.erase(*it_bins);
    }
    set<int> small_comm;
    int comm_size = size;
    int small_bin;
    for(map<int, set<int> >::iterator it_bins = final_composition.begin(); it_bins != final_composition.end(); it_bins++) {
        if(it_bins->second.size() < comm_size) {
            small_comm = it_bins->second;
            small_bin = it_bins->first;
            comm_size = small_comm.size();
        }
    }
    set<int> *small = new set<int>;
    for(set<int>::iterator it = small_comm.begin(); it != small_comm.end(); it++) {
        small->insert(*it);
    }
    cout<<"Smallest "<<small_comm<<endl;
    return small;
}

int leaderElection(set<int>* comm) {
    MPI_Request req;
    MPI_Status stats;
    if(rank == *comm->begin()) {
        final_value = getRandBit(2);
        for (int i = 0; i < size; i++) {
            if (rank != i)
                MPI_Send(&final_value, 1, MPI_INT, i, 1000, MPI_COMM_WORLD);
	    	    number_of_messages++;
	    	    number_of_bits += 1;
        }
        delete comm;
        return 1;
    }
    delete comm;
    return 0;
}

void getFinalValue() {
    MPI_Request req;
    int val;
    for (int i = 0; i < size; i++) {
        val = -1;
        if (rank != i) {
            MPI_Irecv(&val, 1, MPI_INT, i, 1000, MPI_COMM_WORLD, &req);
            if (val != -1) {
                final_value = val;
                break;
            }
        }
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

    numOfBins = size/quorum_size;

    num_failures = (size/4) - 1;
    num_failures = num_failures > 0? num_failures:0;

    number_of_messages = 0;
    number_of_bits = 0;

    if((rank % (size/num_failures) == 0) && rank != 0) {
        byzantine = 1;
    } else {
        byzantine = 0;
    }

    //-------------------------STAGE 1----------------------------------------
    stage1();

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    //-------------------------STAGE 2----------------------------------------
    set<int> *small_comm = stage2();
    //stage2();

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    if(small_comm->size() == 0)
        final_value = 1;
    else {
        int temp = 0;
        if(!leaderElection(small_comm))
            temp = 1;

        // To ensure all messages have been sent before receiving
        MPI_Barrier(MPI_COMM_WORLD);

        if(temp)
            getFinalValue();
    }

    printf("My %d final value is %d\n", rank, final_value);
    int *msg_per_node;
    int *bit_per_node;
    int *final_per_node;
    if(rank == 0) {
	    msg_per_node = new int[size];
	    bit_per_node = new int[size];
	    final_per_node = new int[size];
    }
    printf("%d's total messages = %d\n", rank, number_of_messages);
    MPI_Gather(&number_of_messages, 1, MPI_INT, msg_per_node, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Gather(&number_of_bits, 1, MPI_INT, bit_per_node, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Gather(&final_value, 1, MPI_INT, final_per_node, 1, MPI_INT, 0, MPI_COMM_WORLD);
    int total_messages = 0;
    int total_bits = 0;
    int final_value1 = 0;
    int final_value2 = 0;
    int value1 = 1, value2 = 2;
    if(rank == 0) {
	    for(int i = 0; i < size; i++) {
	        total_messages += msg_per_node[i];
	        total_bits += bit_per_node[i];
	        if(final_per_node[i] == value1)
	    	    final_value1++;
	        else
	    	    final_value2++;
	    }
	    printf("Total number of messages = %d \n Total number of bits = %d\n", total_messages, total_bits);
	    printf("Total number of final %d = %d \n Total number of final %d = %d\n", value1, final_value1, value2, final_value2);
    }
	delete[] msg_per_node;
	delete[] bit_per_node;
	delete[] final_per_node;


    MPI_Finalize();

    return(0);
}
