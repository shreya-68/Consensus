
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

map<int, int> rand_strings;

vector<int> ids;
vector<int> combination;
vector<vector<int> > combinations;
set<int> elementof;

// stores the gstring - the value agreed upon by majority of nodes
int getRandBit(int num) {
    sleep(rank*2);
    srand(time(NULL));
    return (rand() % num) + 1;
}

void go(int offset, int k, int in) {
  if (k == 0) {
    combinations.push_back(combination);
    if(in)
        elementof.insert(combinations.size() - 1);
    return;
  }
  for (int i = offset; i <= ids.size() - k; ++i) {
    combination.push_back(ids[i]);
    if(ids[i] == rank)
        go(i+1, k-1, 1);
    else
        go(i+1, k-1, in);
    combination.pop_back();
  }
}

int getAllCommittees() {

  for (int i = 0; i < size; ++i) { ids.push_back(i+1); }
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
    map<int, map<int, int> > step3;
    int step2[size][size];
    map<int, map<int, int> > step4;
    map<int, map<int, int> > *output = new map<int, map<int, int> >();

    int i;

    //---------------STEP1-----------------------
    for (i = 0; i < size; i++) {
        if (rank != i) {
            if(rank == 0)
                printf("I %d am SENDING to %d\n", rank, i);
            MPI_Isend(&v, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &req);
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
        }
        step2[i][0] = i;
        step2[i][1] = v;
        step3[i][v]++;
    }

    for (i = 0; i < size; i++) {
        if (rank != i) {
            for (int j = 0; j < size; j++) {
                if(rank == 0)
                    printf("I %d am SENDING to %d this %d,%d\n", rank, i, step2[j][0], step2[j][1]);
                MPI_Isend(step2[j], 2, MPI_INT, i, 2, MPI_COMM_WORLD, &req);
            }
        }
    }

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
                recd_vij = new int[2];
                recd_vij[0] = -1;
                MPI_Irecv(recd_vij, 2, MPI_INT, i, 2, MPI_COMM_WORLD, &req);
            }
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    for (i = 0; i < size; i++) {
        int sent = 0;
        for(map<int, int>::iterator it_vals = step3[i].begin(); it_vals != step3[i].end(); it_vals++) {
            if (it_vals->second >= size - num_failures) {
                int message[2] = {i, it_vals->first};
                step4[i][it_vals->first] = 1;
                for (int j = 0; j < size; j++) {
                    if (rank != j) {
                        if(rank == 0)
                            printf("I %d am SENDING to %d this %d,%d\n", rank, j, message[0], message[1]);
                        MPI_Isend(message, 2, MPI_INT, j, 3, MPI_COMM_WORLD, &req);
                    }
                }
                sent = 1;
                break;
            }
        }
        if (!sent) {
            //int message[2] = {i, -1};
            //step4[i][-1] = 1;
            //for (int j = 0; j < size; j++) {
            //    if (rank != j) {
            //        if(rank == 0)
            //            printf("I %d am SENDING to %d this %d,%d\n", rank, j, message[0], message[1]);
            //        MPI_Isend(message, 2, MPI_INT, j, 3, MPI_COMM_WORLD, &req);
            //    }
            //}
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    //--------------STEP4----------------------------
    int *recd_mu;
    for (i = 0; i < size; i++) {
        if(rank != i) {
            recd_mu = new int[2];
            recd_mu[0] = -1;
            MPI_Irecv(recd_mu, 2, MPI_INT, i, 3, MPI_COMM_WORLD, &req);
            while (recd_mu[0] != -1) {
                step4[recd_mu[0]][recd_mu[1]]++;
                recd_mu = new int[2];
                recd_mu[0] = -1;
                MPI_Irecv(recd_mu, 2, MPI_INT, i, 3, MPI_COMM_WORLD, &req);
            }
        }
    }
    for (i = 0; i < size; i++) {
        int set = 0;
        for(map<int, int>::iterator it_vals = step4[i].begin(); it_vals != step4[i].end(); it_vals++) {
            if (it_vals->second >= num_failures*2 + 1) {
                (*output)[i][it_vals->first] = 2;
            } else if (it_vals->second >= num_failures + 1) {
                (*output)[i][it_vals->first] = 1;
            }
            set = 1;
        }
        if (!set) {
            (*output)[i][-1] = 0;
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
                    view[it_vals->first].insert(i);
                    extView[it_vals->first].insert(i);
                } else if (it_vals->second == 1) {
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
    set<int> subset(sub.begin(), sub.end());
    return includes(subset.begin(), subset.end(), big.begin(), big.end());
}

set<int>* stage2() {
    MPI_Request req;
    if(rank == 0) {
        printf("My external view is:\n");
    }
    for(map<int, set<int> >::iterator it_bins = extView.begin(); it_bins != extView.end(); it_bins++) {
        int *members = new int[size];
        if(rank == 0) {
            printf("Bin:%d\n", it_bins->first);
        }
        for(set<int>::iterator in = it_bins->second.begin(); in != it_bins->second.end(); in++) {
            members[*in] = 1;
            if(rank == 0) {
                printf("Member:%d\n", *in);
            }
        }
        for (int i = 0; i < size; i++) {
            if (rank != i) {
                if(rank == 0)
                    printf("SENDING to %d Stage2, extView of bin %d \n", i, it_bins->first);
                MPI_Isend(members, size, MPI_INT, i, it_bins->first, MPI_COMM_WORLD, &req);
            }
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    map<int, map<int, set<int> > > collection;
    for(int bin = 1; bin <= numOfBins; bin++) {
        for (int i = 0; i < size; i++) {
            int *members = new int[size];
            if (rank != i) {
                MPI_Irecv(members, size, MPI_INT, i, bin, MPI_COMM_WORLD, &req);
                for (int j = 0; j < size; j++) {
                    if(members[j])
                        collection[bin][i].insert(j);
                }
            } else {
                collection[bin][i] = extView[bin];
            }
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    map<int, map<int, int> > disq;
    map<int, vector<int> > notDisq;
    set<pair<int, int> > sent;
    for(int bin = 1; bin <= numOfBins; bin++) {
        for(set<int>::iterator it_sj = elementof.begin(); it_sj != elementof.end(); it_sj++) {
            set<int> extOthers;
            for(vector<int>::iterator it_member = combinations[*it_sj].begin(); it_member != combinations[*it_sj].end(); it_member++) {
               extOthers.insert(collection[bin][*it_member].begin(), collection[bin][*it_member].end());
            }
            if (extOthers.size() > quorum_size) {
                disq[*it_sj][bin] = 1;
            } else {
                disq[*it_sj][bin] = 0;
                notDisq[*it_sj].push_back(bin);
                int *members = new int[size];
                for(set<int>::iterator in = view[bin].begin(); in != view[bin].end(); in++) {
                    members[*in] = 1;
                }
                for(vector<int>::iterator it_member = combinations[*it_sj].begin(); it_member != combinations[*it_sj].end(); it_member++) {
                    if (rank != *it_member && sent.find(make_pair(*it_member, bin)) == sent.end()) {
                        MPI_Isend(members, size, MPI_INT, *it_member, bin, MPI_COMM_WORLD, &req);
                        sent.insert(make_pair(*it_member, bin));
                    }
                }
            }
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    map<int, map<int, set<int> > > viewCollection;
    for(int bin = 1; bin <= numOfBins; bin++) {
        for (int i = 0; i < size; i++) {
            int *members = new int[size];
            if (rank != i) {
                MPI_Irecv(members, size, MPI_INT, i, bin, MPI_COMM_WORLD, &req);
                for (int j = 0; j < size; j++) {
                    if(members[j])
                        viewCollection[bin][i].insert(j);
                }
            } else {
                viewCollection[bin][i] = view[bin];
            }
        }
    }

    map<int, map<int, set<int> > > composition;
    for(map<int, vector<int> >::iterator it_view = notDisq.begin(); it_view != notDisq.end(); it_view++) {
        for(vector<int>::iterator it_member = combinations[it_view->first].begin(); it_member != combinations[it_view->first].end(); it_member++) {
            for(vector<int>::iterator it_bins = it_view->second.begin(); it_bins != it_view->second.end(); it_bins++) {
               composition[*it_bins][it_view->first].insert(viewCollection[*it_bins][*it_member].begin(), viewCollection[*it_bins][*it_member].end());
            }
        }
    }

    //---------------------------------------------------------------------
    for(int bin = 1; bin <= numOfBins; bin++) {
        for(set<int>::iterator it_sj = elementof.begin(); it_sj != elementof.end(); it_sj++) {
            int *message = new int[size+3];
            message[0] = *it_sj;
            message[1] = bin;
            if(disq[*it_sj][bin] == 1) {
                message[2] = 1;
                for(set<int>::iterator in = composition[bin][*it_sj].begin(); in != composition[bin][*it_sj].end(); in++) {
                    message[*in+3] = 1;
                }
            } else {
                message[2] = 0;
            }
            for (int i = 0; i < size; i++) {
                if (rank != i) {
                    MPI_Isend(message, size+3, MPI_INT, i, bin, MPI_COMM_WORLD, &req);
                }
            }
        }
    }

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    //---------------------------------------------------------------------
    map<int, set<int>* > final_composition;
    set<int> final_disq;
    for(int bin = 1; bin <= numOfBins; bin++) {
        map<int, int> disqCount;
        map<int, map<set<int>, int> > comp;
        set<int> *final_set;
        for (int i = 0; i < size; i++) {
            int *rcvd_msg;
            if(rank != i) {
                rcvd_msg = new int[size+3];
                rcvd_msg[0] = -1;
                MPI_Irecv(rcvd_msg, size+3, MPI_INT, i, bin, MPI_COMM_WORLD, &req);
                while (rcvd_msg[0] != -1) {
                    if(find(i, combinations[rcvd_msg[0]]) && subset(combinations[rcvd_msg[0]], view[rcvd_msg[0]])) {
                        if(rcvd_msg[2] == 1) {
                            if(++disqCount[rcvd_msg[0]] >= quorum_size/2)
                                final_disq.insert(bin);
                        } else {
                            set<int> *D = new set<int>;
                            for(int j = 3; j < size + 3; j++) {
                                if(rcvd_msg[j] == 1)
                                    D->insert(j-3);
                            }
                            if(++comp[rcvd_msg[0]][*D] >= quorum_size/2) {
                                if(D->size() > final_set->size())
                                    final_set = D;
                            }
                        }
                    }
                    rcvd_msg = new int[size+3];
                    rcvd_msg[0] = -1;
                    MPI_Irecv(rcvd_msg, size+3, MPI_INT, i, bin, MPI_COMM_WORLD, &req);
                }
            }
        }
        final_composition[bin] = final_set;
    }
    
    for(set<int>::iterator it_bins = final_disq.begin(); it_bins != final_disq.end(); it_bins++) {
        final_composition.erase(*it_bins);
    }
    set<int> *small_comm;
    int comm_size = size;
    int small_bin;
    for(map<int, set<int>* >::iterator it_bins = final_composition.begin(); it_bins != final_composition.end(); it_bins++) {
        if(it_bins->second->size() < comm_size) {
            small_comm = it_bins->second;
            small_bin = it_bins->first;
            comm_size = small_comm->size();
        }
    }
    return small_comm;
}

int leaderElection(set<int>* comm) {
    MPI_Request req;
    if(rank == *comm->begin()) {
        final_value = getRandBit(2);
        for (int i = 0; i < size; i++) {
            if (rank != i)
                MPI_Isend(&final_value, 1, MPI_INT, i, -2, MPI_COMM_WORLD, &req);
        }
        return 1;
    }
    return 0;
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

    //-------------------------STAGE 1----------------------------------------
    stage1();

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    //-------------------------STAGE 2----------------------------------------
    set<int> *small_comm = stage2();

    // To ensure all messages have been sent before receiving
    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Request req;
    if(!leaderElection(small_comm)) {
        int val;
        for (int i = 0; i < size; i++) {
            val = -1;
            if (rank != i) {
                MPI_Irecv(&val, 1, MPI_INT, i, -2, MPI_COMM_WORLD, &req);
                if (val != -1) {
                    final_value = val;
                    break;
                }
            }
        }
    }

    printf("My %d final value is %d\n", rank, final_value);


    MPI_Finalize();

    return(0);
}
