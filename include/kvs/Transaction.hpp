//
// Created by Taras Lykhenko on 23/07/2020.
//

#ifndef ANNA_TRANSACTION_H
#define ANNA_TRANSACTION_H
#include <anna.pb.h>


class Transaction {
private:
    unsigned long long commitTimestamp;
    unsigned long long prepareTimestamp;
    std::vector<KeyTuple> _operations;

public:
    Transaction(unsigned long long  _prepareTimestamp, std::vector<KeyTuple> operations){
        prepareTimestamp = _prepareTimestamp;
        _operations = operations;
    }


    long getCommitTimestamp(){
        return commitTimestamp;
    }

    void setCommitTimestamp(unsigned long long _commitTimestamp){
        commitTimestamp = _commitTimestamp;
    }


    long getPrepareTimestamp(){
        return prepareTimestamp;
    }

    std::vector<KeyTuple> getOperations(){
        return _operations;
    }


};


#endif //ANNA_TRANSACTION_H
