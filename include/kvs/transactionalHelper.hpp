//
// Created by Taras Lykhenko on 22/07/2020.
//

#ifndef TRANSACTIONALHELPER_HPP_
#define TRANSACTIONALHELPER_HPP_

#include "kvs/Transaction.hpp"


class TransactionalHelper {
    private:
    std::vector<std::string> insertOrderPrepared;
    std::map<std::string, Transaction> prepared;
    std::map<unsigned long long, Transaction> committed;
    std::mutex mtx;
    unsigned long long hlc = static_cast<unsigned long long>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count());
    std::map<std::string, unsigned long long> vc;

    std::string partitionID;


public:
    /*TransactionalHelper(std::string partitionID);


    unsigned long long getCurrentTime();

    void updateHLC(unsigned long long int seenHLC);

    unsigned long long prepareTx(unsigned long long clientSeen,std::string txId, std::vector<KeyTuple> operations);

    unsigned long long commitTx(unsigned long long clientSeen, unsigned long long committedTime,std::string txId);

    std::vector<Transaction> writebackTx();

    unsigned long long getLocalVisible();

    void updateVC(std::string id, unsigned long long clockValue);

     */


    TransactionalHelper(std::string _partitionID){
        partitionID = _partitionID;
        updateVC(partitionID, getCurrentTime());
    }

    unsigned long long getCurrentTime(){
        return static_cast<unsigned long long>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count());
    }

    unsigned long long updateHLC(unsigned long long seenHLC){
        hlc = std::max({hlc+1, getCurrentTime(), seenHLC+1});
        return hlc;
    }

    unsigned long long prepareTx(unsigned long long clientSeen,std::string txId, std::vector<KeyTuple> operation){
        unsigned long long prepareTimestamp = updateHLC(clientSeen);
        insertOrderPrepared.push_back(txId);
        Transaction t{prepareTimestamp,operation};
        prepared.insert(std::pair<std::string, Transaction>(txId, t));
        return prepareTimestamp;
    }

    void commitTx(unsigned long long clientSeen, unsigned long long committedTime,std::string txId){
        updateHLC(clientSeen);
        if(prepared.find(txId) == prepared.end()){
          return;
        }
        Transaction t = prepared.at(txId);
        auto p = std::find(insertOrderPrepared.crbegin(), insertOrderPrepared.crend(), txId);
        insertOrderPrepared.erase(std::remove(insertOrderPrepared.begin(), insertOrderPrepared.end(), txId), insertOrderPrepared.end());
        prepared.erase(txId);
        t.setCommitTimestamp(committedTime);
        committed.insert(std::pair<unsigned long long, Transaction>(committedTime, t));
    }

    void updateVC(std::string id, unsigned long long clockValue){
        std::map<std::string , unsigned long long>::iterator it = vc.find(id);
        if (it != vc.end()) {
            it->second = std::max({it->second,clockValue});
        }else{
            vc.insert(std::pair<std::string, unsigned long long>(id, clockValue));
        }
    }

    void updateVC(unsigned long long clockValue){
        std::map<std::string , unsigned long long>::iterator it = vc.find(partitionID);
        if (it != vc.end()) {
            it->second = std::max({it->second,clockValue});
        }else{
            vc.insert(std::pair<std::string, unsigned long long>(partitionID, clockValue));
        }
    }
    unsigned long long getUB(){
        unsigned long long ub = 0;
        if(!insertOrderPrepared.empty()){
            ub = prepared.at(insertOrderPrepared.at(0)).getPrepareTimestamp();
            if(hlc-ub > 20000000){
              commitTx(0,getCurrentTime(),insertOrderPrepared.at(0));
            }
        }else{
            ub = std::max({hlc,getCurrentTime()});
        }
        return ub;
    }

    pair<unsigned long long, std::vector<Transaction>> writebackTx(){
        unsigned long long ub = getUB();
        std::vector<Transaction> commit;

        std::map<unsigned long long, Transaction>::iterator it;
        bool flag = false;
        for ( it = committed.begin(); it != committed.end() && it->first < ub ; it++ )
        {
            flag = true;
            commit.push_back(it->second);
        }
        if(flag){
            committed.erase(committed.begin(),it);
        }
        return pair<unsigned long long, std::vector<Transaction>>(ub,commit);
    }

    unsigned long long getLocalVisible(){
        std::map<std::string, unsigned long long>::iterator it;
        unsigned long long lv = std::numeric_limits<unsigned long long>::max();
        for ( it = vc.begin(); it != vc.end(); it++ )
        {
            lv = std::min({lv, it->second});
        }
        return lv;
    }

    std::string getPartitionID(){
        return partitionID;
    }


    };


#endif //ANNA_TRANSACTIONALHELPER_HPP_
