//  Copyright 2019 U.C. Berkeley RISE Lab
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  Modifications copyright (C) 2021 Taras Lykhenko, Rafael Soares
#include "kvs/kvs_handlers.hpp"

void user_request_handler(
    unsigned &access_count, unsigned &seed, string &serialized, logger log,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, vector<PendingRequest>> &pending_requests,
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
    map<Key, KeyProperty> &stored_key_map,
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, SerializerMap &serializers, SocketCache &pushers, TransactionalHelper &th) {
  KeyRequest request;
  request.ParseFromString(serialized);

  KeyResponse response;
  string response_id = request.request_id();
  response.set_response_id(request.request_id());

  response.set_type(request.type());
  std::vector<KeyTuple> localPrepareOperations;


  bool succeed;
  if(request.lastseen()){
    th.updateHLC(request.lastseen());
  }
  RequestType request_type = request.type();
  unsigned long long readAt = th.getLocalVisible();

  if(request.readat()){
      readAt = request.readat();
  }
  string response_address = request.response_address();



  for (const auto &tuple : request.tuples()) {
    // first check if the thread is responsible for the key
    if(request_type == RequestType::COMMIT){
        log->info("Committing key {}", tuple.key());
        continue;
    }
    Key key = tuple.key();
    string payload = tuple.payload();
    unsigned long long t_low = std::numeric_limits<unsigned long long>::min();
    if(tuple.t_low()){
      t_low = tuple.t_low();
    }



    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.replication_response_connect_address(), key, is_metadata(key),
        global_hash_rings, local_hash_rings, key_replication_map, pushers,
        kSelfTierIdVector, succeed, seed);

    if (succeed) {
      if (std::find(threads.begin(), threads.end(), wt) == threads.end()) {
        if (is_metadata(key)) {
          // this means that this node is not responsible for this metadata key
          KeyTuple *tp = response.add_tuples();

          tp->set_key(key);
          tp->set_lattice_type(tuple.lattice_type());
          tp->set_error(AnnaError::WRONG_THREAD);
        } else {
          // if we don't know what threads are responsible, we issue a rep
          // factor request and make the request pending
          kHashRingUtil->issue_replication_factor_request(
              wt.replication_response_connect_address(), key,
              global_hash_rings[Tier::MEMORY], local_hash_rings[Tier::MEMORY],
              pushers, seed);

          pending_requests[key].push_back(
              PendingRequest(request_type, tuple.lattice_type(), payload,
                             response_address, response_id));
        }
      } else { // if we know the responsible threads, we process the request
        KeyTuple *tp = response.add_tuples();
        tp->set_key(key);
        log->info("Got request for key {}", key);
        if(request_type == RequestType::PREPARE) {
            log->info("Preparing key {}", key);
            KeyTuple k{tuple};
            localPrepareOperations.push_back(k);
        }
        else if (request_type == RequestType::GET) {
          if (stored_key_map.find(key) == stored_key_map.end() ||
              stored_key_map[key].type_ == LatticeType::NONE) {

              log->info("Key {} does not exist", key);

            tp->set_error(AnnaError::KEY_DNE);
          } else {
              log->info("Getting key {}", key);

              auto res = process_get(key, serializers[stored_key_map[key].type_],t_low,readAt);
            if(stored_key_map[key].type_ == LatticeType::WREN){
              tp->set_lattice_type(LatticeType::LWW);
                LWWPairLattice<string> lattice = deserialize_lww(res.first);
                tp->set_t_low(lattice.reveal().timestamp);
                tp->set_t_high(lattice.reveal().promise);
                log->info("Key {} has t_low {} and t_high {}", key, lattice.reveal().timestamp, lattice.reveal().promise);
            }else{
              tp->set_lattice_type(stored_key_map[key].type_);
            }
            tp->set_payload(res.first);
            tp->set_error(res.second);
          }
        } else if (request_type == RequestType::PUT) {
          if (tuple.lattice_type() == LatticeType::NONE) {
            log->error("PUT request missing lattice type.");
          } else if (stored_key_map.find(key) != stored_key_map.end() &&
                     stored_key_map[key].type_ != LatticeType::NONE &&
                     stored_key_map[key].type_ != tuple.lattice_type()) {
            log->error(
                "Lattice type mismatch for key {}: query is {} but we expect "
                "{}.",
                key, LatticeType_Name(tuple.lattice_type()),
                LatticeType_Name(stored_key_map[key].type_));
          } else {
              log->info("Put request for key {}", key);

              process_put(key, tuple.lattice_type(), payload,
                        serializers[tuple.lattice_type()], stored_key_map);

            local_changeset.insert(key);
            tp->set_lattice_type(tuple.lattice_type());
          }
        } else {
          log->error("Unknown request type {} in user request handler.",
                     request_type);
        }

        if (tuple.address_cache_size() > 0 &&
            tuple.address_cache_size() != threads.size()) {
          tp->set_invalidate(true);
        }

        key_access_tracker[key].insert(std::chrono::system_clock::now());
        access_count += 1;
      }
    } else {
        log->info("Pending for key {}", key);
      pending_requests[key].push_back(
          PendingRequest(request_type, tuple.lattice_type(), payload,
                         response_address, response_id));
    }
  }

  if(!localPrepareOperations.empty()){
      long preparedTime = th.prepareTx(request.lastseen(),request.tx_id(),localPrepareOperations);
      response.set_preparedtime(preparedTime);
  }

  if(request_type == RequestType::COMMIT){
      th.commitTx(request.lastseen(), request.committime(), request.tx_id());
  }
    response.set_lastseen(th.getLocalVisible());
    response.set_tx_id(request.tx_id());
  if (response.tuples_size() > 0 && request.response_address() != "") {
      log->info("Responding to client {}", request.response_address());

      string serialized_response;
    response.SerializeToString(&serialized_response);
    kZmqUtil->send_string(serialized_response,
                          &pushers[request.response_address()]);
  }
}
