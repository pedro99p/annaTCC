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

#include <fstream>

#include "client/kvs_client.cpp"

#include "yaml-cpp/yaml.h"
#include <algorithm>
#include <assert.h>

unsigned kRoutingThreadCount;

ZmqUtil zmq_util;
ZmqUtilInterface *kZmqUtil = &zmq_util;

void print_set(set<string> set) {
    std::cout << "{ ";
    for (const string &val : set) {
        std::cout << val << " ";
    }

    std::cout << "}" << std::endl;
}

void handle_request(KvsClientInterface *client, string input) {
    vector<string> v;
    split(input, ' ', v);

    if (v.size() == 0) {
        std::exit(EXIT_SUCCESS);
    }

    if (v[0] == "GET") {
        client->get_async(v[1]);

        vector<KeyResponse> responses = client->receive_async();
        while (responses.size() == 0) {
            responses = client->receive_async();
        }

        if (responses.size() > 1) {
            std::cout << "Error: received more than one response" << std::endl;
        }

        assert(responses[0].tuples(0).lattice_type() == LatticeType::LWW);

        LWWPairLattice<string> lww_lattice =
                deserialize_lww(responses[0].tuples(0).payload());
        std::cout << lww_lattice.reveal().value << std::endl;
    }else if (v[0] == "WREN") {
        set<string> vec = {"1","2","3","4","5"};
        vector<KeyResponse> result;
        map<string,string> key_value = {{"1","a"},{"2","b"},{"3","c"},{"4","d"},{"5","e"}};
        string tx_id = client->get_request_id();
        bool flag = true;
        while(flag) {
            flag = false;
            auto address = client->get_worker_thread_multi(vec);
            if (address.size() == 0) {
                client->receive_async();
                flag = true;
            } else {
                auto ids = client->vput_async(address, key_value, LatticeType::WREN);
                while (ids.size() != 0) {
                    vector<KeyResponse> responses = client->receive_async();
                    for (KeyResponse response: responses) {
                        ids.erase(response.response_id());
                    }
                }
                unsigned long long commitTime = 0;
                ids = client->prepare_async(address, key_value, tx_id, LatticeType::WREN, nullptr);
                while (ids.size() != 0) {
                    vector<KeyResponse> responses = client->receive_async();
                    for (KeyResponse response: responses) {
                        unsigned long long preparedTime = response.preparedtime();
                        commitTime = std::max({commitTime, preparedTime});
                        ids.erase(response.response_id());
                    }
                }
                ids = client->commit_async(address, commitTime, tx_id, nullptr);
                while (ids.size() != 0) {
                    vector<KeyResponse> responses = client->receive_async();
                    for (KeyResponse response: responses) {
                        ids.erase(response.response_id());
                    }
                }
                ids = client->vget_async(address, commitTime, false, nullptr, "");

                while (ids.size() != 0) {
                    vector<KeyResponse> responses = client->receive_async();
                    for (KeyResponse response: responses) {
                        if (response.error() == 0) {
                            result.push_back(response);
                            ids.erase(response.response_id());
                        }
                    }
                }
            }
        }
        std::cout << "FIRST ROUND" << std::endl;
        unsigned long long maxCommitTimestamp = 0;
        vector<string> secondVec{};
        for( auto keyResponse : result){
            for( auto tuple : keyResponse.tuples()){
                TimestampValuePair<string> mkcl =
                        TimestampValuePair<string>(to_multi_key_wren_payload(
                            deserialize_multi_key_wren(tuple.payload())));
                maxCommitTimestamp = std::max({maxCommitTimestamp,mkcl.timestamp});
                std::cout << "{" << tuple.key() << " : "
                          << mkcl.value << ":" << std::to_string(mkcl.timestamp) << ":" << std::to_string(keyResponse.lastseen()) << "}" << std::endl;
            }
        }

        for( auto keyResponse : result){
            if(keyResponse.lastseen() < maxCommitTimestamp){
                for( auto tuple : keyResponse.tuples()){
                    secondVec.push_back(tuple.key());
                }
            }
        }

        if(secondVec.size() !=0 ){
            flag = true;
            while(flag) {
                flag = false;
                auto address = client->get_worker_thread_multi(vec);
                if(address.size() == 0){
                    client->receive_async();
                    flag = true;
                }

                auto ids = client->vget_async(address, maxCommitTimestamp, true,nullptr,"");
                while(ids.size()!=0){
                    vector<KeyResponse> responses = client->receive_async();
                    for(KeyResponse response: responses){
                        if(response.error() == 0) {
                            result.push_back(response);
                            ids.erase(response.response_id());
                        }
                    }
                }

                for( auto keyResponse : result){
                    for( auto tuple : keyResponse.tuples()){
                        TimestampValuePair<string> mkcl =
                                TimestampValuePair<string>(to_multi_key_wren_payload(
                                        deserialize_multi_key_wren(tuple.payload())));
                        maxCommitTimestamp = std::max({maxCommitTimestamp,mkcl.timestamp});

                    }
                }
            }

        }



    }
    else if (v[0] == "WREN2") {
      set<string> vec = {"1","2","3","4","5"};
      vector<KeyResponse> result;
      map<string,string> key_value = {{"1","a"},{"2","b"},{"3","c"},{"4","d"},{"5","e"}};
      string tx_id = client->get_request_id();
      auto missing_keys = client->get_missing_worker_threads_multi(vec);
      for(Key key : missing_keys) {
        set<Address> workers = client->get_all_worker_threads(key);
      }
      bool flag = true;
      while(flag) {
        flag = false;
        auto missing_keys = client->get_missing_worker_threads_multi(vec);
        if (missing_keys.size() != 0) {
          client->receive_async();
          flag = true;
        } else {
          auto address = client->get_worker_thread_multi(vec);
          auto ids = client->vput_async(address, key_value, LatticeType::WREN);
          while (ids.size() != 0) {
            vector<KeyResponse> responses = client->receive_async();
            for (KeyResponse response: responses) {
              ids.erase(response.response_id());
            }
          }
        }
      }
      client->writeTx(key_value);
      client->readTx(vec, 0, std::numeric_limits<unsigned long long>::max());
      while (result.size() == 0) {
        result = client->receive_async();
      }
      std::cout << "FIRST ROUND" << std::endl;
      unsigned long long maxCommitTimestamp = 0;
      vector<string> secondVec{};
      for( auto keyResponse : result){
        for( auto tuple : keyResponse.tuples()){
          TimestampValuePair<string> mkcl =
              TimestampValuePair<string>(to_multi_key_wren_payload(
                  deserialize_multi_key_wren(tuple.payload())));
          maxCommitTimestamp = std::max({maxCommitTimestamp,mkcl.timestamp});
          std::cout << "{" << tuple.key() << " : "
                    << mkcl.value << ":" << std::to_string(mkcl.timestamp) << ":" << std::to_string(keyResponse.lastseen()) << "}" << std::endl;
        }
      }

      while (true) {
        result = client->receive_async();
      }


    }
    else if (v[0] == "GET_CAUSAL") {
            // currently this mode is only for testing purpose
            client->get_async(v[1]);

            vector<KeyResponse> responses = client->receive_async();
            while (responses.size() == 0) {
                responses = client->receive_async();
            }

            if (responses.size() > 1) {
                std::cout << "Error: received more than one response" << std::endl;
            }

            assert(responses[0].tuples(0).lattice_type() == LatticeType::MULTI_CAUSAL);

            MultiKeyCausalLattice<SetLattice<string>> mkcl =
                    MultiKeyCausalLattice<SetLattice<string>>(to_multi_key_causal_payload(
                            deserialize_multi_key_causal(responses[0].tuples(0).payload())));

            for (const auto &pair : mkcl.reveal().vector_clock.reveal()) {
                std::cout << "{" << pair.first << " : "
                << std::to_string(pair.second.reveal()) << "}" << std::endl;
            }

            for (const auto &dep_key_vc_pair : mkcl.reveal().dependencies.reveal()) {
                std::cout << dep_key_vc_pair.first << " : ";
                for (const auto &vc_pair : dep_key_vc_pair.second.reveal()) {
                    std::cout << "{" << vc_pair.first << " : "
                    << std::to_string(vc_pair.second.reveal()) << "}"
                    << std::endl;
                }
            }

            std::cout << *(mkcl.reveal().value.reveal().begin()) << std::endl;
        }
    else if (v[0] == "PUT") {
        Key key = v[1];
        LWWPairLattice<string> val(
                TimestampValuePair<string>(generate_timestamp(0), v[2]));

        string rid = client->put_async(key, serialize(val), LatticeType::LWW);
        vector<KeyResponse> responses = client->receive_async();
        while (responses.size() == 0) {
            responses = client->receive_async();
        }

        KeyResponse response = responses[0];

        std::cout << "asd: " << response.error() << "\n";


        if (response.response_id() != rid) {
            std::cout << "Invalid response: ID did not match request ID!"
                      << std::endl;
        }
        if (response.error() == AnnaError::NO_ERROR) {
            std::cout << "Success!" << std::endl;
        } else {
            std::cout << "Failure!" << std::endl;
        }
    } else if (v[0] == "PUT_CAUSAL") {
        // currently this mode is only for testing purpose
        Key key = v[1];

        MultiKeyCausalPayload<SetLattice<string>> mkcp;
        // construct a test client id - version pair
        mkcp.vector_clock.insert("test", 1);

        // construct one test dependencies
        mkcp.dependencies.insert(
                "dep1", VectorClock(map<string, MaxLattice<unsigned>>({{"test1", 1}})));

        // populate the value
        mkcp.value.insert(v[2]);

        MultiKeyCausalLattice<SetLattice<string>> mkcl(mkcp);

        string rid =
                client->put_async(key, serialize(mkcl), LatticeType::MULTI_CAUSAL);

        vector<KeyResponse> responses = client->receive_async();
        while (responses.size() == 0) {
            responses = client->receive_async();
        }

        KeyResponse response = responses[0];

        if (response.response_id() != rid) {
            std::cout << "Invalid response: ID did not match request ID!"
                      << std::endl;
        }
        if (response.error() == AnnaError::NO_ERROR) {
            std::cout << "Success!" << std::endl;
        } else {
            std::cout << "Failure!" << std::endl;
        }
    } else if (v[0] == "PUT_SET") {
        set<string> set;
        for (int i = 2; i < v.size(); i++) {
            set.insert(v[i]);
        }

        string rid = client->put_async(v[1], serialize(SetLattice<string>(set)),
                                       LatticeType::SET);

        vector<KeyResponse> responses = client->receive_async();
        while (responses.size() == 0) {
            responses = client->receive_async();
        }

        KeyResponse response = responses[0];

        if (response.response_id() != rid) {
            std::cout << "Invalid response: ID did not match request ID!"
                      << std::endl;
        }
        if (response.error() == AnnaError::NO_ERROR) {
            std::cout << "Success!" << std::endl;
        } else {
            std::cout << "Failure!" << std::endl;
        }
    } else if (v[0] == "GET_SET") {
        client->get_async(v[1]);
        string serialized;

        vector<KeyResponse> responses = client->receive_async();
        while (responses.size() == 0) {
            responses = client->receive_async();
        }

        SetLattice<string> latt = deserialize_set(responses[0].tuples(0).payload());
        print_set(latt.reveal());
    } else {
        std::cout << "Unrecognized command " << v[0]
                  << ". Valid commands are GET, GET_SET, PUT, PUT_SET, PUT_CAUSAL, "
                  << "and GET_CAUSAL." << std::endl;
        ;
    }
}

void run(KvsClientInterface *client) {
    string input;
    while (true) {
        std::cout << "kvs> ";

        getline(std::cin, input);
        handle_request(client, input);
    }
}

void run(KvsClientInterface *client, string filename) {
    string input;
    std::ifstream infile(filename);

    while (getline(infile, input)) {
        handle_request(client, input);
    }
}

/*inline string serialize(const LWWPairLattice<string>& l) {
    LWWValue lww_value;
    lww_value.set_timestamp(l.reveal().timestamp); //ulonglong
    lww_value.set_value(l.reveal().value); // string
    lww_value.set_promise(l.reveal().promise); // ulonglong


    string serialized;
    lww_value.SerializeToString(&serialized);
    return serialized;
}*/

int main(int argc, char *argv[]) {
    if (argc < 2 || argc > 3) {
        std::cerr << "Usage: " << argv[0] << " conf-file <input-file>" << std::endl;
        std::cerr
                << "Filename is optional. Omit the filename to run in interactive mode."
                << std::endl;
        return 1;
    }

    // read the YAML conf
    YAML::Node conf = YAML::LoadFile(argv[1]);
    kRoutingThreadCount = conf["threads"]["routing"].as<unsigned>();

    YAML::Node user = conf["user"];
    Address ip = user["ip"].as<Address>();
    std::cerr << "IP: " << ip << "\n";

    vector<Address> routing_ips;
    if (YAML::Node elb = user["routing-elb"]) {
        routing_ips.push_back(elb.as<string>());
    } else {
        YAML::Node routing = user["routing"];
        for (const YAML::Node &node : routing) {
            routing_ips.push_back(node.as<Address>());
        }
    }

    std::cerr << "Routing IPs: " << routing_ips[0] << "\n";


    vector<UserRoutingThread> threads;
    for (Address addr : routing_ips) {
        for (unsigned i = 0; i < kRoutingThreadCount; i++) {
            threads.push_back(UserRoutingThread(addr, i));
        }
    }


    KvsClient client(threads, ip, 0, 10000);

    LWWValue lww_value;
    Address responseAddress;
    lww_value.set_timestamp(0); //ulonglong
    lww_value.set_value("THERE"); // string
    lww_value.set_promise(0); // ulonglong


    string serialized;
    lww_value.SerializeToString(&serialized);

    map<Key, string> key_map;

    key_map.insert(pair<Key, string>("HELLO", serialized));


        for (auto &p : key_map) {
                std::cout << p.first << " " << p.second << std::endl;
        }

    (&client)->writeTx(key_map);
    //responseAddress = request.response_address();
    vector<KeyResponse> responses = (&client)->receive_async();
    for (KeyResponse response: responses) {
        std::cout << "Response Id: " << response.response_id() << std::endl;
    }


    set<string> vec = {"HELLO"};
    vector<KeyResponse> result;


    (&client)->readTx(vec, 0, std::numeric_limits<unsigned long long>::max());
    std::cout << "1" << std::endl;
    while (result.size() == 0) {

        std::cout << "1.2: before" << std::endl;
        result = (&client)->receive_async();
        std::cout << "1.2: after" << std::endl;
    }
    std::cout << "2" << std::endl;
    std::cout << "FIRST ROUND" << std::endl;
    unsigned long long maxCommitTimestamp = 0;
    vector<string> secondVec{};
    for( auto keyResponse : result){
        for( auto tuple : keyResponse.tuples()){
            TimestampValuePair<string> mkcl = TimestampValuePair<string>(to_multi_key_wren_payload(deserialize_multi_key_wren(tuple.payload())));
            maxCommitTimestamp = std::max({maxCommitTimestamp,mkcl.timestamp});
            std::cout << "{" << tuple.key() << " : "
                    << mkcl.value << ":" << std::to_string(mkcl.timestamp) << ":" << std::to_string(keyResponse.lastseen()) << "}" << std::endl;
            std::cout << "Max Timestamp: " << maxCommitTimestamp << std::endl;
        }
    }

}