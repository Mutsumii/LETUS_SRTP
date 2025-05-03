#include <iostream>
#include <chrono>
#include "DMMTrie.hpp"
// #include "LSVPS.hpp"
#include "VDLS.hpp"
#include <cassert>
#include <random>

std::vector<int> generateRandomNumbers(int count) {
    std::vector<int> numbers;
    // unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    unsigned seed = 0;
    std::mt19937 generator(seed);
    std::uniform_int_distribution<int> distribution(10000000, 99999999);
    
    for (int i = 0; i < count; ++i) {
        numbers.push_back(distribution(generator));
    }
    
    return numbers;
}

inline char RandomPrintChar() { return rand() % 94 + 33; }

typedef struct KVPair
{
    std::string key;
    std::string value;
} KVPair;


int main() {
    std::string index_path = "/home/xuwenhao/DMMTree/";
    // LSVPS* page_store = new LSVPS(index_path);
    std::string data_path;
    data_path = "/home/xuwenhao/DMMTree/data/";//your own path
    VDLS* value_store = new VDLS(data_path);
    DMMTrie* trie = new DMMTrie(0, value_store);
    // page_store->RegisterTrie(trie);

    KVPair kvs[100000];
    int key_len = 5;
    // for (int i = 0; i < 50000; i++) {
    //     char buffer[20];  // 假设数字不会超过 20 位
    //     sprintf(buffer, "%d", i);  // 格式化输出，补零
    //     int j = 0;
    //     for (j = 0; j < key_len && buffer[j] != 0; j++);
    //     for(; j < key_len; j++){
    //         buffer[j] = '0';
    //     }
    //     buffer[key_len] = '\0'; // 确保字符串以 null 结尾
    //     kvs[i].key = std::string(buffer);
    //     kvs[i].value = "";
    //     kvs[i].value.append(32, RandomPrintChar());
    // }
    // {
    //     kvs[0] = { "00000", "00000000000000000000000000000000" };
    //     kvs[1] = { "11000", "11111111111111111111111111111111" };
    //     kvs[2] = { "22000", "22222222222222222222222222222222" };
    //     kvs[3] = { "33000", "00000000000000000000000000000000" };
    //     kvs[4] = { "00010", "11111111111111111111111111111111" };
    //     kvs[5] = { "22010", "22222222222222222222222222222222" };
    //     kvs[6] = { "00011", "11111111111111111111111111111111" };
    //     kvs[7] = { "22110", "22222222222222222222222222222222" };
    //     kvs[8] = { "66666", "11111111111111111111111111111111" };
    //     kvs[9] = { "33333", "22222222222222222222222222222222" };
    // }
    auto random_keys = generateRandomNumbers(20000);
    for (int i = 0; i < 20000; i++) {
        kvs[i].key = std::to_string(random_keys[i]);
        kvs[i].value = "";
        kvs[i].value.append(32, RandomPrintChar());
    }


    // std::cout << "KVPairs Generated." << std::endl;
    auto start = chrono::system_clock::now();
    // auto start = chrono::system_clock::now();
    constexpr int TEST_VERSION = 10;
    for (int ver = 1; ver < TEST_VERSION; ver++) {
        for (int i = 0; i < 20000; i++) {
            trie->Put(0, ver, kvs[i].key, kvs[i].value);
        }
        trie->Commit(ver);
    }
    // std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // start = chrono::system_clock::now();
    // end = chrono::system_clock::now();
    // duration = chrono::duration_cast<chrono::microseconds>(end - start);
    // double commit_latency = double(duration.count()) *
    // chrono::microseconds::period::num /
    //     chrono::microseconds::period::den;
    //     std::cout << "COMMIT: " << commit_latency << " s" << std::endl;
    // std::cout << trie->Get(0, TEST_VERSION - 1, kvs[8].key) << std::endl;
    // std::cout << kvs[8].value << std::endl;
    assert(trie->Get(0, TEST_VERSION - 1, kvs[8].key) == kvs[8].value);
    // DMMTrieProof merkle_proof = trie->GetProof(0, TEST_VERSION - 1, kvs[8].key);
    // for(int i = 0; i < merkle_proof.proofs.size(); i++) {
    //     std::cout << "Proof " << i << ": " << merkle_proof.proofs[i].index << std::endl;
    //     for (int j = 0; j < DMM_NODE_FANOUT; j++) {
    //         std::cout << "Sibling Hash " << j << ": " << HashToString(merkle_proof.proofs[i].sibling_hash[j]) << std::endl;
    //     }
    // }
    // std::cout << kvs[8].key;
    // std::cout << " " << trie->Get(0, 3, kvs[8].key);
    // std::cout << " <==> " << kvs[8].value.at(0) << std::endl;
    auto end = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
    double put_latency = double(duration.count()) *
    chrono::microseconds::period::num /
    chrono::microseconds::period::den;
    std::cout << "TOTAL: " << put_latency << " s" << std::endl;
    trie->Stop();
    return 0;
}