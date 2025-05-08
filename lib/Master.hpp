#pragma once

#include "parallel.hpp"
#include "Region.hpp"
#include "Joiner.hpp"
#include "VDLS.hpp"
#include "DMMTrie.hpp"
#include "third_party/parallel_hashmap/phmap.h"
#include "mutex"

class BufferItem;
class BasePage;
class DeltaPage;
class VDLS;

class Master {
    friend class Joiner;
    friend class Region;
    public:
    Master(VDLS* value_store);
    ~Master() {
        for (auto& region : regions_) {
            delete region;
        }
        delete joiner_;
    }

    string Get(uint64_t tid, uint64_t version, const string& key);
    void Put(uint64_t tid, uint64_t version, const string& key,
        const string& value);

    void Commit(uint64_t version);
    void LoadBalance();

    void AddDeltaPageVersion(const string& pid, uint64_t version);

    DMMTrieProof GetProof(uint64_t tid, uint64_t version,
        const string& key);
    void Stop();

    void WaitForCommit(uint64_t version);

    private:
    DMMTrie* trie_;
    // LSVPS* page_store_;
    VDLS* value_store_;
    const uint8_t MAX_REGION_NUM = 8;
    vector<Region*> regions_;
    Joiner* joiner_;
    vector<ConcurrentArray<pair<uint64_t, list<BufferItem>>>> bottomup_buffers_;
    // std::unordered_map<int8_t, uint8_t> nibble_dict_;
    uint8_t nibble_dict_[256];
    NibbleBucket* nibble_buckets_[256];
    uint8_t next_region_ = 0;
    // const size_t max_cache_size_ = 32;          // maximum pages in cache
    std::unordered_map<string, vector<uint64_t>>
        deltapage_versions_;  // the versions of deltapages for every pid
    uint64_t current_version_;
    // size_t region_workload_[256] = {0};

    void PrintLog(const string& log) {
        std::string logmsg = std::string(RED) + "[ Master ] " + log + RESET + "\n";
        std::cout << logmsg;
    }
};