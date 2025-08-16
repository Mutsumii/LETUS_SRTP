#pragma once

#include "parallel.hpp"
#include "Region.hpp"
#include "Joiner.hpp"
#include "VDLS.hpp"
#include "DMMTrie.hpp"
#include "third_party/parallel_hashmap/phmap.h"
#include "mutex"
#include "LSVPS.hpp"

class BufferItem;
class BasePage;
class DeltaPage;
class VDLS;

class Master {
    friend class Joiner;
    friend class Region;
public:
    Master(std::string data_path, size_t max_region_num = 8);
    ~Master() {
        delete joiner_;
        for (auto& region : regions_) {
            delete region;
        }
        delete value_store_;
    }

    string Get(uint64_t tid, uint64_t version, const string& key);
    void Put(uint64_t tid, uint64_t version, const string& key,
        const string& value);

    void Commit(uint64_t version);
    void LoadBalance();
    VDLS* GetValueStore() {
        return value_store_;
    }
    std::unordered_map<string, vector<uint64_t>> GetDeltaPageVersions() {
        return deltapage_versions_;
    }
    void AddDeltaPageVersion(const string& pid, uint64_t version);

    DMMTrieProof GetProof(uint64_t tid, uint64_t version,
        const string& key);
    void Stop();

    void WaitForCommit(uint64_t version);
    void Flush();
private:
    //DMMTrie* trie_;
    //LSVPS* page_store_;
    VDLS* value_store_;
    const uint8_t MAX_REGION_NUM = 1;
    vector<Region*> regions_;
    Joiner* joiner_;
    vector<ConcurrentArray<pair<uint64_t, list<BufferItem>>>> bottomup_buffers_;
    // std::unordered_map<int8_t, uint8_t> nibble_dict_;
    uint8_t nibble_dict_[256];
    uint8_t next_region_ = 0;
    // const size_t max_cache_size_ = 32;          // maximum pages in cache
    std::unordered_map<string, vector<uint64_t>>
        deltapage_versions_;  // the versions of deltapages for every pid
    uint64_t current_version_;
    // size_t region_workload_[256] = {0};
    void PrintLog(const string& log) {
        std::string logmsg = std::string(RED) + "[ Master ] " + log + RESET + "\n";
        int fd = open("/home/zz/LETUS_SRTP/gantt.txt", O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (fd == -1) {
        // 输出错误信息到标准错误
        std::cerr << "Failed to open log file: " << strerror(errno) << std::endl;
        return;
        }
    
        ssize_t written = write(fd, logmsg.c_str(), logmsg.size());
        if (written == -1) {
            std::cerr << "Failed to write log: " << strerror(errno) << std::endl;
        }

        close(fd);
    }
};