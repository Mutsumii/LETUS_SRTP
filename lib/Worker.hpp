#pragma once

#include "DMMTrie.hpp"
#include "ElementPool.hpp"
class DeltaPage;

inline uint8_t GetNibbleValue(const string& key) {
    if (key.length() >= 2) {
        return (GetIndex(key[0])) * 16 + (GetIndex(key[1]));
    } else if (key.length() == 1) {
        return GetIndex(key[0]);
    }
    return 0;
}

class NibbleBucket {
    public:
    NibbleBucket(uint8_t nibble_value=0) :
        nibble_value_(nibble_value) {
    }
    BasePage* GetPage(const PageKey& pagekey);
    DeltaPage* GetDeltaPage(const string& pid);

    const pair<uint64_t, uint64_t> & GetPageVersion(PageKey pagekey);
    void  UpdatePageVersion(PageKey pagekey, uint64_t current_version, uint64_t latest_basepage_version);

    void  PutPage(const PageKey& pagekey, BasePage* page);

    void  UpdatePageKey(const PageKey& old_pagekey, const PageKey& new_pagekey);
    void  WritePageCache(PageKey pagekey, Page* page);
    inline void SetOwnerRegion(uint8_t region_id) {
        owner_region_id_ = region_id;
    }
    inline uint8_t GetOwnerRegion() {
        return owner_region_id_;
    }
    inline uint64_t GetAccessCount() {
        return access_count_;
    }
    inline size_t GetPageKeySize(){
        return lru_cache_.size();
    }

    BasePage* AllocPage() {
        return pool_.allocate();
    }
    void UpdateMasterNibbleBucket() {
        // std::cout << "NibbleValue: " << nibble_value_ << std::endl;
        // std::cout << to_string(reinterpret_cast<uintptr_t>(master_nibble_bucket_)) + "update " + to_string(nibble_value_) << std::endl;
        
        master_nibble_bucket_[nibble_value_] = this;
    }

    static NibbleBucket** master_nibble_bucket_;

    protected:
    uint8_t nibble_value_;
    uint64_t access_count_{ 0 };
    std::map<PageKey, Page*> page_cache_;
    std::unordered_map<PageKey, std::list<std::pair<PageKey, BasePage*>>::iterator, PageKey::Hash> lru_cache_;
    list<pair<PageKey, BasePage*>> pagekeys_;  // list to maintain cache order
    std::unordered_map<std::string, DeltaPage> active_deltapages_;  // deltapage of all pages, delta pages are indexed by pid
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> page_versions_;  // current version, latest basepage version
    ElementPool<BasePage> pool_;
    ElementPool<DeltaPage> pool_delta_;
    uint64_t workload_{ 0 };
    uint8_t owner_region_id_;
};

class Worker {
    public:
    BasePage* GetPage(const PageKey& pagekey);
    DeltaPage* GetDeltaPage(const string& pid);

    pair<uint64_t, uint64_t>  GetPageVersion(PageKey pagekey);
    void  UpdatePageVersion(PageKey pagekey, uint64_t current_version, uint64_t latest_basepage_version);

    void  PutPage(const PageKey& pagekey, BasePage* page);

    void  UpdatePageKey(const PageKey& old_pagekey, const PageKey& new_pagekey);
    void  WritePageCache(PageKey pagekey, Page* page);
    
    NibbleBucket* GetNibbleBucket(uint8_t nibble_value) {
        // if (nibble_buckets_[nibble_value] == nullptr) {
        //     throw std::runtime_error("Nibble bucket not initialized");
        // }
        // bool waited = false;
        // if (nibble_buckets_[nibble_value] == nullptr) {
        //     waited = true;
        // }
        while (nibble_buckets_[nibble_value] == nullptr) {
            // waited = true;
            // std::cout << "Waiting for NibbleBucket " << nibble_value << std::endl;
            std::this_thread::yield();  // wait for the nibble bucket to be initialized
        }
        // if (waited) {
        //     std::cout << "Waited for NibbleBucket " << nibble_value << std::endl;
        //     // nibble_buckets_[nibble_value]->UpdateMasterNibbleBucket();
        // }
        // if (nibble_buckets_[nibble_value])
        // return nibble_buckets_[nibble_value].get();
        return nibble_buckets_[nibble_value];
        // else
        //     return nullptr;
    }

    protected:
    // std::unique_ptr<NibbleBucket> nibble_buckets_[256];
    NibbleBucket* nibble_buckets_[256];
    std::map<PageKey, Page*> page_cache_;
    std::unordered_map<PageKey, std::list<std::pair<PageKey, BasePage*>>::iterator, PageKey::Hash> lru_cache_;
    list<pair<PageKey, BasePage*>> pagekeys_;  // list to maintain cache order
    std::unordered_map<std::string, DeltaPage*> active_deltapages_;  // deltapage of all pages, delta pages are indexed by pid
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> page_versions_;  // current version, latest basepage version
    ElementPool<BasePage> pool_;
};