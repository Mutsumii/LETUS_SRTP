#pragma once

#include "DMMTrie.hpp"
#include "ElementPool.hpp"
#include "LSVPS.hpp"
class DeltaPage;

inline uint8_t GetNibbleValue(const string& key) {
    if (key.length() >= 2) {
        return (GetIndex(key[0])) * 16 + (GetIndex(key[1]));
    } else if (key.length() == 1) {
        return GetIndex(key[0]);
    }
    return 0;
}

class Worker {
    public:
    Worker(bool pool_init = false) {
        if (pool_init) {
            pool_.reserve();
        }
        page_pool_.reserve();
        pool_delta_.reserve();
    }
    ~Worker() {
        page_store_->Flush();
        for (auto& it : page_cache_) {
            if (it.second) {
                delete it.second;  // delete BasePage
            }
        }
        for (auto& it : active_deltapages_) {
            if (it.second) {
                delete it.second;  // delete DeltaPage
            }
        }
    }
    BasePage* GetPage(const PageKey& pagekey);
    BasePage* JoinerGetPage(const PageKey& pagekey);
    DeltaPage* GetDeltaPage(const string& pid);

    pair<uint64_t, uint64_t>  GetPageVersion(PageKey pagekey);
    void  UpdatePageVersion(PageKey pagekey, uint64_t current_version, uint64_t latest_basepage_version);

    void  PutPage(const PageKey& pagekey, BasePage* page);
    
    void  UpdatePageKey(const PageKey& old_pagekey, const PageKey& new_pagekey);
    void  WritePageCache(PageKey pagekey, Page* page);

    //新增
    PageKey GetLatestBasePageKey(PageKey pagekey) const;
    uint64_t GetVersionUpperbound(const string& pid, uint64_t version,Master* master);
    LSVPS* GetPageStore() { return page_store_; } //新增，获取LSVPS
    protected:
    LSVPS* page_store_;
    std::map<PageKey, Page*> page_cache_;
    std::unordered_map<PageKey, std::list<std::pair<PageKey, BasePage*>>::iterator, PageKey::Hash> lru_cache_; //缓存BasePage
    list<pair<PageKey, BasePage*>> pagekeys_;  // list to maintain cache order
    std::unordered_map<std::string, DeltaPage*> active_deltapages_;  // deltapage of all pages, delta pages are indexed by pid
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> page_versions_;  // 根据pagekey索引current version, latest basepage version
    ElementPool<BasePage> pool_;
    PagePool page_pool_;
    ElementPool<DeltaPage> pool_delta_;
};