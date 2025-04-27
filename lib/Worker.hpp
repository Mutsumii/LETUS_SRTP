#pragma once

#include "DMMTrie.hpp"
#include "ElementPool.hpp"
class DeltaPage;

class Worker {
    public:
    BasePage* GetPage(const PageKey& pagekey);
    DeltaPage* GetDeltaPage(const string& pid);

    pair<uint64_t, uint64_t>  GetPageVersion(PageKey pagekey);
    void  UpdatePageVersion(PageKey pagekey, uint64_t current_version, uint64_t latest_basepage_version);

    void  PutPage(const PageKey& pagekey, BasePage* page);

    void  UpdatePageKey(const PageKey& old_pagekey, const PageKey& new_pagekey);
    void  WritePageCache(PageKey pagekey, Page* page);

    protected:
    std::map<PageKey, Page*> page_cache_;
    std::unordered_map<PageKey, std::list<std::pair<PageKey, BasePage*>>::iterator, PageKey::Hash> lru_cache_;
    list<pair<PageKey, BasePage*>> pagekeys_;  // list to maintain cache order
    std::unordered_map<std::string, DeltaPage> active_deltapages_;  // deltapage of all pages, delta pages are indexed by pid
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> page_versions_;  // current version, latest basepage version
    ElementPool<BasePage> pool_;
};