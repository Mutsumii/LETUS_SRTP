#include "Worker.hpp"

BasePage* Worker::GetPage(
    const PageKey& pagekey) {  // get a page by its pagekey
    // std::lock_guard<std::mutex> lock(pagekey_mutex_);
    auto it = lru_cache_.find(pagekey);
    if (it != lru_cache_.end()) {  // page is in cache
        // move the accessed page to the front
        pagekeys_.splice(pagekeys_.begin(), pagekeys_, it->second);
        it->second = pagekeys_.begin();  // update iterator
        return it->second->second;
    }
    // BasePage* basepage = new BasePage(master_, nullptr, pagekey.pid);
    // PutPage(pagekey, basepage);
    // return basepage;
    return nullptr;
}

pair<uint64_t, uint64_t> Worker::GetPageVersion(PageKey pagekey) {
    auto it = page_versions_.find(pagekey.pid);
    if (it != page_versions_.end()) {
        return it->second;
    }
    return { 0, 0 };
}
void Worker::UpdatePageVersion(PageKey pagekey, uint64_t current_version,
    uint64_t latest_basepage_version) {
    page_versions_[pagekey.pid] = { current_version, latest_basepage_version };
}

void Worker::PutPage(const PageKey& pagekey,
    BasePage* page) {        // add page to cache
    // insert the pair of PageKey and BasePage* to the front
    pagekeys_.push_front(std::make_pair(pagekey, page));
    lru_cache_[pagekey] = pagekeys_.begin();
}

void Worker::UpdatePageKey(
    const PageKey& old_pagekey,
    const PageKey& new_pagekey) {  // update pagekey in lru cache
    auto it = lru_cache_.find(old_pagekey);
    if (it != lru_cache_.end()) {
        // save the basepage indexed by old pagekey
        BasePage* basepage = it->second->second;
        pagekeys_.erase(it->second);  // delete old pagekey item
        lru_cache_.erase(it);
        pagekeys_.push_front(std::make_pair(new_pagekey, basepage));
        lru_cache_[new_pagekey] = pagekeys_.begin();
    }
}

DeltaPage* Worker::GetDeltaPage(const string& pid) {
    auto it = active_deltapages_.find(pid);
    if (it != active_deltapages_.end()) {
        return &it->second;  // return deltapage if it exiests
    }
    else {
        DeltaPage new_page;
        new_page.SetLastPageKey(PageKey{ 0, 0, false, pid });
        active_deltapages_[pid] = new_page;
        return &active_deltapages_[pid];
    }
}

void  Worker::WritePageCache(PageKey pagekey, Page* page) {
    page_cache_[pagekey] = page;
}