#include "Worker.hpp"
#include "Master.hpp"
BasePage* Worker::GetPage(
    const PageKey& pagekey) {  // get a page by its pagekey
        auto it = lru_cache_.find(pagekey);
        if (it != lru_cache_.end()) {  // page is in cache
            // move the accessed page to the front
            pagekeys_.splice(pagekeys_.begin(), pagekeys_, it->second);
            it->second = pagekeys_.begin();  // update iterator
            return it->second->second;
        }
        BasePage* basepage = page_store_->LoadPage(pagekey);
        if(!basepage) return nullptr;
        PutPage(pagekey, basepage);
        return basepage;
        //return nullptr;
}

BasePage* Worker::JoinerGetPage(const PageKey& pagekey) {
  auto it = lru_cache_.find(pagekey);
  if(it != lru_cache_.end()) {  // page is in cache
    // move the accessed page to the front
    pagekeys_.splice(pagekeys_.begin(), pagekeys_, it->second);
    it->second = pagekeys_.begin();  // update iterator
    return it->second->second;
  }
    return nullptr;
}

DeltaPage* Worker::GetDeltaPage(const string& pid) {
    auto it = active_deltapages_.find(pid);
    if (it != active_deltapages_.end()) {
        return it->second;  // return deltapage if it exists
    }
    else {
        DeltaPage* new_page=  new(pool_delta_.allocate()) DeltaPage(page_pool_.allocate(), pid);
        new_page->SetLastPageKey(PageKey{ 0, 0, false, pid });
        active_deltapages_[pid] = new_page;
        return active_deltapages_[pid];
    }
}

pair<uint64_t, uint64_t>  Worker::GetPageVersion(PageKey pagekey) {
    auto it = page_versions_.find(pagekey.pid); //跟据pid寻找version
    if (it != page_versions_.end()) {
        return it->second;
    }
    return { 0, 0 };
}

void  Worker::UpdatePageVersion(PageKey pagekey, uint64_t current_version, uint64_t latest_basepage_version) {
        page_versions_[pagekey.pid] = { current_version, latest_basepage_version };
}

void  Worker::PutPage(const PageKey& pagekey, BasePage* page) {
    // auto bucket = GetNibbleBucket(GetNibbleValue(pagekey.pid));
    // if (bucket == nullptr) {
    //     throw std::runtime_error("NibbleBucket not found");
    // }
    auto it = lru_cache_.find(pagekey);
    if (it != lru_cache_.end()) {
    //   delete it->second->second;
      pagekeys_.erase(it->second);
      lru_cache_.erase(it);
    }
      pagekeys_.push_front(std::make_pair(pagekey, page));
      lru_cache_[pagekey] = pagekeys_.begin();
}

void  Worker::UpdatePageKey(const PageKey& old_pagekey, const PageKey& new_pagekey) {
    auto it = lru_cache_.find(old_pagekey);
    if (it != lru_cache_.end()) {
        // save the basepage indexed by old pagekey
        BasePage* basepage = it->second->second;
        pagekeys_.erase(it->second);  // delete old pagekey item
        lru_cache_.erase(it);
        pagekeys_.push_front(std::make_pair(new_pagekey, basepage)); //更新lru cache
        lru_cache_[new_pagekey] = pagekeys_.begin();
    }
}
void  Worker::WritePageCache(PageKey pagekey, Page* page) {
    page_cache_[pagekey] = page;
}

//新增
PageKey Worker::GetLatestBasePageKey(PageKey pagekey) const {
  auto it = page_versions_.find(pagekey.pid);
  if (it != page_versions_.end()) {
    return {it->second.second, pagekey.tid, false, pagekey.pid};
  }
  return PageKey{0, 0, false, pagekey.pid};
}

uint64_t Worker::GetVersionUpperbound(const string &pid, uint64_t version,Master* master_) {
  std::unordered_map<string, vector<uint64_t>> deltapage_versions_ = master_->GetDeltaPageVersions();
  if (deltapage_versions_.find(pid) == deltapage_versions_.end()) {
    return 0;  // no deltapage of this pid
  }
}