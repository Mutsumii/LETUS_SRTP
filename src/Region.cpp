#include "Region.hpp"
#include "Master.hpp"

std::tuple<uint64_t, std::string, std::string> TaskQueue::empty_task_ = std::make_tuple(0, "", "");

auto CompareStrings = [](const std::string &a, const std::string &b) {
  if (a.size() != b.size()) {
    return a.size() > b.size();  // first compare length
  }
  return a < b;  // then compare alphabetical order
};

void Region::run() {
  while (!stop_) {
    auto task = queue_.popTask();
        if (get<0>(task) != 0) {
#ifdef REGION_LOG 
            PrintLog("Pop Task [" + to_string(get<0>(task)) + "-" + get<1>(task) + "-" + get<2>(task) + "]");
#endif
            Put(task);
#ifdef REGION_LOG 
            PrintLog("PUT [" + get<1>(task) + "-" + get<2>(task) + "]");
#endif
        }
        else if (get<1>(task) == "<COMMIT>") {
#ifdef REGION_LOG 
            PrintLog("Pop Task [COMMIT]");
#endif
            uint64_t version = atoi(get<2>(task).c_str());
            #ifdef TIMESTAMP_LOG
            PrintLog("COMMIT START <" + to_string(version) + "> | " + GetCurrentTimeStamp(3));
            #endif

#ifdef REGION_LOG 
            PrintLog("COMMIT [" + get<2>(task) + "]");
#endif
            Commit(version);
        }
        else if (get<1>(task) == "<STOP>") {
          // assert(queue_.size() == 0);
#ifdef REGION_LOG 
          PrintLog("Pop Task [STOP]");
#endif
            Stop();
            
            // PrintLog("Pop Task [" + to_string(get<0>(task)) + "-" + get<1>(task) + "-" + get<2>(task) + "]");
            // PrintLog("JOIN [" + get<2>(task) + "]");
            // Join();
        }
        
        // {
        //   std::string buffer_state = "Buffer Size: " + std::to_string(buffer_.size()) + "\n";
        //   buffer_state += buffer_.ToString() + "\n";
        //   // for (size_t i = buffer_) {
        //   //   buffer_state += "Version: " + std::to_string(i.first) + "\n";
        //   //   for (auto j : i.second) {
        //   //     buffer_state += "\t" + j.ToString() + "\n";
        //   //   }
        //   // }
        //   PrintLog(buffer_state);
        // }
    }
}
void Region::Put(tuple<uint64_t, string, string> kvpair) {
    uint64_t version;
    string key;
    string value;
    tie(version, key, value) = kvpair;
    if (version < region_version_) {
        cout << "Version " << version << " is outdated!"
            << endl;  // version invalid
        return;
    }
    if (value == "") {
        cout << "Value cannot be empty string" << endl;
        return;
    }
    region_version_ = version;
    put_cache_[key] = value;
    return;
}


void Region::Commit(uint64_t version) {
    if (version < commited_version_) {
        PrintLog("Commit version incompatible");
        return;
    }
    if (version > region_version_) {
      // PrintLog("Buffer Size: " + to_string(buffer_.size()));
      for (uint64_t i = buffer_.back().first + 1; i <= version; i++) {
        PrintLog("Pushing empty buffer for version " + to_string(i));
        buffer_.push_back(make_pair(i, list<BufferItem>{}));
        }
#ifdef REGION_LOG 
    PrintLog("During Commit, Force updated to version " + to_string(version));
#endif
    region_version_ = version;
    }

    map<string, set<string>, decltype(CompareStrings)> updates(CompareStrings);

      for (const auto &it : put_cache_) {
        for (int i = it.first.size() % 2 == 0 ? it.first.size()
                                              : it.first.size() - 1;
             i > 0; i -= 2) {
          // store the pid and nibbles of each page updated in every put
          updates[it.first.substr(0, i)].insert(it.first.substr(i, 2));
        }
      }
      for (const auto &it : updates) {
        string pid = it.first;
        // get the latest version number of a page
        uint64_t page_version = GetPageVersion({ 0, 0, false, pid }).first;
        PageKey pagekey = {version, 0, false, pid},
                old_pagekey = {page_version, 0, false, pid};
        BasePage *page = GetPage(old_pagekey);  // load the page into lru cache
    
        if (page == nullptr) {
          // GetPage returns nullptr means that the pid is new
          // page = new BasePage(this, nullptr, pid);
          page = pool_.allocate();
          page->SetAttribute(this, nullptr, pid);
          PutPage(pagekey, page);  // add the newly generated page into cache
        }
    
        DeltaPage *deltapage = GetDeltaPage(pid);
    
        for (const auto &nibbles : it.second) {
          // path is key when page is leaf page, pid of child page when page is
          // index page
          string path = pid + nibbles;
          tuple<uint64_t, uint64_t, uint64_t> location;
          string value, child_hash;
          if (nibbles.size() == 2) {  // indexnode + indexnode
            child_hash = GetPage({ version, 0, false, path })->GetRoot()->GetHash();
          } else {  // (indexnode + leafnode) or leafnode
            value = put_cache_[path];
            location = value_store_->WriteValue(version, path, value);
          }
        page->UpdatePage(version, location, value, nibbles, child_hash,
                             deltapage, pagekey);
        }
    
        UpdatePageKey(old_pagekey, pagekey);
      }
      for (const auto& it : put_cache_) {
          std::string nibbles = it.first.substr(0, 2);
          tuple<uint64_t, uint64_t, uint64_t> location;
          string value, child_hash;
          if (nibbles.size() == 2) {  // indexnode + indexnode
            BasePage* base = GetPage({ version, 0, false, nibbles });
            child_hash = base->GetRoot()->GetHash();
          }
          else {  // (indexnode + leafnode) or leafnode
            value = put_cache_[nibbles];
            location = value_store_->WriteValue(version, nibbles, value);
          }
          BufferItem result = BufferItem(location, value, nibbles, child_hash);
          if (!buffer_.empty()) {
            auto& buffer_back = buffer_.back();
            if (buffer_back.first == version) {
                buffer_back.second.push_back(result);
            }
            else{
            buffer_.push_back(make_pair(version, list<BufferItem>{result}));
            }
          }
            else {
                buffer_.push_back(make_pair(version, list<BufferItem>{result}));
          }
      }
      // Push empty buffer for next version, indicating that the version is committed
      while(!buffer_.push_back(make_pair(version + 1, list<BufferItem>{}))) {
        PrintLog("Buffer is full, waiting for commit");
      }
      // std::this_thread::sleep_for(chrono::milliseconds(1));
      page_cache_.clear();
      put_cache_.clear();
      commited_version_ = version;
      #ifdef TIMESTAMP_LOG
      PrintLog("COMMIT DONE <" + to_string(version) + "> | " + GetCurrentTimeStamp(3));
      #endif
#ifdef REGION_LOG
      PrintLog("Version " + to_string(version) + " committed");
    #endif
}

void Region::Stop() {
  if (stop_) return;
  stop_ = true;
  queue_.stopQueue();
  // master_->running_region_num_ -= 1;
  // region_thread_.join();
  while (region_version_ != commited_version_);
  if (buffer_.size() == 0) {
    PrintLog("Empty Buffer Before Stop");
    throw std::runtime_error("Empty Buffer Before Stop");
  }
  auto& item = buffer_.back();
  if (item.first != commited_version_ + 1 || item.second.size() != 0) {
    // PrintLog("Invalid Buffer State Before Stop: [First] " + std::to_string(item.first) + " [SecondSize] " + std::to_string(item.second.size()));
    throw std::runtime_error("Invalid Buffer State Before Stop: [First] " + std::to_string(item.first) + " [SecondSize] " + std::to_string(item.second.size()));
  }
  item.first = 0;
  // PrintLog("Stopped | "+ GetCurrentTimeStamp(3));
#ifdef REGION_LOG
  PrintLog("STOPPED");
#endif
}

void Region::Join() {
  if (stop_) return;
  stop_ = true;
  queue_.stopQueue();
  region_thread_.join();
  PrintLog("JOINED");
#ifdef REGION_LOG
  PrintLog("JOINED");
#endif
}
