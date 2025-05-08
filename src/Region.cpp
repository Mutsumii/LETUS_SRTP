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
  // PrintLog(string("Nibble Bucket Address:") + to_string(reinterpret_cast<uintptr_t>(this->nibble_buckets_)));
  while (!stop_) {
    auto task = queue_.popTask();
        if (get<0>(task) != 0) {
#ifdef REGION_LOG 
            PrintLog("Pop Task [" + to_string(get<0>(task)) + "-" + get<1>(task) + "-" + get<2>(task) + "]");
#endif
            Put(task);
#ifdef REGION_LOG 
            // PrintLog("PUT [" + get<1>(task) + "-" + get<2>(task) + "]");
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
//         else if (get<1>(task).substr(0, 6) == "<MOVE>") {
// #ifdef REGION_LOG 
//             PrintLog(string("Pop Task ")+get<1>(task));
// #endif
//             // regions_[old_region_id]->postTask(make_tuple(0, "<MOVE> "+std::to_string(new_dist[i][j].nibble_value), to_string(reinterpret_cast<uintptr_t>(regions_[new_region_id]->nibble_buckets_))));
// // regions_[new_region_id]->nibble_buckets_[new_dist[i][j].nibble_value] = std::move(regions_[old_region_id]->nibble_buckets_[new_dist[i][j].nibble_value]);
// // nibble_buckets_[new_dist[i][j].nibble_value] = regions_[new_region_id]->nibble_buckets_[new_dist[i][j].value].get();
//             // std::unique_ptr<NibbleBucket>* dest_nibble_buckets = reinterpret_cast<std::unique_ptr<NibbleBucket>*>(std::stoull(get<2>(task)));
//             auto dest_nibble_buckets = reinterpret_cast<NibbleBucket**>(std::stoull(get<2>(task)));
//             uint8_t nibble_value = std::stoul(get<1>(task).substr(7));
//             // PrintLog(to_string(nibble_value));
//             // dest_nibble_buckets[nibble_value] = std::move(nibble_buckets_[nibble_value]);
//             dest_nibble_buckets[nibble_value] = nibble_buckets_[nibble_value];
//             // PrintLog(to_string(nibble_buckets_[nibble_value] == nullptr));
//             // nibble_buckets_[nibble_value] = nullptr;
//             // dest_nibble_buckets[nibble_value]->UpdateMasterNibbleBucket();
//             // PrintLog("5");
//         }
        else {
          if (get<1>(task).length() > 0) {
            PrintLog(string("Unknown Task:") + get<1>(task));
            PrintLog(get<1>(task).substr(0, 6));
            PrintLog(get<1>(task).substr(7));
          }
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

    chrono::time_point<chrono::system_clock> start;
    chrono::time_point<chrono::system_clock> end;
    vector<chrono::microseconds> durations;
    durations.resize(3, chrono::microseconds(0));
    // start = chrono::system_clock::now();
    map<string, set<string>, decltype(CompareStrings)> updates(CompareStrings);

      for (const auto &it : put_cache_) {
        for (int i = it.first.size() % 2 == 0 ? it.first.size()
                                              : it.first.size() - 1;
             i > 0; i -= 2) {
          // store the pid and nibbles of each page updated in every put
          updates[it.first.substr(0, i)].insert(it.first.substr(i, 2));
        }
      }
      // end = chrono::system_clock::now();
      // auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
      // PrintLog("Time taken to update put_cache_: " + to_string(duration.count()) + " microseconds");
      //     set<string> pids;

  // for (const auto &it : put_cache_) {
  //   for (int i = it.first.size() % 2 == 0 ? it.first.size()
  //                                         : it.first.size() - 1;
  //        i > 0; i -= 2) {
  //     pids.insert(it.first.substr(0, i));
  //   }
  // }

      size_t cnt = 0;
      // start = chrono::system_clock::now();
      for (const auto& it : updates) {
    string pid = it.first;
    // PrintLog(string("Wait ") + to_string(GetNibbleValue(pid)));
    // NibbleBucket* bucket = nullptr;
    // while (!(bucket = GetNibbleBucket(GetNibbleValue(pid))));

    // while (!bucket);
    // get the latest version number of a page

    // start = chrono::system_clock::now();

    uint64_t page_version = GetPageVersion({ 0, 0, false, pid }).first;
        PageKey pagekey = {version, 0, false, pid},
                old_pagekey = {page_version, 0, false, pid};
        BasePage* page = GetPage(old_pagekey);  // load the page into lru cache
        if (page == nullptr) {
          // GetPage returns nullptr means that the pid is new
          page = new (pool_.allocate()) BasePage(this, nullptr, pid,page_pool_.allocate());
          // cnt++;
          // start = chrono::system_clock::now();
          // page = pool_.allocate();
          // end = chrono::system_clock::now();
          // durations[0] += chrono::duration_cast<chrono::microseconds>(end - start);
          // start = chrono::system_clock::now();
          // page->SetAttribute(this, nullptr, pid, page_pool_.allocate());
          // end = chrono::system_clock::now();
          // durations[1] += chrono::duration_cast<chrono::microseconds>(end - start);
          // start = chrono::system_clock::now();
          PutPage(pagekey, page);  // add the newly generated page into cache
          // end = chrono::system_clock::now();
          // durations[2] += chrono::duration_cast<chrono::microseconds>(end - start);
        }

        DeltaPage* deltapage = GetDeltaPage(pid);

        for (const auto& nibbles : it.second) {
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
        // start = chrono::system_clock::now();
        UpdatePageKey(old_pagekey, pagekey);
        // end = chrono::system_clock::now();
        // durations[4] += chrono::duration_cast<chrono::microseconds>(end - start);
      }
      // PrintLog("Total number of pages created: " + to_string(cnt));
      // for(int i = 0; i < durations.size(); i++) {
      //   PrintLog("Time taken to update page phase" + to_string(i) + ": " + to_string(durations[i].count()) + " microseconds");
      // }

      // end = chrono::system_clock::now();
      // auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
      // PrintLog("Time taken to update page: " + to_string(duration.count()) + " microseconds");
      // start = chrono::system_clock::now();
      for (const auto& it : put_cache_) {
        std::string nibbles = it.first.substr(0, 2);
        // NibbleBucket* bucket = GetNibbleBucket(GetNibbleValue(nibbles));
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
      // end = chrono::system_clock::now();
      // duration = chrono::duration_cast<chrono::microseconds>(end - start);
      // PrintLog("Time taken to update buffer: " + to_string(duration.count()) + " microseconds");
      // Push empty buffer for next version, indicating that the version is committed
      while(!buffer_.push_back(make_pair(version + 1, list<BufferItem>{}))) {
        PrintLog("Buffer is full, waiting for commit");
      }
      commited_version_ = version;
      // std::this_thread::sleep_for(chrono::milliseconds(1));
      page_cache_.clear();
      put_cache_.clear();
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
  // while (region_version_ != commited_version_);
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
