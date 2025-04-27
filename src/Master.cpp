#include "Master.hpp"
#include "DMMTrie.hpp"

// convert hexadecimal digit to corresponding index 0~15
inline int GetIndex(char ch) {
    if (isdigit(ch)) {
      return ch - '0';
    } else if (ch >= 'a' && ch <= 'f') {
      return ch - 'a' + 10;
    } else if (ch >= 'A' && ch <= 'F') {
      return ch - 'A' + 10;
    } else {
      return -1;
    }
  }

Master::Master(VDLS* value_store) : value_store_(value_store) {
    regions_.reserve(MAX_REGION_NUM);
    bottomup_buffers_ = vector<ConcurrentArray<pair<uint64_t, list<BufferItem>>>>(MAX_REGION_NUM);
    for (uint8_t i = 0; i < 255; i++) {
        nibble_dict_[i] = 255;
    }
    for (uint8_t i = 0; i < MAX_REGION_NUM; i++) {
        // PrintLog("Creating Region " + to_string(i));
        auto new_region = new Region(value_store_, bottomup_buffers_.at(i), this, i);
        regions_.push_back(new_region);
        // PrintLog("Region " + to_string(i) + " created");
    }
    joiner_ = new Joiner(this, value_store_);
}

void Master::Put(uint64_t tid, uint64_t version, const string& key,
    const string& value){
    auto nibble = key.length() >= 2 ? key.substr(0, 2) : key;
    size_t nibble_value = nibble.length() == 2 ? (nibble[0] - '0') * 10 + (nibble[1] - '0') : (nibble[0] - '0');
    // auto it = nibble_dict_.find(nibble_value);
    //     if (it != nibble_dict_.end()) {
    // #ifdef MASTER_LOG 
    //     PrintLog("ND Post Task [" + to_string(version) + "-" + key + "-" + value + "] to [Region " + to_string((size_t)it->second) + "]");
    // #endif
    //         regions_.at(it->second)->postTask(make_tuple(version, key, value));
    //     }
    //     else {
    //         #ifdef MASTER_LOG 
    //                 PrintLog("RR Post Task [" + to_string(version) + "-" + key + "-" + value + "] to [Region " + to_string((size_t)next_region_) + "]");
    //         #endif
    //         regions_.at(next_region_)->postTask(make_tuple(version, key, value));
    //         nibble_dict_.emplace(nibble_value, next_region_);
    //         next_region_ = (next_region_ + 1) % MAX_REGION_NUM;
    //     }
    auto region_id = nibble_dict_[nibble_value];
    // PrintLog("Nibble Value: " + to_string(nibble_value)+" Region ID: " + to_string(region_id));
    if (region_id < MAX_REGION_NUM) {
    #ifdef MASTER_LOG 
        PrintLog("ND Post Task [" + to_string(version) + "-" + key + "-" + value + "] to [Region " + to_string((size_t)region_id) + "]");
#endif
        // region_workload_[region_id]++;
        regions_.at(region_id)->postTask(make_tuple(version, key, value));
        }
        else {
            #ifdef MASTER_LOG 
                    PrintLog("RR Post Task [" + to_string(version) + "-" + key + "-" + value + "] to [Region " + to_string((size_t)next_region_) + "]");
            #endif
            regions_.at(next_region_)->postTask(make_tuple(version, key, value));
            nibble_dict_[nibble_value] = next_region_;
            // region_workload_[next_region_]++;
            next_region_ = (next_region_ + 1) % MAX_REGION_NUM;
        }
}

void Master::Commit(uint64_t version){
    #ifdef MASTER_LOG 
            PrintLog("Commit Signal");
    #endif
            // for(uint8_t i = 0; i < MAX_REGION_NUM; i++) {
            //    PrintLog("Region " + to_string(i) + " Workload: " + to_string(region_workload_[i]));
            // }
            // exit(0);
            for (auto& region : regions_) {
                region->postTask(make_tuple(0, "<COMMIT>", to_string(version)));
            }
}



void  Master::AddDeltaPageVersion(const string& pid, uint64_t version) {
    deltapage_versions_[pid].push_back(version);
}

std::string Master::Get(uint64_t tid, uint64_t version, const std::string& key) {
    // if (version >= joiner_->version_) {
    //     cout << "Version " << version << " has not committed yet" << endl;
    //     return "";
    // }
#ifdef MASTER_LOG
    PrintLog("Get [" + to_string(version) + "-" + key + "]");
    PrintLog("Wait");
#endif
// PrintLog("Wait");
    WaitForCommit(version);
    // PrintLog("Get Started");
#ifdef MASTER_LOG
    PrintLog("Get Started");
#endif
    string nibble_path = key;
  uint64_t page_version = version;
  LeafNode* leafnode = nullptr;
  uint8_t nibble_value = nibble_path.length() >= 2
      ? (nibble_path[0] - '0') * 10 + (nibble_path[1] - '0')
      : (nibble_path[0] - '0');
//   auto nibble_item = nibble_dict_.find(nibble_value);
//   if(nibble_item == nibble_dict_.end()) {
//     cout << "Key " << key << " not found at version " << version << endl;
//     return "";
//   }
uint8_t region_id = nibble_dict_[nibble_value];
if(region_id >= MAX_REGION_NUM) {
  cout << "Key " << key << " not found at version " << version << endl;
  return "";
}
  for (int i = 0; i <= key.size(); i += 2) {
      string pid = nibble_path.substr(0, i);
      BasePage* page = nullptr;
      if (i == 0)
          page = joiner_->GetPage({ page_version, 0, false, pid });  // false means basepage
      else 
      page =
        regions_[region_id]->GetPage({page_version, 0, false, pid});  // false means basepage
    if (page == nullptr) {
      cout << "Key " << key << " not found at version " << version << endl;
      return "";
    }

    if (!page->GetRoot()->IsLeaf()) {  // first level in page is indexnode
      if (!page->GetRoot()->GetChild(nibble_path[i] - '0')->IsLeaf()) {
        // second level is indexnode
        page_version = page->GetRoot()
                           ->GetChild(nibble_path[i] - '0')
                           ->GetChildVersion(nibble_path[i + 1] - '0');
      } else {  // second level is leafnode
        leafnode = static_cast<LeafNode *>(
            page->GetRoot()->GetChild(nibble_path[i] - '0'));
      }
    } else {  // first level is leafnode
      leafnode = static_cast<LeafNode *>(page->GetRoot());
    }
  }
  tuple<uint64_t, uint64_t, uint64_t> location = leafnode->GetLocation();
#ifdef MASTER_LOG
  cout << "location:" << get<0>(location) << " " << get<1>(location) << " "
       << get<2>(location) << endl;
#endif
  string value = value_store_->ReadValue(leafnode->GetLocation());
#ifdef MASTER_LOG
  cout << "Key " << key << " has value " << value << " at version " << version
       << endl;
#endif
  return value;
}

DMMTrieProof Master::GetProof(uint64_t tid, uint64_t version,
    const string &key) {
DMMTrieProof merkle_proof;
string nibble_path = key;
uint64_t page_version = version;
LeafNode *leafnode = nullptr;
for (int i = 0; i < key.size(); i += 2) {
string pid = nibble_path.substr(0, i);
BasePage* page = nullptr;
uint8_t nibble_value = nibble_path.length() >= 2
    ? GetIndex(nibble_path[0]) * 16 + GetIndex(nibble_path[1])
    : GetIndex(nibble_path[0]);
if (i == 0)
    page = joiner_->GetPage({ page_version, 0, false, pid });  // false means basepage
else {
    uint8_t region_id = nibble_dict_[nibble_value];
    if(region_id >= MAX_REGION_NUM) {
        cout << "Key " << key << " not found at version " << version << endl;
        merkle_proof.value = "";
        return merkle_proof;
    }
    page = regions_[region_id]->GetPage({ page_version, 0, false, pid });  // false means basepage
}
if (page == nullptr || page->GetRoot() == nullptr) {
    cout << "Key " << key << " not found at version " << version << endl;
    merkle_proof.value = "";
    return merkle_proof;
}

if (!page->GetRoot()->IsLeaf()) {
    if (!page->GetRoot()->HasChild(GetIndex(nibble_path[i]))) {

        cout << "Key " << key << " not found at version " << version << endl;
        merkle_proof.value = "";
        return merkle_proof;
      }
    // first level in page is indexnode
merkle_proof.proofs.push_back(
page->GetRoot()->GetNodeProof(i, GetIndex(nibble_path[i])));
if (!page->GetRoot()->GetChild(GetIndex(nibble_path[i]))->IsLeaf()) {
// second level is indexnode
merkle_proof.proofs.push_back(
page->GetRoot()
->GetChild(GetIndex(nibble_path[i]))
->GetNodeProof(i + 1, nibble_path[i + 1] - '0'));
page_version = page->GetRoot()
->GetChild(GetIndex(nibble_path[i]))
->GetChildVersion(GetIndex(nibble_path[i+1]));
} else {  // second level is leafnode
leafnode = static_cast<LeafNode *>(
page->GetRoot()->GetChild(GetIndex(nibble_path[i])));
}
} else {  // first level is leafnode
leafnode = static_cast<LeafNode *>(page->GetRoot());
}
}
merkle_proof.value = value_store_->ReadValue(leafnode->GetLocation());
reverse(merkle_proof.proofs.begin(), merkle_proof.proofs.end());
return merkle_proof;
}

void Master::Stop() {
    for(auto& region : regions_) {
        region->postTask(make_tuple(0, "<STOP>", ""));
    }
    joiner_->Stop();
    // while(true)
    // if (joiner_->stopped_) return;
#ifdef MASTER_LOG
    PrintLog("JOINED");
    #endif
}

void Master::WaitForCommit(uint64_t version) {
    while (version >= joiner_->version_) {
        std::this_thread::yield();
    }
}