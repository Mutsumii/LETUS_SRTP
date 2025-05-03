#include "Master.hpp"
#include "DMMTrie.hpp"
#include "LoadBalancer.hpp"
#include "Worker.hpp"

const std::string& GetNibble(uint8_t nibble_value) {
    static string nibbles[256] = {
        "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0A", "0B", "0C", "0D", "0E", "0F",
        "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1A", "1B", "1C", "1D", "1E", "1F",
        "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "2A", "2B", "2C", "2D", "2E", "2F",
        "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3A", "3B", "3C", "3D", "3E", "3F",
        "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "4A", "4B", "4C", "4D", "4E", "4F",
        "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "5A", "5B", "5C", "5D", "5E", "5F",
        "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6A", "6B", "6C", "6D", "6E", "6F",
        "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "7A", "7B", "7C", "7D", "7E", "7F",
        "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "8A", "8B", "8C", "8D", "8E", "8F",
        "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9A", "9B", "9C", "9D", "9E", "9F",
        "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "AA", "AB", "AC", "AD", "AE", "AF",
        "B0", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "BA", "BB", "BC", "BD", "BE", "BF",
        "C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "CA", "CB", "CC", "CD", "CE", "CF",
        "D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "DA", "DB", "DC", "DD", "DE", "DF",
        "E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "EA", "EB", "EC", "ED", "EE", "EF",
        "F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "FA", "FB", "FC", "FD", "FE", "FF"
    };
    return nibbles[nibble_value];
}

void Master::LoadBalance() {
    vector<TaggedInt> data;
    data.reserve(256);
    // TaggedInt data[256];
    for (int i = 0; i < 256; i++) {
        data.emplace_back(TaggedInt{ nibble_buckets_[i]->GetPageKeySize(),nibble_dict_[i], static_cast<uint8_t>(i) });
        // data[i].original_region = nibble_dict_[i];
        // data[i].value = nibble_buckets_[i]->GetAccessCount();
    }
    auto new_dist = partitionIntoNGroups(data, MAX_REGION_NUM);
    for(int i = 0; i < MAX_REGION_NUM; i++) {
     for(int j = 0; j < new_dist[i].size(); j++) {
        auto old_region_id = new_dist[i][j].original_region;
        auto new_region_id = i;
        if (old_region_id != new_region_id && new_dist[i][j].value > 0) {
            // PrintLog("Move Nibble \"" + GetNibble(new_dist[i][j].nibble_value) + "\"(" + to_string(new_dist[i][j].value) + ")" + " from Region " + to_string(old_region_id) + " to Region " + to_string(new_region_id));
            regions_[old_region_id]->postTask(make_tuple(0, "<MOVE> " + std::to_string(new_dist[i][j].nibble_value), to_string(reinterpret_cast<uintptr_t>(regions_[new_region_id]->nibble_buckets_))));
            // PrintLog(string("Post Task [<MOVE> ") + std::to_string(new_dist[i][j].nibble_value) + " | " + to_string(reinterpret_cast<uintptr_t>(regions_[new_region_id]->nibble_buckets_)) +"] to Region " + to_string(old_region_id));
            // regions_[new_region_id]->nibble_buckets_[new_dist[i][j].nibble_value] = std::move(regions_[old_region_id]->nibble_buckets_[new_dist[i][j].nibble_value]);
            // nibble_buckets_[new_dist[i][j].nibble_value] = regions_[new_region_id]->nibble_buckets_[new_dist[i][j].value].get();
            nibble_buckets_[new_dist[i][j].nibble_value]->SetOwnerRegion(new_region_id);
            nibble_dict_[new_dist[i][j].nibble_value] = new_region_id;
        }
     }
    }
}

Master::Master(VDLS* value_store) : value_store_(value_store) {
    regions_.reserve(MAX_REGION_NUM);
    bottomup_buffers_ = vector<ConcurrentArray<pair<uint64_t, list<BufferItem>>>>(MAX_REGION_NUM);
    // for (uint8_t i = 0; i < 255; i++) {
    //     nibble_dict_[i] = 255;
    //     nibble_buckets_[i] = std::make_unique<NibbleBucket>();
    // }
    NibbleBucket::master_nibble_bucket_ = this->nibble_buckets_;
    // PrintLog(string("Master Nibble Bucket: ") + to_string(reinterpret_cast<uintptr_t>(nibble_buckets_)));
    for (uint8_t i = 0; i < MAX_REGION_NUM; i++) {
        // PrintLog("Creating Region " + to_string(i));
        auto new_region = new Region(value_store_, bottomup_buffers_.at(i), this, i);
        for(uint16_t j = i; j < 256; j += MAX_REGION_NUM) {
            nibble_dict_[j] = i;
            // new_region->nibble_buckets_[j] = std::make_unique<NibbleBucket>(j);
            new_region->nibble_buckets_[j] = new NibbleBucket(j);
            // nibble_buckets_[j] = new_region->nibble_buckets_[j].get();
            nibble_buckets_[j] = new_region->nibble_buckets_[j];
            nibble_buckets_[j]->SetOwnerRegion(i);
        }
        regions_.push_back(new_region);
        // PrintLog("Region " + to_string(i) + " created");
    }
    joiner_ = new Joiner(this, value_store_);
}

void Master::Put(uint64_t tid, uint64_t version, const string& key,
    const string& value){
    // auto nibble = key.length() >= 2 ? key.substr(0, 2) : key;
    size_t nibble_value = GetNibbleValue(key);
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
    // if (region_id < MAX_REGION_NUM) {
    #ifdef MASTER_LOG 
        PrintLog("Post Task [" + to_string(version) + "-" + key + "-" + value + "] to [Region " + to_string((size_t)region_id) + "]");
#endif
        // region_workload_[region_id]++;
        regions_.at(region_id)->postTask(make_tuple(version, key, value));
        // }
        // else {
        //     #ifdef MASTER_LOG 
        //             PrintLog("RR Post Task [" + to_string(version) + "-" + key + "-" + value + "] to [Region " + to_string((size_t)next_region_) + "]");
        //     #endif
        //     regions_.at(next_region_)->postTask(make_tuple(version, key, value));
        //     nibble_dict_[nibble_value] = next_region_;
        //     regions_.at(next_region_)->nibble_buckets_[nibble_value] = std::move(nibble_buckets_[nibble_value]);
        //     // region_workload_[next_region_]++;
        //     next_region_ = (next_region_ + 1) % MAX_REGION_NUM;
        // }
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
            // if (version % 5 == 0) {
            //     LoadBalance();
            // }
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
  uint8_t nibble_value = GetNibbleValue(nibble_path);
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
      if (!page->GetRoot()->GetChild(GetIndex(nibble_path[i]))->IsLeaf()) {
        // second level is indexnode
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
for (int i = 0; i < key.size()+1; i += 2) {
string pid = nibble_path.substr(0, i);
BasePage* page = nullptr;
uint8_t nibble_value = GetNibbleValue(pid);
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
->GetNodeProof(i + 1, GetIndex(nibble_path[i+1])));
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