#pragma once

#include <tuple>
#include <string>
#include <cstdint>
#include <atomic>
#include <set>
#include <map>
#include <list>
#include "third_party/conc_queue.hpp"
#include "parallel.hpp"
#include "VDLS.hpp"
#include "DMMTrie.hpp"
#include "third_party/parallel_hashmap/phmap.h"
#include "Worker.hpp"

class TaskQueue {
public:
    TaskQueue() {
        stop_ = false;
        empty_task_ = std::make_tuple(0, "", "");
    }

    ~TaskQueue() = default;

    inline void postTask(std::tuple<uint64_t, std::string, std::string> kvpair) {
        if (isStopped()) {
            std::cout << "TaskQueue is stopped, cannot post task." << std::endl;
            return;
        }
#ifdef USE_THIRD_PARTY_LIBRARY
        queue_.enqueue(kvpair);
#else
        while (!queue_.push_back(kvpair));
#endif
    }

    inline std::tuple<uint64_t, std::string, std::string> popTask() {
        std::tuple<uint64_t, std::string, std::string> task;
#ifdef USE_THIRD_PARTY_LIBRARY
        if (queue_.try_dequeue(task)) {
            return task;
        }
#else
        if (queue_.size() > 0) {
            task = std::move(queue_.front());
            queue_.pop_front();
            return task;
        }
#endif

        return TaskQueue::empty_task_;
    }

    inline void stopQueue() {
        stop_ = true;
    }

    inline bool isStopped() const {
        return stop_;
    }

    inline size_t size() const {
#ifdef USE_THIRD_PARTY_LIBRARY
        return queue_.size_approx();
#else
        return queue_.size();
#endif
    }

private:
#ifdef USE_THIRD_PARTY_LIBRARY
    moodycamel::ConcurrentQueue<std::tuple<uint64_t, std::string, std::string>> queue_;
#else
    ConcurrentArray<std::tuple<uint64_t, std::string, std::string>> queue_{ 65536 };
#endif
    bool stop_;
    static std::tuple<uint64_t, std::string, std::string> empty_task_;
};

class Region : Worker {
    friend class Joiner;
    friend class Master;
public:
    Region(std::string data_path, ConcurrentArray<pair<uint64_t, std::list<BufferItem>>>& buffer, Master* master, size_t id) :
        stop_(false), buffer_(buffer), master_(master), thread_id_(id), Worker(true) {
        // PrintLog("Creating Region " + std::to_string(thread_id_));
        buffer_.push_back(make_pair(1, list<BufferItem>{})); // buffer the first version
        string file_path = data_path + "region_" + std::to_string(thread_id_) + "_index";
        string cache_path = data_path + "region_" + std::to_string(thread_id_) + "_cache";
        LSVPS* page_store = new LSVPS(file_path, cache_path);
        page_store->RegisterWorker(this);
        page_store->RegisterTrie(master);
        this->page_store_ = page_store;
        // PrintLog("Buffered item for version 1");
        region_thread_ = thread(std::bind(&Region::run, this));
#ifdef LOG 
        PrintLog("STARTED");
#endif
    }
    ~Region() {
        //page_store_->Flush();
        if (stop_) return;
        stop_ = true;
        queue_.stopQueue();
        if (region_thread_.joinable()) region_thread_.join();
        delete page_store_;
    }
    inline void postTask(std::tuple<uint64_t, std::string, std::string> kvpair) {
        queue_.postTask(kvpair);
    }
    VDLS* GetValueStore();
private:
    void run();

    void Put(std::tuple<uint64_t, std::string, std::string> kvpair);

    void Commit(uint64_t version);

    void Stop();
    void Join();
    void Flush();
    Master* master_;
    TaskQueue queue_;
    const size_t thread_id_;
    std::thread region_thread_;
    uint64_t region_version_ = 1;
    uint64_t commited_version_ = 0;
    bool stop_;


    std::map<std::string, std::string> put_cache_; 
    ConcurrentArray<std::pair<uint64_t, list<BufferItem>>>& buffer_;

inline void PrintLog(const std::string& log) {
    std::string logmsg = "[Region " + std::to_string(thread_id_) + "] " + log + "\n";
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