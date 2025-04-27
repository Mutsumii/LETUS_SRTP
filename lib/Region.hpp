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
    TaskQueue() {}

    ~TaskQueue() = default;

    inline void postTask(std::tuple<uint64_t, std::string, std::string> kvpair) {
        if (isStopped()) {
            std::cout << "TaskQueue is stopped, cannot post task." << std::endl;
            return;
        }
        queue_.enqueue(kvpair);
    }

    inline std::tuple<uint64_t, std::string, std::string> popTask() {
        std::tuple<uint64_t, std::string, std::string> task;
        if (queue_.try_dequeue(task)) {
            return task;
        }
        return TaskQueue::empty_task_;
    }

    inline void stopQueue() {
        stop_ = true;
    }

    inline bool isStopped() const {
        return stop_;
    }

    inline size_t size() const {
        return queue_.size_approx();
    }

    private:
    moodycamel::ConcurrentQueue<std::tuple<uint64_t, std::string, std::string>> queue_;
    bool stop_;
    static std::tuple<uint64_t, std::string, std::string> empty_task_;
};

class Region : Worker{
    friend class Joiner;
    friend class Master;
public:
    Region(VDLS* value_store, ConcurrentArray<pair<uint64_t, std::list<BufferItem>>>& buffer, Master* master, size_t id) :
        value_store_(value_store), stop_(false), buffer_(buffer), master_(master), thread_id_(id) {
        // PrintLog("Creating Region " + std::to_string(thread_id_));
        buffer_.push_back(make_pair(1, list<BufferItem>{})); // buffer the first version
        // PrintLog("Buffered item for version 1");
        region_thread_ = thread(std::bind(&Region::run, this));
#ifdef LOG 
        PrintLog("STARTED");
#endif
    }
    ~Region() {
        if (stop_) return;
        stop_ = true;
        queue_.stopQueue();
        region_thread_.join();
    }
    inline void postTask(std::tuple<uint64_t, std::string, std::string> kvpair) {
        queue_.postTask(kvpair);
    }
    
    private:
    void run();

    void Put(std::tuple<uint64_t, std::string, std::string> kvpair);

    void Commit(uint64_t version);

    void Stop();
    void Join();

    Master* master_;
    VDLS* value_store_;
    TaskQueue queue_;

    const size_t thread_id_;
    std::thread region_thread_;
    uint64_t region_version_ = 1;
    uint64_t commited_version_ = 0;
    bool stop_;


    std::map<std::string, std::string> put_cache_;
    ConcurrentArray<std::pair<uint64_t, list<BufferItem>>>& buffer_;

    inline void PrintLog(const string& log) {
        std::string logmsg = "[Region " + std::to_string(thread_id_) + "] " + log + "\n";
        std::cout << logmsg;
    }
};