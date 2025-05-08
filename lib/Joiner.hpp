#pragma once

#include "parallel.hpp"
#include "Master.hpp"
#include "Region.hpp"
#include "Worker.hpp"
// #define LOG

class Master;
class Region;
class BufferItem;

class Joiner : Worker{
    friend class Master;
    friend class Region;
    public:
    Joiner(Master* master, std::string data_path);
    ~Joiner() {
        if (joiner_thread_.joinable())
            joiner_thread_.join();
    }
    bool WaitForOldVersion(uint64_t version);

    void WriteAllBufferItems();

    void run();

    void Stop();

    void Join();
    private:
    Master* master_;
    VDLS* value_store_;
    thread joiner_thread_;
    uint64_t version_ = 1;
    vector<BufferItem> buffer_;
    bool stop_{ false };
    bool stopped_{ false };

    void PrintLog(const string& log) {
        std::string logmsg = std::string(BLUE) + "[Joiner] " + log + RESET + "\n";
        std::cout << logmsg;
    }
};