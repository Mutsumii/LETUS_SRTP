#ifndef _VDLS_HPP_
#define _VDLS_HPP_

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <atomic>
#include <deque>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <vector>
#include <condition_variable>
#include <cstring>
#include <chrono>
#include <filesystem>
#include "third_party/blockingconcurrentqueue.h"  // 包含阻塞无锁队列库

using namespace std;

class VDLS {
public:
    VDLS(string file_path = "/mnt/c/Users/qyf/Desktop/LETUS_prototype/data/", string prefix = "")
        : file_path_(file_path + "data_file_" + prefix + "_"),
          write_map_(MAP_FAILED),
          read_map_(MAP_FAILED),
          read_map_fileID_(-1),
          stop_flag_(false) {
        current_fileID_.store(0);
        current_offset_.store(0);
        OpenAndMapWriteFile();
        write_thread_ = thread(&VDLS::WriteThreadValue, this);
    }

    ~VDLS() {
        stop_flag_.store(true); // 设置停止标志
        //给出退出信号
        write_queue_.enqueue({ nullptr, -1, 0, 0 }); // 发送退出信号到写线程
        if (write_thread_.joinable()) {
            write_thread_.join();
        }
        RemainingWrite(); // 确保所有剩余的写入任务都被处理
        if (read_map_ != MAP_FAILED) {
            munmap(read_map_, MaxFileSize);
        }
    }
    
    tuple<int, size_t, size_t> WriteValue(uint64_t version, const string& key, const string& value) {
        auto record_ptr = make_unique<string>(to_string(version) + "," + key + "," + value + "\n");
        size_t record_size = record_ptr->size();

        int current_fileID;
        size_t offset;

        // 原子地预留空间并处理文件切换
        while (true) {
            current_fileID = current_fileID_.load();
            offset = current_offset_.load();

            // 检查是否需要切换文件
            if (offset + record_size > MaxFileSize) {
                int new_fileID = current_fileID + 1;
                // 尝试原子切换文件
                if (current_fileID_.compare_exchange_strong(current_fileID, new_fileID)) {
                    // 切换成功：重置偏移量并预留空间
                    current_offset_.store(record_size); // 下条记录从record_size开始
                    offset = 0; // 当前记录在新文件的0偏移
                    current_fileID = new_fileID;
                    break;
                }
                // 切换失败则重试（其他线程已切换）
                continue;
            }

            // 尝试原子预留空间
            if (current_offset_.compare_exchange_weak(offset, offset + record_size)) {
                // 预留成功：使用当前文件和预留偏移
                break;
            }
        }

        // 入队无锁队列
        write_queue_.enqueue({ move(record_ptr), current_fileID, offset, record_size });
        
        return make_tuple(current_fileID, offset, record_size);
    }
    
    void WriteThreadValue() {
        while (true) {
            WriteTask task;
            // 阻塞等待任务
            write_queue_.wait_dequeue(task);
            
            // 检查退出信号
            if (task.fileID == -1 || stop_flag_.load()) {
                break;
            }

            if (task.fileID != fileID) {
                // 切换文件
                if (write_map_ != MAP_FAILED) {
                    msync(write_map_, MaxFileSize, MS_SYNC);
                    munmap(write_map_, MaxFileSize);
                }
                fileID = task.fileID;
                OpenAndMapWriteFile();
            }
            memcpy(static_cast<char*>(write_map_) + task.offset, 
                   task.record_ptr->c_str(), 
                   task.size);
        }
    }
    //最后清理队列中任务
    void RemainingWrite() {
        WriteTask task;
        while (write_queue_.try_dequeue(task)) {
            if (task.fileID == -1 || !task.record_ptr) continue;
            
            // 检查文件是否需要切换
            if (task.fileID != fileID) {
                if (write_map_ != MAP_FAILED) {
                    msync(write_map_, MaxFileSize, MS_SYNC);
                    munmap(write_map_, MaxFileSize);
                }
                fileID = task.fileID;
                OpenAndMapWriteFile();
            }
            memcpy(static_cast<char*>(write_map_) + task.offset, 
                   task.record_ptr->c_str(), 
                   task.size);
        }
        
        // 最后刷盘
        if (write_map_ != MAP_FAILED) {
            msync(write_map_, MaxFileSize, MS_SYNC);
            munmap(write_map_, MaxFileSize);
            write_map_ = MAP_FAILED;
        }
    }
    
    string ReadValue(const tuple<uint64_t, uint64_t, uint64_t>& location) {
        uint64_t fileID, offset, size;
        tie(fileID, offset, size) = location;

        // 检查是否需要重新映射文件
        if (fileID != read_map_fileID_) {
            if (read_map_ != MAP_FAILED) {
                munmap(read_map_, MaxFileSize);
                read_map_ = MAP_FAILED;
            }
            OpenAndMapReadFile(fileID);
            read_map_fileID_ = fileID;
        }

        // 从映射区域读取数据
        string line(static_cast<char*>(read_map_) + offset, size);

        stringstream ss(line);
        string temp, value;

        getline(ss, temp, ',');  // 版本号
        getline(ss, temp, ',');  // 键
        getline(ss, value);      // 值

        return value;
    }

private:
    struct WriteTask {
        unique_ptr<string> record_ptr;  // 使用unique_ptr存储字符串指针
        int fileID;
        size_t offset;
        size_t size;
    };

    string file_path_;
    const uint64_t MaxFileSize = 64 * 1024 * 1024; // 64MB
    moodycamel::BlockingConcurrentQueue<WriteTask, moodycamel::ConcurrentQueueDefaultTraits> write_queue_;
    thread write_thread_;
    atomic<int> current_fileID_;
    atomic<size_t> current_offset_;
    void* write_map_;
    void* read_map_;
    int64_t read_map_fileID_;
    atomic<bool> stop_flag_;
    int fileID = 0;  // 用于跟踪当前文件ID
    void OpenAndMapWriteFile() {
        string filename = file_path_ + to_string(fileID) + ".dat";

        // 打开或创建文件
        int fd = open(filename.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        if (fd == -1) {
            throw runtime_error("Cannot open or create file: " + filename);
        }

        // 确保文件至少有 MaxFileSize 大小
        ftruncate(fd, MaxFileSize);

        // 内存映射文件为写映射区域
        write_map_ = mmap(nullptr, MaxFileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (write_map_ == MAP_FAILED) {
            close(fd);
            throw runtime_error("Memory map for writing failed: " + filename);
        }

        close(fd);
    }

    void OpenAndMapReadFile(uint64_t fileID) {
        string filename = file_path_ + to_string(fileID) + ".dat";
        while (!filesystem::exists(filename)) {  // 需要 #include <filesystem>
            if (stop_flag_.load()) {
                throw runtime_error("File not found and stop flag set: " + filename);
            }
            this_thread::sleep_for(chrono::milliseconds(10));  // 短暂等待后重试
        }
        // 打开文件
        int fd = open(filename.c_str(), O_RDONLY);
        if (fd == -1) {
            throw runtime_error("Cannot open file for reading: " + filename);
        }

        // 直接映射MaxFileSize文件大小
        read_map_ = mmap(nullptr, MaxFileSize, PROT_READ, MAP_SHARED, fd, 0);
        if (read_map_ == MAP_FAILED) {
            close(fd);
            throw runtime_error("Memory map for reading failed: " + filename);
        }

        close(fd);
    }
};
#endif