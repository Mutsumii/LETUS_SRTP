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
        {
            lock_guard<mutex> lock(mtx_);
            stop_flag_ = true; // 设置停止标志
        }
        cv_.notify_all(); // 唤醒可能在等待的写线程
        if (write_thread_.joinable()) {
            write_thread_.join();
        }
        if (write_map_ != MAP_FAILED) {
            //最后刷盘
            msync(write_map_, MaxFileSize, MS_SYNC);
            munmap(write_map_, MaxFileSize);
        }
        if (read_map_ != MAP_FAILED) {
            munmap(read_map_, MaxFileSize);
        }

    }

    // 工作线程处理写入请求，使用unique_ptr避免字符串拷贝
    tuple<int, size_t, size_t> WriteValue(uint64_t version, const string& key, const string& value) {
        auto record_ptr = make_unique<string>(to_string(version) + "," + key + "," + value + "\n");
        size_t record_size = record_ptr->size();

        int current_fileID;
        size_t offset;
    
        // 原子地计算文件ID和偏移量，处理文件切换
        while (true) {
            current_fileID = current_fileID_.load();
            offset = current_offset_.fetch_add(record_size);

            // 检查是否需要切换文件
            if (offset + record_size <= MaxFileSize) {
                break; // 无需切换，直接使用当前文件
            }

            // 尝试切换文件
            int new_fileID = current_fileID + 1;
            if (current_fileID_.compare_exchange_weak(current_fileID, new_fileID)) {
                // 切换成功，重置偏移量
                current_offset_.store(record_size);
                offset = 0;
                current_fileID = new_fileID;
                break;
            }
        }

        // 写入队列
        {
            lock_guard<mutex> lock(mtx_);
            write_queue_.push({ move(record_ptr), current_fileID, offset, record_size });
            cv_.notify_one();
        }

        return make_tuple(current_fileID, offset, record_size);
    }
    // 后台线程处理写入任务
    void WriteThreadValue() {
        int fileID = 0;
        while (!stop_flag_) {
            WriteTask task;
            {
                unique_lock<mutex> lock(mtx_);
                // 等待任务队列非空
                cv_.wait(lock, [this] { return !write_queue_.empty() || stop_flag_; });
                if (stop_flag_) break;
                
                task = move(write_queue_.front());
                write_queue_.pop();
            }
            
            // 写入数据到映射内存
            if (task.fileID != fileID) {
                // 如果当前文件ID和任务的文件ID不一致，说明需要切换文件
                if (write_map_ != MAP_FAILED) {
                    //刷盘操作
                    msync(write_map_, MaxFileSize, MS_SYNC);
                    // 解除映射
                    munmap(write_map_, MaxFileSize);
                }
                fileID = task.fileID;
                // 打开新文件并映射
                OpenAndMapWriteFile();
            }
            memcpy(static_cast<char*>(write_map_) + task.offset, 
                   task.record_ptr->c_str(), 
                   task.size);
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
    const uint64_t MaxFileSize = 64 * 1024 * 1024;
    queue<WriteTask> write_queue_;// 写入任务队列
    thread write_thread_;
    // 使用原子变量来跟踪当前文件ID和偏移量
    atomic<int> current_fileID_;
    atomic<size_t> current_offset_;
    void* write_map_;
    void* read_map_;
    int64_t read_map_fileID_;
    mutex mtx_;
    condition_variable cv_;// 用于通知写线程
    atomic<bool> stop_flag_;

    void OpenAndMapWriteFile() {
        string filename = file_path_ + to_string(current_fileID_) + ".dat";

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

        // 关闭文件描述符，因为已经映射了文件
        close(fd);
    }

    void OpenAndMapReadFile(uint64_t fileID) {
        string filename = file_path_ + to_string(fileID) + ".dat";

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

        // 关闭文件描述符，因为已经映射了文件
        close(fd);
    }
};
#endif