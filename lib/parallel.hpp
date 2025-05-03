#pragma once

#include <iostream>
#include <list>
#include <memory>
#include <string>
#include "common.hpp"
#include <thread>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <map>
#include <unordered_map>
#include <vector>
#include <functional>
#include <algorithm>
#include <stdexcept>

// #define LOG
// #define COLOR
#define USE_THIRD_PARTY_LIBRARY

#ifdef LOG
#define MASTER_LOG
#define REGION_LOG
#define JOINER_LOG
#else
// #define MASTER_LOG
// #define REGION_LOG
// #define JOINER_LOG
    // #define TIMESTAMP_LOG
#endif

#ifdef COLOR
    #define RESET   "\033[0m"      // 重置所有属性
    #define RED     "\033[31m"     // 红色
    #define GREEN   "\033[32m"     // 绿色
    #define YELLOW  "\033[33m"     // 黄色
    #define BLUE    "\033[34m"     // 蓝色
    #define MAGENTA "\033[35m"     // 洋红色
    #define CYAN    "\033[36m"     // 青色
    #define WHITE   "\033[37m"     // 白色
#else
    #define RESET   ""      // 重置所有属性
    #define RED     ""     // 红色
    #define GREEN   ""     // 绿色
    #define YELLOW  ""     // 黄色
    #define BLUE    ""     // 蓝色
    #define MAGENTA ""     // 洋红色
    #define CYAN    ""     // 青色
    #define WHITE   ""     // 白色
#endif

class Master;
class Region;
class Joiner;
class DMMTrie;
class BasePage;

class BufferItem {
    public:
    BufferItem(
        std::tuple<uint64_t, uint64_t, uint64_t> location,
        const std::string value, const std::string nibbles,
        const std::string child_hash) : location_(location), value_(std::move(value)),
        nibbles_(std::move(nibbles)), child_hash_(std::move(child_hash)) {
    }

    std::string ToString() {
        return "BufferItem: " + std::to_string(std::get<0>(location_)) + "-" +
            std::to_string(std::get<1>(location_)) + "-" +
            std::to_string(std::get<2>(location_)) + "-" + value_ + "-" +
            nibbles_;
    }

    std::tuple<uint64_t, uint64_t, uint64_t> location_;
    std::string value_;
    std::string nibbles_;
    std::string child_hash_;
};

template <typename T>
class ConcurrentArray {
public:
    explicit ConcurrentArray(size_t capacity=32) 
        : capacity_(capacity),
          buffer_(std::make_unique<Node[]>(capacity + 1)),  // 多分配一个空间用于区分空和满
          head_(0),
          tail_(0) {}

    // 生产者线程调用，将元素放入队列尾部
    bool push_back(const T& value) {
        size_t tail = tail_.load(std::memory_order_relaxed);
        size_t next_tail = next_index(tail);
        
        // 检查队列是否已满
        if (next_tail == head_.load(std::memory_order_acquire)) {
            std::cout << "Queue is full" << std::endl;
            return false;  // 队列已满
        }
        
        buffer_[tail].value = value;
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    // 消费者线程调用，从队列头部取出元素
    bool pop_front() {
        size_t head = head_.load(std::memory_order_relaxed);
        
        // 检查队列是否为空
        if (head == tail_.load(std::memory_order_acquire)) {
            return false;  // 队列为空
        }

        head_.store(next_index(head), std::memory_order_release);
        return true;
    }

    // 查看队列头部元素（不删除），队列为空时抛出异常
    T& front() {
        size_t head = head_.load(std::memory_order_relaxed);
        
        if (head == tail_.load(std::memory_order_acquire)) {
            throw std::runtime_error("Queue is empty");
        }
        
        return buffer_[head].value;
    }

    // 查看队列尾部元素（不删除），队列为空时抛出异常
    T& back() {
        size_t tail = tail_.load(std::memory_order_relaxed);
        
        if (head_.load(std::memory_order_acquire) == tail) {
            throw std::runtime_error("Queue is empty");
        }
        
        size_t last = prev_index(tail);
        return buffer_[last].value;
    }

    // 返回队列中元素的数量（近似值）
    size_t size() const {
        size_t head = head_.load(std::memory_order_acquire);
    size_t tail = tail_.load(std::memory_order_acquire);   
        
        if (tail >= head) {
            return tail - head;
        }
        return capacity_ + 1 - (head - tail);
    }

    bool empty() const {
        return head_.load(std::memory_order_acquire) == 
               tail_.load(std::memory_order_acquire);
    }

private:
    struct alignas(64) Node {  // 缓存行对齐，避免伪共享
        T value;
    };

    size_t next_index(size_t idx) const {
        return (idx + 1) % (capacity_ + 1);
    }

    size_t prev_index(size_t idx) const {
        return (idx == 0) ? capacity_ : (idx - 1);
    }

    const size_t capacity_;
    std::unique_ptr<Node[]> buffer_;
    
    // 确保head和tail位于不同的缓存行
    alignas(64) std::atomic<size_t> head_;  // 消费者访问
    alignas(64) std::atomic<size_t> tail_;   // 生产者访问
};

#include <array>

inline std::string dec_to_hex2_fast(uint8_t dec) {
    static constexpr std::array<const char*, 256> table = {
        "00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f",
        "10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f",
        "20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f",
        "30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f",
        "40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f",
        "50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f",
        "60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f",
        "70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f",
        "80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f",
        "90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f",
        "a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af",
        "b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf",
        "c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf",
        "d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df",
        "e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef",
        "f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"
    };
    return table[dec];
}

inline std::string HashToString(const std::string& hash) {
    std::string result;
    if (hash.empty()) {
        return "null";
    }
    for (size_t i = 0; i < hash.size(); ++i) {
        result += dec_to_hex2_fast(static_cast<uint8_t>(hash[i]));
        if (i != hash.size() - 1) {
            result += ":";
        }
    }
    return result;
    
}

#include "Timestamp.hpp"