#pragma once

#include <cstdlib>
#include <cstring>

template <typename T>
class ElementPool {
    public:
    ElementPool() = default;
    ~ElementPool() {
        if (pool_) {
            free(pool_);
            capacity_ = 0;
            size_ = 0;
        }
        for (auto p : garbage) {
            free(p);
        }
    }
    inline T* allocate() {
        if (size_ >= capacity_)
            reserve();
        return &pool_[size_++];
    }
    void reserve() {
        // std::cout << "Reserving " << capacity_ << std::endl;
        if (pool_) {
            garbage.push_back(pool_);
        }
        capacity_ = capacity_ == 0 ? 8192 : capacity_ * 2;
        size_ = 0;
        pool_ = (T*)malloc(capacity_ * sizeof(T));
        // memset(pool_, 0, capacity_ * sizeof(T));
        if (pool_ == nullptr) {
            exit(0);
        }
        // std::cout << "Reserved " << capacity_ << std::endl;
    }

    private:
    T* pool_ = nullptr;
    size_t capacity_ = 0;
    size_t size_ = 0;
    std::vector<T*> garbage;

};

class PagePool {
    public:
    PagePool() = default;
    ~PagePool() {
        if (pool_) {
            free(pool_);
            capacity_ = 0;
            size_ = 0;
        }
        for (auto p : garbage) {
            free(p);
        }
    }
    inline char* allocate() {
        if (size_ >= capacity_)
            reserve();
        return &pool_[(size_++) * PAGE_SIZE];
    }
    void reserve() {
        // std::cout << "Reserving " << capacity_ << std::endl;
        if (pool_) {
            garbage.push_back(pool_);
        }
        capacity_ = capacity_ == 0 ? 131072 : capacity_ * 2;
        size_ = 0;
        pool_ = (char*)aligned_alloc(4096, capacity_ * PAGE_SIZE);
        // memset(pool_, 0, capacity_ * sizeof(T));
        if (pool_ == nullptr) {
            throw std::runtime_error("Failed to allocate memory for PagePool");
            exit(0);
        }
        // std::cout << "Reserved " << capacity_ << std::endl;
    }

    private:
    char* pool_ = nullptr;
    size_t capacity_ = 0;
    size_t size_ = 0;
    std::vector<char*> garbage;

};