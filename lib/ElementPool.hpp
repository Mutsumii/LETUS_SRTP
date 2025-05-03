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

    private:
    T* pool_ = nullptr;
    size_t capacity_ = 0;
    size_t size_ = 0;
    std::vector<T*> garbage;

    void reserve() {
        // std::cout << "Reserving " << capacity_ << std::endl;
        if (pool_) {
            garbage.push_back(pool_);
        }
        capacity_ = capacity_ == 0 ? 64 : capacity_ * 2;
        size_ = 0;
        pool_ = (T*)malloc(capacity_ * sizeof(T));
        memset(pool_, 0, capacity_ * sizeof(T));
        if (pool_ == nullptr) {
            exit(0);
        }
        // std::cout << "Reserved " << capacity_ << std::endl;
    }
};