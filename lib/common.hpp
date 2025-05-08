#ifndef _COMMON_H_
#define _COMMON_H_
#include <cstdint>
#include <filesystem>
#include <string>
#include <cstring>
#include <memory>
#include <cstdlib>
#include <cstring>
#include <new>

static constexpr int PAGE_SIZE = 12288;  // 每个页面的大小为12KB

// PageKey结构体
struct PageKey {
  uint64_t version;
  uint64_t tid;
  bool type;        // basepage(false)或deltapage(true)
  std::string pid;  // nibble，例如"Alice"中的"Al"

  std::string ToString() {
    return "PageKey: [Version] " + std::to_string(version) + " - [Type] " + std::to_string(type) + " - [PID] " + pid;
  }

  // 重载比较运算符
  bool operator<(const PageKey& other) const {
    if (version != other.version) return version < other.version;
    if (tid != other.tid) return tid < other.tid;
    if (type != other.type) return type < other.type;
    return pid < other.pid;
  }

  bool operator==(const PageKey& other) const {
    return version == other.version && tid == other.tid && type == other.type &&
           pid == other.pid;
  }

  bool operator>(const PageKey& other) const { return other < *this; }

  bool operator!=(const PageKey& other) const { return !(*this == other); }

  bool operator<=(const PageKey& other) const {
    return *this < other || *this == other;
  }

  bool operator>=(const PageKey& other) const {
    return *this > other || *this == other;
  }

  // 添加哈希函数支持
  struct Hash {
    size_t operator()(const PageKey& key) const {
      size_t h1 = std::hash<uint64_t>{}(key.version);
      size_t h2 = std::hash<int>{}(key.tid);
      size_t h3 = std::hash<bool>{}(key.type);
      size_t h4 = std::hash<std::string>{}(key.pid);

      return h1 ^ (h2 << 1) ^ (h3 << 2) ^ (h4 << 3);
    }
  };

  // 序列化函数
  bool SerializeTo(std::ostream& out) const {
    try {
      out.write(reinterpret_cast<const char*>(&version), sizeof(version));
      out.write(reinterpret_cast<const char*>(&tid), sizeof(tid));
      out.write(reinterpret_cast<const char*>(&type), sizeof(type));

      size_t pid_size = pid.size();
      out.write(reinterpret_cast<const char*>(&pid_size), sizeof(pid_size));
      out.write(pid.data(), pid_size);
      
      return out.good();
    } catch (const std::exception&) {
      return false;
    }
  }

  // 反序列化函数
  bool Deserialize(std::istream& in) {
    try {
      in.read(reinterpret_cast<char*>(&version), sizeof(version));
      in.read(reinterpret_cast<char*>(&tid), sizeof(tid));
      in.read(reinterpret_cast<char*>(&type), sizeof(type));

      size_t pid_size;
      in.read(reinterpret_cast<char*>(&pid_size), sizeof(pid_size));
      
      if (pid_size > 256) { //pid maybe ""
        return false;
      }
      
      pid.resize(pid_size);
      in.read(&pid[0], pid_size);
      
      return in.good();
    } catch (const std::exception&) {
      return false;
    }
  }
};

namespace std {
  template<>
  struct hash<PageKey> {
      size_t operator()(const PageKey& key) const {
          // 组合各个成员的哈希值
          size_t h1 = std::hash<std::string>{}(key.pid);
          size_t h2 = std::hash<std::uint64_t>{}(key.version);
          size_t h3 = std::hash<std::uint64_t>{}(key.tid);
          // 更多成员...

          // 使用一个简单的方法组合哈希值（boost::hash_combine 是更好的选择）
          return h1 ^ (h2 << 1) ^ (h3 << 2) ^ key.type;
      }
  };
}

// 页面类
class Page {  //设置成抽象类 序列化 反序列化 getPageKey setPageKey 子类
              // DMMTriePage DeltaPage

 private:
//  alignas(4096) char internal[PAGE_SIZE]{};
alignas(4096) char* data_ = nullptr;
 PageKey pagekey_;
 bool owns_data_ = false;

 static char* allocate_aligned() {
  return static_cast<char*>(std::aligned_alloc(4096, PAGE_SIZE));
}

static void free_aligned(char* ptr) {
  std::free(ptr);
}

  public:
  // 默认构造：动态分配对齐内存
  Page() : data_(allocate_aligned()), owns_data_(true) {
    std::memset(data_, 0, PAGE_SIZE);
}

// 构造时传入 PageKey
Page(PageKey pagekey) : data_(allocate_aligned()), owns_data_(true), pagekey_(pagekey) {
    std::memset(data_, 0, PAGE_SIZE);
}

// 构造时传入外部数据（不分配内存）
Page(char* external_data, PageKey pagekey) : data_(external_data), owns_data_(false), pagekey_(pagekey) {}

// 析构：仅释放拥有的数据
~Page() {
    if (owns_data_) {
        free_aligned(data_);
    }
}

Page(const Page& other) : owns_data_(true), pagekey_(other.pagekey_) {
      // 深拷贝内部数据
      data_ = allocate_aligned();
      std::memcpy(data_, other.data_, PAGE_SIZE);

}
// 拷贝赋值
Page& operator=(const Page& other) {
  if (this != &other) {
      // 释放原有数据
      if (owns_data_) {
          free_aligned(data_);
      }

      // 复制数据和状态
      owns_data_ = other.owns_data_;
      pagekey_ = other.pagekey_;
      // if (other.owns_data_) {
          data_ = allocate_aligned();
          std::memcpy(data_, other.data_, PAGE_SIZE);
      // } else {
      //     data_ = other.data_;
      // }
  }
  return *this;
}


// 移动构造
Page(Page&& other) noexcept 
    : data_(other.data_), owns_data_(other.owns_data_), pagekey_(other.pagekey_) {
    other.owns_data_ = false; // 防止双重释放
}

// 移动赋值
Page& operator=(Page&& other) noexcept {
    if (this != &other) {
        if (owns_data_) {
            free_aligned(data_);
        }
        data_ = other.data_;
        owns_data_ = other.owns_data_;
        pagekey_ = other.pagekey_;
        other.owns_data_ = false;
    }
    return *this;
}
//   Page() {
//   // data_ = static_cast<char*>(::operator new(PAGE_SIZE, std::align_val_t{PAGE_SIZE}));
//    // data_ = new char[PAGE_SIZE];
//   // memset(data_, 0, PAGE_SIZE);
//  }

//  Page(PageKey pagekey) : pagekey_(pagekey) {
//   // data_ = static_cast<char*>(::operator new(PAGE_SIZE, std::align_val_t{PAGE_SIZE}));
//    // data_ = new char[PAGE_SIZE];
//     // memset(data_, 0, PAGE_SIZE);
//   }

//   // Page(char* data) {
//   //   data_ = data;
//   // }

//   Page(const Page& other) {
//     // data_ = new char[PAGE_SIZE];
//     memcpy(data_, other.data_, PAGE_SIZE);
//     pagekey_ = other.pagekey_;
//   }

  const PageKey& GetPageKey() const {
    // std::cout << "[GetPageKey]" << pagekey_.pid << std::endl;
    return pagekey_;
  }

  // virtual size_t GetSerializedSize() = 0;

  virtual bool SerializeTo(std::ostream& out) const { return true; }

  virtual bool Deserialize(std::istream& in) { 
    try {
      in.read(data_, PAGE_SIZE);
      return in.good();
    } catch (const std::exception&) {
      return false;
    }
  }

  void SetPageKey(const PageKey& key) { pagekey_ = key; }

  const char* GetData() const { return data_; }

  char* GetData() { return data_; }
};
inline std::ostream &operator<<(std::ostream &os, const PageKey &key) {
  os << "PageKey(version=" << key.version << ", tid=" << key.tid << ", type=" << key.type
    << ", pid=" << key.pid << ")";
  return os;
}
#endif
