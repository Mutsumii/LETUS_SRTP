#ifndef _COMMON_H_
#define _COMMON_H_
#include <cstdint>
#include <filesystem>
#include <string>
#include <cstring>
#include <memory>

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
  alignas(4096) char data_[PAGE_SIZE]{};
  PageKey pagekey_;

 public:
  Page() {}

  Page(PageKey pagekey) : pagekey_(pagekey) {}

  Page(const Page& other){
    memcpy(data_, other.data_, PAGE_SIZE);
    pagekey_ = other.pagekey_;
  }

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
