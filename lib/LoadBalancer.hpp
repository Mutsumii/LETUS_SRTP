#include <vector>
#include <algorithm>
#include <numeric>
#include <climits>
#include <iostream>
#include <queue>
#include <random>
#include <chrono>
#include <tuple>

using namespace std;

// 带标记的数据结构
struct TaggedInt {
    size_t value;
    uint8_t original_region; // 原始组号
    uint8_t nibble_value;
    
    // 用于排序的比较函数
    bool operator>(const TaggedInt& other) const {
        return value > other.value;
    }
};

// 比较函数，用于降序排序
bool compareDesc(const TaggedInt& a, const TaggedInt& b) {
    return a > b;
}

// 贪心算法实现
vector<vector<TaggedInt>> greedyPartition(vector<TaggedInt>& nums, int n) {
    // 降序排序有助于更均匀分配
    sort(nums.begin(), nums.end(), compareDesc);
    
    // 使用最小堆来跟踪当前和最小的组
    priority_queue<pair<int, int>, vector<pair<int, int>>, greater<pair<int, int>>> minHeap;
    
    // 初始化n个组
    vector<vector<TaggedInt>> groups(n);
    vector<int> sums(n, 0);
    
    for (int i = 0; i < n; ++i) {
        minHeap.push({0, i});
    }
    
    // 分配数字到当前和最小的组
    for (const auto& num : nums) {
        auto [currentSum, groupIdx] = minHeap.top();
        minHeap.pop();
        
        groups[groupIdx].push_back(num);
        sums[groupIdx] += num.value;
        
        minHeap.push({sums[groupIdx], groupIdx});
    }
    
    return groups;
}

// 优化函数：尝试交换元素来减少最大差异
void optimizePartition(vector<TaggedInt>& nums, vector<vector<TaggedInt>>& groups, vector<int>& sums, int maxIterations = 1000) {
    int n = groups.size();
    int bestDiff = *max_element(sums.begin(), sums.end()) - *min_element(sums.begin(), sums.end());
    
    for (int iter = 0; iter < maxIterations; ++iter) {
        // 找到当前和最大和最小的组
        int maxIdx = max_element(sums.begin(), sums.end()) - sums.begin();
        int minIdx = min_element(sums.begin(), sums.end()) - sums.begin();
        
        // 尝试从最大组中找一个元素放到最小组
        for (size_t i = 0; i < groups[maxIdx].size(); ++i) {
            const auto& num = groups[maxIdx][i];
            
            // 计算交换后的新和
            int newMaxSum = sums[maxIdx] - num.value;
            int newMinSum = sums[minIdx] + num.value;
            
            // 如果交换能减少最大差异
            if (max(newMaxSum, newMinSum) - min(newMaxSum, newMinSum) < bestDiff) {
                // 执行交换
                auto moved_num = groups[maxIdx][i];
                groups[maxIdx].erase(groups[maxIdx].begin() + i);
                groups[minIdx].push_back(moved_num);
                sums[maxIdx] = newMaxSum;
                sums[minIdx] = newMinSum;
                bestDiff = *max_element(sums.begin(), sums.end()) - *min_element(sums.begin(), sums.end());
                break; // 找到一个改进就跳出循环
            }
        }
        
        // 如果没有改进，提前终止
        if (*max_element(sums.begin(), sums.end()) - *min_element(sums.begin(), sums.end()) >= bestDiff) {
            break;
        }
    }
}

// 主函数
vector<vector<TaggedInt>> partitionIntoNGroups(vector<TaggedInt>& nums, int n) {
    if (nums.empty() || n <= 0) return {};
    if (n == 1) return {nums};
    
    // 第一步：使用贪心算法获得初始解
    auto groups = greedyPartition(nums, n);
    
    // 计算各组和
    vector<int> sums(n, 0);
    for (int i = 0; i < n; ++i) {
        for (const auto& num : groups[i]) {
            sums[i] += num.value;
        }
    }
    
    // 第二步：使用优化算法改进解
    optimizePartition(nums, groups, sums, 2000); // 增加迭代次数
    
    return groups;
}

// // 生成不均匀测试数据
// vector<TaggedInt> generateUnevenData(int size, int original_groups) {
//     vector<TaggedInt> data(size);
//     random_device rd;
//     mt19937 gen(rd());
    
//     // 90%的小数 (1-100)
//     uniform_int_distribution<int> small_dist(1, 100);
//     // 10%的大数 (1000-10000)
//     uniform_int_distribution<int> large_dist(1000, 10000);
//     // 原始组号
//     uniform_int_distribution<int> group_dist(0, original_groups-1);
    
//     for (int i = 0; i < size; ++i) {
//         if (i % 10 == 0) { // 10%的大数
//             data[i].value = large_dist(gen);
//         } else {
//             data[i].value = small_dist(gen);
//         }
//         data[i].original_region = group_dist(gen);
//     }
    
//     return data;
// }

// // 打印结果
// void printResult(const vector<vector<TaggedInt>>& groups) {
//     vector<int> sums;
//     vector<vector<int>> original_groups_composition(groups.size());
    
//     for (const auto& group : groups) {
//         int sum = 0;
//         vector<int> original_counts(groups.size(), 0);
        
//         for (const auto& num : group) {
//             sum += num.value;
//             original_counts[num.original_region]++;
//         }
        
//         sums.push_back(sum);
        
//         cout << "Group (Sum=" << sum << ", Size=" << group.size() << "):\n";
//         cout << "  Original groups composition: ";
//         for (size_t i = 0; i < original_counts.size(); ++i) {
//             if (original_counts[i] > 0) {
//                 cout << "G" << i << ":" << original_counts[i] << " ";
//             }
//         }
//         cout << "\n  Values: ";
        
//         // 只打印前5个和后5个元素，避免输出太长
//         int print_count = min(5, static_cast<int>(group.size()));
//         for (int i = 0; i < print_count; ++i) {
//             cout << group[i].value << "(" << group[i].original_region << ") ";
//         }
//         if (group.size() > 10) {
//             cout << "... ";
//             for (int i = group.size()-5; i < group.size(); ++i) {
//                 cout << group[i].value << "(" << group[i].original_region << ") ";
//             }
//         } else if (group.size() > 5) {
//             for (int i = 5; i < group.size(); ++i) {
//                 cout << group[i].value << "(" << group[i].original_region << ") ";
//             }
//         }
//         cout << endl << endl;
//     }
    
//     int maxSum = *max_element(sums.begin(), sums.end());
//     int minSum = *min_element(sums.begin(), sums.end());
//     cout << "Maximum difference between groups: " << maxSum - minSum << endl;
    
//     // 计算标准差
//     double mean = accumulate(sums.begin(), sums.end(), 0.0) / sums.size();
//     double variance = 0.0;
//     for (int s : sums) {
//         variance += pow(s - mean, 2);
//     }
//     variance /= sums.size();
//     cout << "Standard deviation of group sums: " << sqrt(variance) << endl;
    
//     // 计算原始组保留率
//     vector<int> original_group_sizes(groups.size(), 0);
//     vector<int> preserved_counts(groups.size(), 0);
//     for (const auto& group : groups) {
//         for (const auto& num : group) {
//             original_group_sizes[num.original_region]++;
//             if (num.original_region == &group - &groups[0]) {
//                 preserved_counts[num.original_region]++;
//             }
//         }
//     }
    
//     cout << "\nOriginal group preservation rates:\n";
//     for (size_t i = 0; i < original_group_sizes.size(); ++i) {
//         if (original_group_sizes[i] > 0) {
//             double rate = static_cast<double>(preserved_counts[i]) / original_group_sizes[i];
//             cout << "Group " << i << ": " << (rate * 100) << "% preserved (" 
//                  << preserved_counts[i] << "/" << original_group_sizes[i] << ")\n";
//         }
//     }
// }

// int main() {
//     // 生成不均匀测试数据 (256个元素，包含10%的大数)
//     // 假设原始有4个组
//     int original_groups = 4;
//     auto nums = generateUnevenData(256, original_groups);
    
//     // 计算总和和平均值
//     int total_sum = 0;
//     for (const auto& num : nums) {
//         total_sum += num.value;
//     }
//     double average_per_group = total_sum / 8.0;
//     cout << "Total sum: " << total_sum << endl;
//     cout << "Average per group (ideal): " << average_per_group << "\n\n";
    
//     int n = 8; // 分成8组
    
//     auto start = chrono::high_resolution_clock::now();
//     auto result = partitionIntoNGroups(nums, n);
//     auto end = chrono::high_resolution_clock::now();
    
//     cout << "Partition Result with Tagged Uneven Data:\n";
//     printResult(result);
    
//     auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
//     cout << "\nTime taken: " << duration.count() << " ms" << endl;
    
//     return 0;
// }