/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "oblsm/util/ob_bloomfilter.h"
#include <mutex>
namespace oceanbase {
std::mutex m;
struct BKDRHash
{
  size_t operator()(const string &s)
  {
    size_t hash = 0;
    for (auto ch : s) {
      hash += ch;
      hash *= 31;
    }

    return hash;
  }
};

struct APHash
{
  size_t operator()(const string &s)
  {
    size_t hash = 0;
    for (long i = 0; i < s.size(); i++) {
      size_t ch = s[i];
      if ((i & 1) == 0) {
        hash ^= ((hash << 7) ^ ch ^ (hash >> 3));
      } else {
        hash ^= (~((hash << 11) ^ ch ^ (hash >> 5)));
      }
    }
    return hash;
  }
};

struct DJBHash
{
  size_t operator()(const string &s)
  {
    size_t hash = 5381;
    for (auto ch : s) {
      hash += (hash << 5) + ch;
    }
    return hash;
  }
};

ObBloomfilter::ObBloomfilter(size_t hash_func_count, size_t total_bits) : bits(total_bits), func_count(hash_func_count)
{
  for (size_t i = 0; i < total_bits; i++) {
    vec.push_back(0);
  }
};

void ObBloomfilter::insert(const string &object)
{
  std::lock_guard lock(m);
  // if (!ObBloomfilter::contains(object)) {
  size_t hash1 = APHash()(object) % ObBloomfilter::bits;
  size_t hash2 = DJBHash()(object) % ObBloomfilter::bits;
  size_t hash3 = BKDRHash()(object) % ObBloomfilter::bits;

  vec[hash1] = 1;
  vec[hash2] = 1;
  vec[hash3] = 1;
  // std::cout << hash1 << " " << hash2 << " " << hash3 << std::endl;
  ObBloomfilter::item_count++;
  // }
}

bool ObBloomfilter::contains(const string &object) const
{
  size_t hash1 = APHash()(object) % ObBloomfilter::bits;
  if (!vec[hash1] == 1) {
    return false;
  }
  size_t hash2 = DJBHash()(object) % ObBloomfilter::bits;
  if (!vec[hash2] == 1) {
    return false;
  }
  size_t hash3 = BKDRHash()(object) % ObBloomfilter::bits;
  if (!vec[hash3] == 1) {
    return false;
  }

  return true;
}
void ObBloomfilter::clear()
{

  for (size_t i = 0; i < ObBloomfilter::bits; i++) {
    vec[i] = 0;
  }
  ObBloomfilter::item_count = 0;
}
size_t ObBloomfilter::object_count() const { return ObBloomfilter::item_count; };
bool   ObBloomfilter::empty() const { return 0 == object_count(); }
}  // namespace oceanbase
