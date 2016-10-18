/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_INTEREST_HELPER_HPP_
#define NDN_VSYNC_INTEREST_HELPER_HPP_

#include <iterator>
#include <limits>
#include <sstream>

#include <ndn-cxx/name.hpp>

#include "vsync-common.hpp"

namespace ndn {
namespace vsync {

inline std::string ToString(const ESN& s) {
  std::ostringstream os;
  os << "{(" << s.vi.first << ',' << s.vi.second << ")," << s.rn
     << ',' << s.seq << '}';
  return os.str();
}

// Helpers for version vector processing

inline VersionVector Merge(const VersionVector& v1, const VersionVector& v2) {
  if (v1.size() != v2.size())
    return {};

  VersionVector res(v1.size());
  std::transform(v1.begin(), v1.end(), v2.begin(), res.begin(),
                 [](uint64_t l, uint64_t r) { return std::max(l, r); });
  return res;
}

inline std::string ToString(const VersionVector& v) {
  std::string s(1, '[');
  s.append(std::accumulate(std::next(v.begin()), v.end(),
                           std::to_string(v[0]),
                           [](std::string a, uint8_t b) {
                             return a + ',' + std::to_string(b);
                           })).append(1, ']');
  return s;
}

// Helpers for sync Interest processing

inline Name MakeVsyncInterestName(const ViewID& vid, uint64_t rn,
                                  const VersionVector& vv) {
  // name = /vsync_prefix/view_num/leader_id/round_num/version_vector
  Name n(kVsyncPrefix);
  n.appendNumber(vid.first).append(vid.second)
    .appendNumber(rn).append(name::Component(vv.begin(), vv.end()));
  return n;
}

inline Name MakeViewInfoName(const ViewID& vid) {
  // name = /[vsync_prefix]/[view_num]/[leader_id]/vinfo
  Name n(kVsyncPrefix);
  n.appendNumber(vid.first).append(vid.second).append("vinfo");
  return n;
}

inline ViewID ExtractViewID(const Name& n) {
  uint64_t view_num = n.get(-4).toNumber();
  std::string leader_id = n.get(-3).toUri();
  return {view_num, leader_id};
}

inline ViewID ExtractViewIDFromViewInfoName(const Name& n) {
  uint64_t view_num = n.get(-3).toNumber();
  std::string leader_id = n.get(-2).toUri();
  return {view_num, leader_id};
}

inline uint64_t ExtractRoundNumber(const Name& n) {
  return n.get(-2).toNumber();
}

inline VersionVector ExtractVersionVector(const Name& n) {
  const auto& comp = n.get(-1);
  return VersionVector(comp.value_begin(), comp.value_end());
}

// Helpers for data processing

inline Name MakeDataName(const Name& prefix, const NodeID& nid, const ViewID& vid,
                         uint64_t rn, uint8_t seq) {
  // name = /node_prefix/node_id/view_num/leader_id/round_num/seq_num
  Name n(prefix);
  n.append(nid).appendNumber(vid.first).append(vid.second)
    .appendNumber(rn).appendNumber(seq);
  return n;
}

inline NodeID ExtractNodeID(const Name& n) {
  return n.get(-5).toUri();
}

inline uint64_t ExtractSequenceNumber(const Name& n) {
  return n.get(-1).toNumber();
}

inline void EncodeNumber(uint64_t num, std::vector<uint8_t>& out) {
  if (num <= std::numeric_limits<uint8_t>::max()) {
    out.push_back(1);
    out.push_back(static_cast<uint8_t>(num));
  } else if (num <= std::numeric_limits<uint16_t>::max()) {
    out.push_back(2);
    out.push_back(static_cast<uint8_t>(num >> 8));
    out.push_back(static_cast<uint8_t>(num));
  } else if (num <= std::numeric_limits<uint32_t>::max()) {
    out.push_back(4);
    out.push_back(static_cast<uint8_t>(num >> 24));
    out.push_back(static_cast<uint8_t>(num >> 16));
    out.push_back(static_cast<uint8_t>(num >> 8));
    out.push_back(static_cast<uint8_t>(num));
  } else {
    out.push_back(8);
    out.push_back(static_cast<uint8_t>(num >> 56));
    out.push_back(static_cast<uint8_t>(num >> 48));
    out.push_back(static_cast<uint8_t>(num >> 40));
    out.push_back(static_cast<uint8_t>(num >> 32));
    out.push_back(static_cast<uint8_t>(num >> 24));
    out.push_back(static_cast<uint8_t>(num >> 16));
    out.push_back(static_cast<uint8_t>(num >> 8));
    out.push_back(static_cast<uint8_t>(num));
  }
}

inline void EncodeString(const std::string& str,
                         std::vector<uint8_t>& out) {
  size_t l = str.size();
  if (l > std::numeric_limits<uint8_t>::max())
    throw "Cannot encode string longer than 255 bytes";
  out.push_back(l);
  std::copy(str.begin(), str.end(), std::back_inserter(out));
}

inline void EncodeLastDataInfo(const ESN& ldi,
                               std::vector<uint8_t>& out) {
  EncodeNumber(ldi.vi.first, out);
  EncodeString(ldi.vi.second, out);
  EncodeNumber(ldi.rn, out);
  EncodeNumber(ldi.seq, out);
}

inline size_t DecodeNumber(const uint8_t* buf, size_t buf_size,
                           uint64_t* num) {
  if (buf_size == 0) return 0;
  else if (buf[0] == 1) {
    if (buf_size < 2) return 0;
    *num = buf[1];
    return 2;
  } else if (buf[0] == 2) {
    if (buf_size < 3) return 0;
    *num = (static_cast<uint64_t>(buf[1]) << 8) + buf[2];
    return 3;
  } else if (buf[0] == 4) {
    if (buf_size < 5) return 0;
    *num = (static_cast<uint64_t>(buf[1]) << 24) +
      (static_cast<uint64_t>(buf[2]) << 16) +
      (static_cast<uint64_t>(buf[3]) << 8) + buf[4];
    return 5;
  } else if (buf[0] == 8) {
    if (buf_size < 9) return 0;
    *num = (static_cast<uint64_t>(buf[1]) << 56) +
      (static_cast<uint64_t>(buf[2]) << 48) +
      (static_cast<uint64_t>(buf[3]) << 40) +
      (static_cast<uint64_t>(buf[4]) << 32) +
      (static_cast<uint64_t>(buf[5]) << 24) +
      (static_cast<uint64_t>(buf[6]) << 16) +
      (static_cast<uint64_t>(buf[7]) << 8) + buf[8];
    return 9;
  } else {
    return 0;
  }
}

inline size_t DecodeString(const uint8_t* buf, size_t buf_size,
                           std::string* str) {
  if (buf_size == 0) return 0;
  size_t l = buf[0];
  if (buf_size < 1 + l) return 0;
  str->append(reinterpret_cast<const char*>(buf) + 1, l);
  return 1 + l;
}

inline std::pair<ESN, bool> DecodeLastDataInfo(const uint8_t* buf,
                                               size_t buf_size) {
  ESN esn;
  size_t r = DecodeNumber(buf, buf_size, &esn.vi.first);
  if (r == 0) return {};
  buf += r;
  buf_size -= r;
  r = DecodeString(buf, buf_size, &esn.vi.second);
  if (r == 0) return {};
  buf += r;
  buf_size -= r;
  r = DecodeNumber(buf, buf_size, &esn.rn);
  if (r == 0) return {};
  buf += r;
  buf_size -= r;
  r = DecodeNumber(buf, buf_size, &esn.seq);
  if (r == 0) return {};

  return {esn, true};
}

} // namespace vsync
} // namespace ndn

#endif // NDN_VSYNC_INTEREST_HELPER_HPP_
