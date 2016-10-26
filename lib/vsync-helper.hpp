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
                           [](std::string a, uint64_t b) {
                             return a + ',' + std::to_string(b);
                           })).append(1, ']');
  return s;
}

inline void EncodeVV(const VersionVector& v, std::string& out) {
  proto::VV vv_proto;
  for (uint64_t n : v) {
    vv_proto.add_entry(n);
  }
  vv_proto.AppendToString(&out);
}

inline VersionVector DecodeVV(const void* buf, size_t buf_size) {
  proto::VV vv_proto;
  if (!vv_proto.ParseFromArray(buf, buf_size)) return {};

  VersionVector vv(vv_proto.entry_size(), 0);
  for (int i = 0; i < vv_proto.entry_size(); ++i) {
    vv[i] = vv_proto.entry(i);
  }
  return vv;
}

// Helpers for sync Interest processing

inline Name MakeVsyncInterestName(const ViewID& vid,
                                  const VersionVector& vv) {
  // name = /[vsync_prefix]/[view_num]/[leader_id]/[version_vector]
  std::string vv_encode;
  EncodeVV(vv, vv_encode);
  Name n(kVsyncPrefix);
  n.appendNumber(vid.first).append(vid.second)
    .append(reinterpret_cast<const uint8_t*>(vv_encode.data()), vv_encode.size());
  return n;
}

inline Name MakeViewInfoName(const ViewID& vid) {
  // name = /[vsync_prefix]/[view_num]/[leader_id]/vinfo
  Name n(kVsyncPrefix);
  n.appendNumber(vid.first).append(vid.second).append("vinfo");
  return n;
}

inline ViewID ExtractViewID(const Name& n) {
  uint64_t view_num = n.get(-3).toNumber();
  std::string leader_id = n.get(-2).toUri();
  return {view_num, leader_id};
}

inline VersionVector ExtractVersionVector(const Name& n) {
  const auto& comp = n.get(-1);
  return DecodeVV(comp.value(), comp.value_size());
}

// Helpers for data processing

inline Name MakeDataName(const Name& prefix, const NodeID& nid,
                         const ViewID& vid, uint64_t seq) {
  // name = /node_prefix/node_id/view_num/leader_id/seq_num
  Name n(prefix);
  n.append(nid).appendNumber(vid.first).append(vid.second).appendNumber(seq);
  return n;
}

inline NodeID ExtractNodeID(const Name& n) {
  return n.get(-4).toUri();
}

inline uint64_t ExtractSequenceNumber(const Name& n) {
  return n.get(-1).toNumber();
}

inline void EncodeESN(const ESN& ldi,
                      std::string& out) {
  proto::ESN esn_proto;
  esn_proto.set_view_num(ldi.vi.first);
  esn_proto.set_leader_id(ldi.vi.second);
  esn_proto.set_seq_num(ldi.seq);
  esn_proto.AppendToString(&out);
}

inline std::pair<ESN, bool> DecodeESN(const void* buf,
                                      size_t buf_size) {
  proto::ESN esn_proto;
  if (!esn_proto.ParseFromArray(buf, buf_size))
    return {};

  ESN esn;
  esn.vi.first = esn_proto.view_num();
  esn.vi.second = esn_proto.leader_id();
  esn.seq = esn_proto.seq_num();
  return {esn, true};
}

inline std::string ToString(const ESN& s) {
  std::ostringstream os;
  os << "{(" << s.vi.first << ',' << s.vi.second << ")," << s.seq << '}';
  return os.str();
}

} // namespace vsync
} // namespace ndn

#endif // NDN_VSYNC_INTEREST_HELPER_HPP_
