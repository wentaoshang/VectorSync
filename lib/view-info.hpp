/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_VIEW_INFO_HPP_
#define NDN_VSYNC_VIEW_INFO_HPP_

#include <algorithm>
#include <exception>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ndn-cxx/name.hpp>

#include "vsync-helper.hpp"

namespace ndn {
namespace vsync {

struct MemberInfo {
  NodeID id;
  Name prefix;

  friend bool operator==(const MemberInfo& l, const MemberInfo& r) {
    return l.id == r.id && l.prefix == r.prefix;
  }
};

class ViewInfo {
 public:
  class Error : public std::exception {
   public:
    Error(const std::string& what) : what_(what) {}

    virtual const char* what() const noexcept override { return what_.c_str(); }

   private:
    std::string what_;
  };

  /**
   * @brief Creates empty view info
   */
  ViewInfo() {}

  /**
   * @brief Creates view info with members from @p member_list
   * @param uri The member info list in this view.
   */
  explicit ViewInfo(const std::vector<MemberInfo>& member_list)
      : member_list_(member_list) {
    for (auto& m : member_list_) {
      if (m.id.empty()) throw Error("Empty node ID in member list");
      // Convert node ID to a string that conforms to NDN URI scheme
      m.id = name::Component(m.id).toUri();
      // TODO: support encoding of node id longer than 255 bytes
      if (m.id.size() > 255)
        throw Error("Node ID longer than 255 bytes: " + m.id);
      // TODO: support encoding of node prefix longer than 255 bytes
      const auto& pfx = m.prefix.toUri();
      if (pfx.size() > 255)
        throw Error("Node prefix URI longger than 255 bytes: " + pfx);
    }

    for (std::size_t i = 0; i != member_list_.size(); ++i) {
      const auto& id = member_list_[i].id;
      auto r = member_index_map_.insert({id, i});
      if (!r.second) throw Error("Duplicate node ID in member list");
    }
  }

  std::pair<NodeIndex, bool> GetIndexByID(const NodeID& nid) const {
    auto iter = member_index_map_.find(nid);
    if (iter == member_index_map_.end())
      return {};
    else
      return {iter->second, true};
  }

  std::pair<NodeID, bool> GetIDByIndex(NodeIndex idx) const {
    if (idx >= member_list_.size())
      return {};
    else
      return {member_list_[idx].id, true};
  }

  std::pair<Name, bool> GetPrefixByIndex(NodeIndex idx) const {
    if (idx >= member_list_.size())
      return {};
    else
      return {member_list_[idx].prefix, true};
  }

  std::size_t Size() const { return member_list_.size(); }

  void Encode(std::string& out) const {
    proto::ViewInfo vinfo_proto;
    for (const auto& m : member_list_) {
      auto* entry = vinfo_proto.add_entry();
      entry->set_id(m.id);
      entry->set_prefix(m.prefix.toUri());
    }
    vinfo_proto.AppendToString(&out);
  }

  bool Decode(const void* buf, size_t buf_size) {
    proto::ViewInfo vinfo_proto;
    if (!vinfo_proto.ParseFromArray(buf, buf_size)) return false;

    std::vector<MemberInfo> minfo;
    std::unordered_map<NodeID, NodeIndex> index_map;

    for (int idx = 0; idx < vinfo_proto.entry_size(); ++idx) {
      const auto& entry = vinfo_proto.entry(idx);
      if (entry.id().empty()) return false;
      if (index_map.find(entry.id()) != index_map.end()) return false;
      Name prefix(entry.prefix());
      index_map[entry.id()] = idx;
      minfo.push_back({entry.id(), prefix});
    }

    member_list_ = std::move(minfo);
    member_index_map_ = std::move(index_map);
    return true;
  }

  bool Merge(const ViewInfo& vinfo) {
    auto old_size = Size();
    for (const auto& m : vinfo.member_list_) {
      if (member_index_map_.find(m.id) != member_index_map_.end())
        // Node id already exists
        continue;
      member_list_.push_back(m);
      member_index_map_[m.id] = member_list_.size() - 1;
    }
    return old_size != Size();
  }

  void Remove(const std::unordered_set<NodeID>& ids) {
    auto iter = std::remove_if(
        member_list_.begin(), member_list_.end(),
        [&ids](const MemberInfo& m) { return ids.find(m.id) != ids.end(); });
    if (iter == member_list_.end()) return;
    member_list_.erase(iter, member_list_.end());

    // Rebuild index map
    member_index_map_.clear();
    for (std::size_t i = 0; i != member_list_.size(); ++i) {
      const auto& id = member_list_[i].id;
      member_index_map_[id] = i;
    }
  }

  friend bool operator==(const ViewInfo& lv, const ViewInfo& rv) {
    return lv.member_list_ == rv.member_list_ &&
           lv.member_index_map_ == rv.member_index_map_;
  }

  friend std::ostream& operator<<(std::ostream& os, const ViewInfo& vinfo) {
    os << "ViewInfo(size=" << vinfo.Size();
    for (size_t i = 0; i != vinfo.member_list_.size(); ++i) {
      const auto& m = vinfo.member_list_[i];
      auto iter = vinfo.member_index_map_.find(m.id);
      if (iter == vinfo.member_index_map_.end())
        throw Error("Cannot find node index for ID " + m.id);
      os << "," << iter->second << "={" << m.id << "," << m.prefix.toUri()
         << "}";
    }
    return os << ")";
  }

 private:
  std::vector<MemberInfo> member_list_;
  std::unordered_map<NodeID, NodeIndex> member_index_map_;
};

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_VIEW_INFO_HPP_
