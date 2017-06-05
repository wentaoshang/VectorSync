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
  Name nid;
  // TODO: add node certificate

  friend bool operator==(const MemberInfo& l, const MemberInfo& r) {
    return l.nid == r.nid;
  }
};

struct MemberInfoCompare {
  bool operator()(const MemberInfo& l, const MemberInfo& r) const {
    return l.nid < r.nid;
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
      if (m.nid.empty()) throw Error("Node name empty");

      const auto& n = m.nid.toUri();
      // TODO: support encoding of node id longer than 255 bytes
      if (n.size() > 255) throw Error("Node name longer than 255 bytes: " + n);
    }

    // Sort members by name in ascending order
    MemberInfoCompare mi_comp;
    std::sort(member_list_.begin(), member_list_.end(), mi_comp);

    // Build index map
    for (std::size_t i = 0; i != member_list_.size(); ++i) {
      const auto& nid = member_list_[i].nid;
      auto r = member_index_map_.insert({nid, i});
      if (!r.second) throw Error("Duplicate node name in member list");
    }
  }

  std::pair<std::size_t, bool> GetIndexByID(const Name& nid) const {
    auto iter = member_index_map_.find(nid);
    if (iter == member_index_map_.end())
      return {};
    else
      return {iter->second, true};
  }

  std::pair<Name, bool> GetIDByIndex(std::size_t idx) const {
    if (idx >= member_list_.size())
      return {};
    else
      return {member_list_[idx].nid, true};
  }

  std::size_t Size() const { return member_list_.size(); }

  void Encode(std::string& out) const {
    proto::ViewInfo vinfo_proto;
    for (const auto& m : member_list_) {
      auto* entry = vinfo_proto.add_entry();
      entry->set_nid(m.nid.toUri());
    }
    vinfo_proto.AppendToString(&out);
  }

  bool Decode(const void* buf, size_t buf_size) {
    proto::ViewInfo vinfo_proto;
    if (!vinfo_proto.ParseFromArray(buf, buf_size)) return false;

    std::vector<MemberInfo> minfo;
    std::unordered_map<Name, std::size_t> index_map;
    MemberInfoCompare mi_comp;

    // Decode memberinfo list
    for (int idx = 0; idx < vinfo_proto.entry_size(); ++idx) {
      const auto& entry = vinfo_proto.entry(idx);
      if (entry.nid().empty()) return false;
      Name n(entry.nid());
      minfo.push_back({n});
    }

    // Sort by name
    std::sort(minfo.begin(), minfo.end(), mi_comp);

    // Build index
    for (size_t i = 0; i < minfo.size(); ++i) {
      index_map[minfo[i].nid] = i;
    }

    member_list_ = std::move(minfo);
    member_index_map_ = std::move(index_map);
    return true;
  }

  bool Merge(const ViewInfo& vinfo) {
    auto old_size = Size();
    for (const auto& m : vinfo.member_list_) {
      if (member_index_map_.find(m.nid) != member_index_map_.end())
        // Node name already exists
        continue;
      member_list_.push_back(m);
    }

    if (old_size == Size()) return false;

    // Sort by name
    MemberInfoCompare mi_comp;
    std::sort(member_list_.begin(), member_list_.end(), mi_comp);

    // Build index
    member_index_map_.clear();
    for (std::size_t i = 0; i < member_list_.size(); ++i) {
      member_index_map_[member_list_[i].nid] = i;
    }

    return true;
  }

  void Remove(const std::unordered_set<Name>& nids) {
    auto iter = std::remove_if(member_list_.begin(), member_list_.end(),
                               [&nids](const MemberInfo& m) {
                                 return nids.find(m.nid) != nids.end();
                               });
    if (iter == member_list_.end()) return;
    member_list_.erase(iter, member_list_.end());

    // Rebuild index map
    member_index_map_.clear();
    for (std::size_t i = 0; i != member_list_.size(); ++i) {
      member_index_map_[member_list_[i].nid] = i;
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
      os << "," << i << "={" << m.nid << "}";
    }
    return os << ")";
  }

 private:
  std::vector<MemberInfo> member_list_;
  std::unordered_map<Name, std::size_t> member_index_map_;
};

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_VIEW_INFO_HPP_
