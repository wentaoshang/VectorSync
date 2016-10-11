/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_VIEW_INFO_HPP_
#define NDN_VSYNC_VIEW_INFO_HPP_

#include <algorithm>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <ndn-cxx/name.hpp>

#include "vsync-common.hpp"

namespace ndn {
namespace vsync {

struct MemberInfo {
  NodeID id;
  Name prefix;
};

class ViewInfo {
public:
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
    // Convert node ID to a string that conforms to NDN URI scheme
    for (auto& m : member_list_) {
      m.id = name::Component(m.id).toUri();
    }

    std::sort(member_list_.begin(), member_list_.end(),
	      [](const MemberInfo& l, const MemberInfo& r) { return l.id < r.id; });
    for (std::size_t i = 0; i < member_list_.size(); ++i) {
      const auto& id = member_list_[i].id;
      auto r = member_index_map_.insert({id, i});
      if (!r.second)
	throw "Duplicate node ID in member list";
    }
  }

  std::pair<NodeIndex, bool> GetIndexByID(const NodeID& nid) const {
    auto iter = member_index_map_.find(nid);
    if (iter == member_index_map_.end()) return {};
    else return {iter->second, true};
  }

  std::pair<NodeID, bool> GetIDByIndex(NodeIndex idx) const {
    if (idx >= member_list_.size()) return {};
    else return {member_list_[idx].id, true};
  }

  std::pair<Name, bool> GetPrefixByIndex(NodeIndex idx) const {
    if (idx >= member_list_.size()) return {};
    else return {member_list_[idx].prefix, true};
  }

  std::size_t Size() const {
    return member_list_.size();
  }

private:
  std::vector<MemberInfo> member_list_;
  std::unordered_map<NodeID, NodeIndex> member_index_map_;
};

} // namespace vsync
} // namespace ndn

#endif // NDN_VSYNC_VIEW_INFO_HPP_
