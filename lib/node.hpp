/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_NODE_HPP_
#define NDN_VSYNC_NODE_HPP_

#include <functional>
#include <memory>
#include <set>
#include <unordered_map>

#include "ndn-common.hpp"
#include "view-info.hpp"
#include "vsync-common.hpp"

namespace ndn {
namespace vsync {

class Node {
public:
  using DataCb = std::function<void(std::shared_ptr<const Data>)>;

  /**
   * @brief Creates a VSync node in a default view with itself as the only member.
   *        The node itself is set as the leader of the view, which has view ID
   *        (1, @p nid).
   */
  Node(Face& face, KeyChain& key_chain, const NodeID& nid, const Name& prefix,
       DataCb on_data);

  void LoadView(const ViewID& vid, const ViewInfo& vinfo);

  void SetInitialRoundNumber(uint64_t rn) { round_number_ = rn; }

  void SetInitialSequenceNumber(uint8_t seq) {
    version_vector_[idx_] = seq;
  }

  void PublishData(const std::vector<uint8_t>& content);

private:
  Node(const Node&) = delete;
  Node& operator=(const Node&) = delete;

  void OnDataInterest(const Interest& interest);
  void SendSyncInterest();
  void OnSyncInterest(const Interest& interest);
  void OnRemoteData(const Data& data);

  void UpdateReceiveWindow(const Data& data);

  Face& face_;
  KeyChain& key_chain_;
  const NodeID id_;
  Name prefix_;
  NodeIndex idx_ = 0;
  ViewID view_id_;
  ViewInfo view_info_;
  uint64_t round_number_ = 0;
  VersionVector version_vector_;

  ESN last_data_info_;
  std::vector<std::set<ESN, ESNCompare>> recv_window_;
  std::unordered_map<Name, std::shared_ptr<const Data>> data_store_;
  DataCb data_cb_;
};

} // namespace vsync
} // namespace ndn

#endif // NDN_VSYNC_NODE_HPP_
