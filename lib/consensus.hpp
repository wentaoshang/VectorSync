/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#ifndef NDN_VSYNC_CONSENSUS_HPP_
#define NDN_VSYNC_CONSENSUS_HPP_

#include <map>
#include <set>
#include <unordered_map>

#include "node.hpp"

namespace ndn {
namespace vsync {

/**
 * @brief Total-ordered sync node supporting linearizability via consensus
 */
class TONode : public Node {
 public:
  using TODataSignal =
      util::Signal<TONode, uint64_t, std::shared_ptr<const Data>>;
  using TODataCb = TODataSignal::Handler;

  struct Action {
    proto::TOData::Command cmd;
    uint64_t num;
    std::string param;
  };

  TONode(Face& face, Scheduler& scheduler, KeyChain& key_chain,
         const NodeID& nid, const Name& prefix, uint32_t seed)
      : Node(face, scheduler, key_chain, nid, prefix, seed) {
    ConnectDataSignal(std::bind(&TONode::OnNodeData, this, _1));
  }

  void SetConsensusGroup(const std::set<NodeID>& group) {
    if (!group_.empty()) return;
    group_ = group;
    majority_size_ = group_.size() / 2 + 1;
  }

  void ConnectTODataSignal(TODataCb cb) { this->to_data_signal_.connect(cb); }

  bool PublishTOData(const std::string&, TODataCb);

 private:
  struct State {
    std::unordered_map<NodeID, NodeID> votes;
    std::shared_ptr<const Data> outcome;
  };

  void OnNodeData(std::shared_ptr<const Data>);

  inline size_t CountVote(const std::unordered_map<NodeID, NodeID>&);

  void MakeNewProposal();

  void ConsumeTOData();

  std::set<NodeID> group_;
  size_t majority_size_;

  std::map<uint64_t, State> consensus_state_;

  uint64_t last_consumed_number_;
  uint64_t last_proposed_number_;
  bool has_pending_data_;
  std::string uncommitted_content_;
  TODataCb after_commit_cb_;

  TODataSignal to_data_signal_;
};

}  // namespace vsync
}  // namespace ndn

#endif  // NDN_VSYNC_CONSENSUS_HPP_
