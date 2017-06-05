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

  TONode(Face& face, Scheduler& scheduler, KeyChain& key_chain, const Name& nid,
         uint32_t seed)
      : Node(face, scheduler, key_chain, nid, seed) {
    ConnectDataSignal(std::bind(&TONode::OnNodeData, this, _1));
  }

  /**
   * @brief  This is a hack. Consensus group should be based on a pre-configured
   *         view info and view changes should be disabled.
   */
  void SetConsensusGroup(const std::set<Name>& group) {
    if (!group_.empty()) return;
    group_ = group;
    majority_size_ = group_.size() / 2 + 1;
  }

  void ConnectTODataSignal(TODataCb cb) { this->to_data_signal_.connect(cb); }

  bool PublishTOData(const std::string&, TODataCb);

 private:
  struct State {
    std::unordered_map<Name, Name> votes;
    bool concluded;
    Name winner;
    std::shared_ptr<const Data> committed_data;
  };

  void OnNodeData(std::shared_ptr<const Data>);

  void CountVote(State&);

  void MakeNewProposal();

  void ConsumeTOData();

  std::set<Name> group_;
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
