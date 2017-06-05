/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include "consensus.hpp"
#include "logging.hpp"
#include "vsync-helper.hpp"

VSYNC_LOG_DEFINE(ndn.vsync.TONode);

namespace ndn {
namespace vsync {

bool TONode::PublishTOData(const std::string& content, TODataCb after_commit) {
  if (has_pending_data_) {
    VSYNC_LOG_TRACE("Has pending data");
    return false;
  }
  has_pending_data_ = true;
  uncommitted_content_ = content;
  after_commit_cb_ = after_commit;
  MakeNewProposal();
  return true;
}

static std::pair<TONode::Action, bool> ParseAction(const Block& content) {
  proto::TOData tod_proto;
  if (!tod_proto.ParseFromArray(content.value(), content.value_size()))
    return {};
  return {{tod_proto.cmd(), tod_proto.num(), tod_proto.param()}, true};
}

void TONode::CountVote(TONode::State& state) {
  std::unordered_map<Name, size_t> counts;
  for (const auto& v : state.votes) {
    ++counts[v.second];
  }
  assert(!counts.empty());
  auto highest = counts.begin();
  for (auto iter = std::next(counts.begin()); iter != counts.end(); ++iter) {
    if (iter->second > highest->second) highest = iter;
  }
  if (highest->second >= majority_size_) {
    // There cannot be two nodes both obtaining majority votes. So the one with
    // higher-than-majority votes must be the winner.
    state.concluded = true;
    state.winner = highest->first;
  } else if (highest->second + (group_.size() - state.votes.size()) <
             majority_size_) {
    // Even the node with highest vote cannot win the majority even if all the
    // unreceived votes are voting for it. That means no one is ever going to
    // win the consensus, i.e., there is split vote. In this case, mark the
    // state as concluded but with empty winner.
    state.concluded = true;
  }
  // Otherwise, keep waiting for more votes to come.
  return;
}

void TONode::OnNodeData(std::shared_ptr<const Data> data) {
  VSYNC_LOG_TRACE("Recv: TOData.Name=" << data->getName());
  auto src = ExtractNodeID(data->getName());
  if (group_.find(src) == group_.end()) {
    VSYNC_LOG_WARN("TOData producer " << src << " is not in consensus group");
    return;
  }

  auto p = ParseAction(data->getContent());
  if (!p.second) {
    VSYNC_LOG_WARN("Cannot parse TOData action: d.name=" << data->getName());
    return;
  }
  const auto& action = p.first;
  auto& state = consensus_state_[action.num];
  if (state.committed_data != nullptr) {
    VSYNC_LOG_TRACE("Ignore action for committed number " << action.num);
    return;
  }

  switch (action.cmd) {
    case proto::TOData::VOTE: {
      VSYNC_LOG_TRACE("Recv vote from node " << src << ": num=" << action.num
                                             << ", param=" << action.param);
      // Record votes in local state ledger
      auto p2 = state.votes.insert({src, action.param});
      if (!p2.second) {
        VSYNC_LOG_TRACE("Vote for number " << action.num << " from node " << src
                                           << " has already been processed");
        return;
      }

      // If we receive a proposal and we have not voted or proposed for the
      // same number, vote for the received proposal.
      if (action.param == src.toUri() &&
          state.votes.find(nid_) == state.votes.end()) {
        VSYNC_LOG_TRACE("Cast vote: num=" << action.num
                                          << ", param=" << action.param);
        proto::TOData tod_proto;
        tod_proto.set_cmd(proto::TOData::VOTE);
        tod_proto.set_num(action.num);
        tod_proto.set_param(action.param);
        PublishData(tod_proto.SerializeAsString());
        // Record our own vote
        state.votes[nid_] = action.param;
      }

      // If the number of received votes is less than the majority size, no
      // one can possibly win the consensus now.
      if (state.votes.size() < majority_size_) return;

      // Try to process the votes and reach conclusion
      CountVote(state);
      if (!state.concluded) return;

      // If the consensus has concluded and we voted for that number, check if
      // we are the winner.
      if (last_proposed_number_ == action.num) {
        if (state.winner == nid_) {
          VSYNC_LOG_INFO("We won majority vote for number " << action.num);
          // Publish uncommitted data under action.num
          proto::TOData tod_proto;
          tod_proto.set_cmd(proto::TOData::COMMIT);
          tod_proto.set_num(action.num);
          tod_proto.set_param(uncommitted_content_);
          auto d = PublishData(tod_proto.SerializeAsString());
          VSYNC_LOG_TRACE("Commit: num=" << action.num
                                         << ", TOData.Name=" << d->getName());
          state.committed_data = d;
        } else {
          // Someone else wins the vote, or no one wins. Give up current
          // proposal and try to get a new number.
          MakeNewProposal();
        }
      }
      break;
    }
    case proto::TOData::COMMIT:
      VSYNC_LOG_TRACE("Recv commit message from node "
                      << src << ": num=" << action.num
                      << ", param=" << action.param);

      // Accept commit message ASAP, even if we have not seen all the votes.
      state.concluded = true;
      state.winner = src;
      state.committed_data = data;
      if (last_proposed_number_ == action.num) {
        // Someone else committed data for the number we proposed
        MakeNewProposal();
      }
      break;
    default:
      VSYNC_LOG_WARN("Unknown action command " << action.cmd);
      return;
  }
  // Try to consume data (including our own data) in total order
  ConsumeTOData();
}

void TONode::MakeNewProposal() {
  // Vote for ourselves in the next available number (based on our local
  // state)
  uint64_t next_number;
  if (consensus_state_.empty())
    next_number = 1;
  else
    next_number = consensus_state_.rbegin()->first + 1;
  proto::TOData tod_proto;
  tod_proto.set_cmd(proto::TOData::VOTE);
  tod_proto.set_num(next_number);
  tod_proto.set_param(nid_.toUri());
  auto d = PublishData(tod_proto.SerializeAsString());
  VSYNC_LOG_TRACE("Vote: num=" << next_number << ", param=" << nid_
                               << ", TOData.Name=" << d->getName());
  last_proposed_number_ = next_number;
  consensus_state_[next_number].votes[nid_] = nid_;
}

void TONode::ConsumeTOData() {
  uint64_t prev_number = last_consumed_number_;
  auto iter = consensus_state_.find(last_consumed_number_ + 1);
  while (iter != consensus_state_.end()) {
    if (iter->first != prev_number + 1) break;  // number must be continuous
    if (!iter->second.concluded) break;         // number must be concluded

    if (iter->second.committed_data == nullptr) {
      // Still waiting for committed data to come
      break;
    }

    if (!iter->second.winner.empty()) {
      if (iter->first == last_proposed_number_) {
        assert(iter->second.winner == nid_);
        if (after_commit_cb_)
          after_commit_cb_(iter->first, iter->second.committed_data);
        last_proposed_number_ = 0;
        has_pending_data_ = false;
        uncommitted_content_.clear();
        after_commit_cb_ = nullptr;
      } else {
        to_data_signal_(iter->first, iter->second.committed_data);
      }
    }

    // If the consensus concluded without a winner, there must be a split vote.
    // Simply skip the number in this case.

    prev_number = iter->first;
    ++iter;
  }
  last_consumed_number_ = prev_number;
}

}  // namespace vsync
}  // namespace ndn
