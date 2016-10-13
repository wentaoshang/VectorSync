/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */

#include <random>

#include <boost/log/trivial.hpp>

#include "node.hpp"
#include "vsync-helper.hpp"

namespace ndn {
namespace vsync {

Node::Node(Face& face, Scheduler& scheduler, KeyChain& key_chain,
           const NodeID& nid, const Name& prefix, Node::DataCb on_data)
  : face_(face)
  , scheduler_(scheduler)
  , key_chain_(key_chain)
  , id_(name::Component(nid).toUri())
  , prefix_(prefix)
  , view_id_({0, nid})
  , view_info_({{nid, prefix}})
  , last_data_info_({{0, "-"}, 0, 0})
  , data_cb_(std::move(on_data))
  , rengine_(rdevice_())
  , rdist_(100, time::milliseconds(kLeaderElectionTimoutMax).count())
  , heartbeat_event_(scheduler_)
  , healthcheck_event_(scheduler_)
  , leader_election_event_(scheduler_) {
  ResetState();
  if (is_leader_) PublishViewInfo();

  face_.setInterestFilter(kVsyncPrefix,
                          std::bind(&Node::OnSyncInterest, this, _2),
                          [this](const Name&, const std::string& reason) {
                            throw Error("Failed to register vsync prefix: "
                                        + reason);
                          });

  face_.setInterestFilter(Name(prefix).append(id_),
                          std::bind(&Node::OnDataInterest, this, _2),
                          [this](const Name&, const std::string& reason) {
                            throw Error("Failed to register data prefix: "
                                        + reason);
                          });

  heartbeat_event_ = scheduler_.scheduleEvent(kHeartbeatInterval,
                                              [this] { PublishHeartbeat(); });

  healthcheck_event_ = scheduler_.scheduleEvent(kHealthcheckInterval,
                                                [this] { DoHealthcheck(); });
}

bool Node::LoadView(const ViewID& vid, const ViewInfo& vinfo) {
  auto p = vinfo.GetIndexByID(id_);
  if (!p.second) {
    BOOST_LOG_TRIVIAL(debug) << "View info does not contain self node ID " << id_;
    return false;
  }

  idx_ = p.first;
  view_id_ = vid;
  view_info_ = vinfo;
  ResetState();
  if (is_leader_) PublishViewInfo();
  PublishHeartbeat();

  return true;
}

void Node::DoViewChange(const ViewID& vid) {
  const auto& view_num = vid.first;
  const auto& leader_id = vid.second;
  if (view_num > view_id_.first ||
      (is_leader_ && ((view_num < view_id_.first && leader_id != id_) ||
                      (view_num == view_id_.first && leader_id < id_)))) {
    // Fetch view info
    auto n = MakeViewInfoName(vid);
    Interest i(n, time::seconds(4));
    face_.expressInterest(i, std::bind(&Node::ProcessViewInfo, this, _1, _2),
                          [](const Interest&, const lp::Nack&) {},
                          [](const Interest&) {});
  }
}

void Node::ProcessViewInfo(const Interest &vinterest, const Data& vinfo) {
  const auto& n = vinfo.getName();
  BOOST_LOG_TRIVIAL(trace) << "Recv: d.name=" << n.toUri();
  if (n.size() != vinterest.getName().size()) {
    BOOST_LOG_TRIVIAL(debug) << "Invalid view info name " << n.toUri();
    return;
  }

  const auto& content = vinfo.getContent();
  ViewInfo view_info;
  if (!view_info.Decode(content.value(), content.value_size())) {
    BOOST_LOG_TRIVIAL(debug) << "Cannot decode view info";
    return;
  }

  BOOST_LOG_TRIVIAL(trace) << "Recv: " << view_info;

  //TODO: verify view info using common trust anchor

  ViewID vid = ExtractViewIDFromViewInfoName(n);
  if (vid.first > view_id_.first) {
    if (!LoadView(vid, view_info)) {
      BOOST_LOG_TRIVIAL(debug) << "Cannot load received view " << view_info;
      return;
    }

    // Cancel any leader election event
    leader_election_event_.cancel();

    // Store a local copy of view info data
    data_store_[n] = vinfo.shared_from_this();
  } else if (is_leader_ && ((vid.first < view_id_.first && vid.second != id_) ||
                            (vid.first == view_id_.first && vid.second < id_))) {
    if (!view_info_.Merge(view_info) && vid.first < view_id_.first)
      // No need to do view change since there is no change to group membership and
      // current view number is higher than the received one. The node with lower
      // view number will move to higher view anyway.
      return;

    ++view_id_.first;
    ResetState();
    PublishViewInfo();
    PublishHeartbeat();
  }
  BOOST_LOG_TRIVIAL(debug) << "Move to new view: id=(" << view_id_.first << ","
                           << view_id_.second << "), vinfo=" << view_info_;
}

void Node::PublishViewInfo() {
  auto n = MakeViewInfoName(view_id_);
  auto content = view_info_.Encode();
  std::shared_ptr<Data> d = std::make_shared<Data>(n);
  d->setFreshnessPeriod(time::seconds(3600));
  d->setContent(&content[0], content.size());
  d->setContentType(kViewInfo);
  key_chain_.sign(*d, signingWithSha256());
  data_store_[n] = d;
}

void Node::PublishData(const std::vector<uint8_t>& content, uint32_t type) {
  if (version_vector_[idx_] == 255) {
    // round change
    ++round_number_;
    // clear version vector
    std::fill(version_vector_.begin(), version_vector_.end(), 0);
  }

  uint8_t seq = ++version_vector_[idx_];
  auto n = MakeDataName(prefix_, id_, view_id_, round_number_, seq);

  if (seq == 1) {
    // the first data packet contains the view id, round number, and sequence
    // number of the last packet from the previous round
    std::vector<uint8_t> ldi_wire;
    EncodeLastDataInfo(last_data_info_, ldi_wire);
    std::shared_ptr<Data> d = std::make_shared<Data>(n);
    d->setFreshnessPeriod(time::seconds(3600));
    d->setContent(&ldi_wire[0], ldi_wire.size());
    d->setContentType(kLastDataInfo);
    key_chain_.sign(*d, signingWithSha256());
    data_store_[n] = d;

    BOOST_LOG_TRIVIAL(trace) << "Publish: " << n.toUri()
                             << " with last data info "
                             << ToString(last_data_info_);

    seq = ++version_vector_[idx_];
    n = MakeDataName(prefix_, id_, view_id_, round_number_, seq);
  }

  std::shared_ptr<Data> data = std::make_shared<Data>(n);
  data->setFreshnessPeriod(time::seconds(3600));
  data->setContent(&content[0], content.size());
  data->setContentType(type);
  key_chain_.sign(*data, signingWithSha256());
  data_store_[n] = data;
  last_data_info_ = {view_id_, round_number_, seq};

  BOOST_LOG_TRIVIAL(trace) << "Publish: " << n.toUri();

  SendSyncInterest();
}

void Node::OnDataInterest(const Interest& interest) {
  const auto& n = interest.getName();
  auto iter = data_store_.find(n);
  if (iter == data_store_.end()) {
    BOOST_LOG_TRIVIAL(debug) << "Unrecognized data interest: " << n.toUri();
    //TODO: send L7 nack based on the sequence number in the Interest name
  } else {
    face_.put(*iter->second);
  }
}

void Node::SendSyncInterest() {
  auto n = MakeVsyncInterestName(view_id_, round_number_, version_vector_);

  BOOST_LOG_TRIVIAL(trace) << "Send: vi = (" << view_id_.first << ","
                           << view_id_.second << "), rn = " << round_number_
                           << ", vv = " << ToString(version_vector_);

  Interest i(n, time::milliseconds(1000));
  face_.expressInterest(i, [](const Interest&, const Data&) {},
                        [](const Interest&, const lp::Nack&) {},
                        [](const Interest&) {});

  // Generate sync reply to purge pending sync Interest
  std::shared_ptr<Data> data = std::make_shared<Data>(n);
  data->setFreshnessPeriod(time::milliseconds(50));
  data->setContent(&version_vector_[0], version_vector_.size());
  data->setContentType(kVsyncReply);
  key_chain_.sign(*data, signingWithSha256());
  face_.put(*data);
}

void Node::OnSyncInterest(const Interest& interest) {
  const auto& n = interest.getName();
  BOOST_LOG_TRIVIAL(trace) << "Recv: i.name=" << n.toUri();

  // Handle request for ViewInfo
  if (n.size() == kVsyncPrefix.size() + 3 && n.get(-1).toUri() == "vinfo") {
    auto iter = data_store_.find(n);
    if (iter == data_store_.end()) {
      BOOST_LOG_TRIVIAL(debug) << "Unrecognized view info name: " << n.toUri();
    } else {
      face_.put(*iter->second);
    }
    return;
  }

  // Check sync interest name size
  if (n.size() != kVsyncPrefix.size() + 4) {
    BOOST_LOG_TRIVIAL(error) << "Invalid sync interest name: " << n.toUri();
    return;
  }

  auto vi = ExtractViewID(n);
  auto rn = ExtractRoundNumber(n);
  auto vv = ExtractVersionVector(n);

  BOOST_LOG_TRIVIAL(trace) << "Recv: vi = (" << vi.first << "," << vi.second
                           << "), rn = " << rn << ", vv = " << ToString(vv);

  // Check view id
  if (vi != view_id_) {
    DoViewChange(vi);
    return;
  }

  // Detect invalid version vector
  if (vv.size() != version_vector_.size()) {
    BOOST_LOG_TRIVIAL(info) << "Ignore version vector of different size: "
                            << ToString(vv);
    return;
  }

  if (vv[idx_] > version_vector_[idx_]) {
    BOOST_LOG_TRIVIAL(info)
      << "Ignore version vector with larger sequence number for ourselves: "
      << ToString(vv);
  }

  // Check round number
  if (rn < round_number_) {
    BOOST_LOG_TRIVIAL(debug) << "Ignore sync interest with smaller round number "
                             << rn;
    return;
  }

  if (rn > round_number_) {
    // Round change
    round_number_ = rn;
    // Clear version vector
    std::fill(version_vector_.begin(), version_vector_.end(), 0);
  }

  // Process version vector
  VersionVector old_vv = version_vector_;
  version_vector_ = Merge(old_vv, vv);

  BOOST_LOG_TRIVIAL(trace) << "Updt: vi = (" << view_id_.first << ","
                           << view_id_.second << "), rn = " << round_number_
                           << ", vv = " << ToString(version_vector_);

  for (size_t i = 0; i < vv.size(); ++i) {
    if (i == idx_) continue;

    auto nid = view_info_.GetIDByIndex(i);
    if (!nid.second)
      throw Error("Cannot get node ID for index " + std::to_string(i));

    auto pfx = view_info_.GetPrefixByIndex(i);
    if (!pfx.second)
      throw Error("Cannot get node prefix for index " + std::to_string(i));

    for (uint64_t seq = old_vv[i] + 1; seq <= vv[i]; ++seq) {
      auto in = MakeDataName(pfx.first, nid.first, vi, rn, seq);
      Interest inst(in, time::milliseconds(1000));
      BOOST_LOG_TRIVIAL(trace) << "Ftch: i.name=" << in.toUri();
      face_.expressInterest(inst, std::bind(&Node::OnRemoteData, this, _2),
                            [](const Interest&, const lp::Nack&) {},
                            [](const Interest&) {});
    }
  }
}

void Node::OnRemoteData(const Data& data) {
  const auto& n = data.getName();
  BOOST_LOG_TRIVIAL(trace) << "Recv: d.name=" << n.toUri();
  if (n.size() < 5) {
    BOOST_LOG_TRIVIAL(error) << "Invalid data name: " << n.toUri();
    return;
  }

  auto nid = ExtractNodeID(n);

  auto index = view_info_.GetIndexByID(nid);
  if (!index.second) {
    BOOST_LOG_TRIVIAL(debug) << "Unkown node id in received data: "
                             << nid;
    return;
  }

  UpdateReceiveWindow(data, index.first);

  // Store a local copy of received data
  data_store_[n] = data.shared_from_this();;

  auto content_type = data.getContentType();
  if (content_type == kHeartbeat) {
    ProcessHeartbeat(index.first);
  } else if (content_type == kUserData && data_cb_) {
    const auto& content = data.getContent();
    data_cb_(content.value(), content.value_size());
  }
}

void Node::UpdateReceiveWindow(const Data& data, NodeIndex index) {
  const auto& n = data.getName();
  auto vi = ExtractViewID(n);
  auto rn = ExtractRoundNumber(n);
  auto seq = ExtractSequenceNumber(n);

  if (seq == 1 || data.getContentType() == kLastDataInfo) {
    // Extract info about last data in the previous round
    const auto& content = data.getContent();
    auto p = DecodeLastDataInfo(content.value(), content.value_size());
    if (!p.second) {
      BOOST_LOG_TRIVIAL(debug) << "Cannot decode last data info";
    } else {
      BOOST_LOG_TRIVIAL(trace) << "Recv last data info: " << ToString(p.first);
      CheckForMissingData(p.first, index);
    }
  }

  // Add the new seq number into the receive window
  auto& win = member_state_[index].recv_window;
  ESN esn{vi, rn, seq};
  BOOST_LOG_TRIVIAL(trace) << "Insert into recv window of node idx " << index
                           << ": " << ToString(esn);
  win.insert(esn);
  // Try to slide the window forward
  auto iter = win.begin();
  auto inext = std::next(iter);
  while (inext != win.end()) {
    if (!IsNext(*iter, *inext)) break;
    iter = inext;
    inext = std::next(iter);
  }
  BOOST_LOG_TRIVIAL(trace) << "Slide recv window of node idx " << index << " from "
                           << ToString(*win.begin()) << " to " << ToString(*iter);
  win.erase(win.begin(), iter);
}

void Node::CheckForMissingData(const ESN& ldi, NodeIndex index) {
}

void Node::PublishHeartbeat() {
  // Schedule next heartbeat event before publishing heartbeat message
  heartbeat_event_ = scheduler_.scheduleEvent(kHeartbeatInterval,
                                              [this] { PublishHeartbeat(); });

  BOOST_LOG_TRIVIAL(trace) << "Send HEARTBEAT";
  PublishData({'H', 'E', 'A', 'R', 'T', 'B', 'E', 'A', 'T', 0}, kHeartbeat);
}

void Node::ProcessHeartbeat(NodeIndex index) {
  BOOST_LOG_TRIVIAL(trace) << "Recv HEARTBEAT from node idx " << index;
  member_state_[index].last_heartbeat = time::steady_clock::now();;
}

void Node::DoHealthcheck() {
  // Schedule next healthcheck event before doing any work
  healthcheck_event_ = scheduler_.scheduleEvent(kHealthcheckInterval,
                                                [this] { DoHealthcheck(); });

  auto now = time::steady_clock::now();
  if (is_leader_) {
    std::unordered_set<NodeID> dead_nodes;
    for (size_t i = 0; i != member_state_.size(); ++i) {
      if (i == idx_) continue;

      if (member_state_[i].last_heartbeat + kHeartbeatTimeout < now) {
        auto p = view_info_.GetIDByIndex(i);
        if (!p.second)
          throw Error("Cannot find node ID for index " + std::to_string(i));

        dead_nodes.insert(p.first);
        BOOST_LOG_TRIVIAL(trace) << "Found dead node ID: " << p.first;
      }
    }
    if (dead_nodes.size() >= kViewChangeThreshold) {
      view_info_.Remove(dead_nodes);
      ++view_id_.first;
      ResetState();
      PublishViewInfo();
      BOOST_LOG_TRIVIAL(debug) << "Move to new view: id=(" << view_id_.first << ","
                               << view_id_.second << "), vinfo=" << view_info_;
      PublishHeartbeat();
    }
  } else {
    const auto& leader_id = view_id_.second;
    auto p = view_info_.GetIndexByID(leader_id);
    if (!p.second)
      throw Error("Cannot find node index for leader " + leader_id);

    if (member_state_[p.first].last_heartbeat + kHeartbeatTimeout < now) {
      BOOST_LOG_TRIVIAL(debug) << "Leader " << leader_id << " is dead";
      // Start leader election timer
      leader_election_event_ =
        scheduler_.scheduleEvent(time::milliseconds(rdist_(rengine_)),
                                 [this] { ProcessLeaderElectionTimeout(); });
    }
  }
}

void Node::ProcessLeaderElectionTimeout() {
  // Remove leader from the view
  view_info_.Remove({view_id_.second});
  // Set self as leader for the new view
  view_id_ = {view_id_.first + 1, id_};
  ResetState();
  PublishViewInfo();
  BOOST_LOG_TRIVIAL(debug) << "Move to new view: id=(" << view_id_.first << ","
                           << view_id_.second << "), vinfo=" << view_info_;
  PublishHeartbeat();
}

} // namespace vsync
} // namespace ndn
