/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <random>

#include <ndn-cxx/util/digest.hpp>

#include "logging.hpp"
#include "node.hpp"
#include "vsync-helper.hpp"

VSYNC_LOG_DEFINE(ndn.vsync.Node);

namespace ndn {
namespace vsync {

Node::Node(Face& face, Scheduler& scheduler, KeyChain& key_chain,
           const NodeID& nid, const Name& prefix, uint32_t seed)
    : face_(face),
      scheduler_(scheduler),
      key_chain_(key_chain),
      id_(name::Component(nid).toUri()),
      prefix_(prefix),
      view_id_({1, nid}),
      view_info_({{nid, prefix}}),
      rengine_(seed),
      heartbeat_random_delay_(10,
                              time::milliseconds(kHeartbeatMaxDelay).count()),
      leader_election_random_delay_(
          100, time::milliseconds(kLeaderElectionTimeoutMax).count()),
      heartbeat_event_(scheduler_),
      healthcheck_event_(scheduler_),
      leader_election_event_(scheduler_) {
  ResetState();

  if (is_leader_) PublishViewInfo();

  face_.setInterestFilter(
      kSyncPrefix, std::bind(&Node::OnSyncInterest, this, _2),
      [this](const Name&, const std::string& reason) {
        throw Error("Failed to register vsync prefix: " + reason);
      });

  face_.setInterestFilter(
      Name(prefix).append(id_), std::bind(&Node::OnDataInterest, this, _2),
      [this](const Name&, const std::string& reason) {
        throw Error("Failed to register data prefix: " + reason);
      });

  heartbeat_event_ = scheduler_.scheduleEvent(
      kHeartbeatInterval +
          time::milliseconds(heartbeat_random_delay_(rengine_)),
      [this] { PublishHeartbeat(); });

  healthcheck_event_ = scheduler_.scheduleEvent(kHealthcheckInterval,
                                                [this] { DoHealthcheck(); });
}

void Node::ResetState() {
  idx_ = view_info_.GetIndexByID(id_).first;
  is_leader_ = view_id_.second == id_;
  view_change_signal_(view_id_, view_info_, is_leader_);

  vector_clock_.clear();
  vector_clock_.resize(view_info_.Size());

  last_heartbeat_.clear();
  auto now = time::steady_clock::now();
  last_heartbeat_.resize(view_info_.Size(), now);

  auto& graph = causality_graph_[view_id_];

  for (std::size_t i = 0; i < view_info_.Size(); ++i) {
    auto nid = view_info_.GetIDByIndex(i).first;
    auto rw = recv_window_[nid];
    vector_clock_[i] = rw.UpperBound();
    graph.insert({nid, {}});
  }

  vector_clock_change_signal_(idx_, vector_clock_);
}

bool Node::LoadView(const ViewID& vid, const ViewInfo& vinfo) {
  auto p = vinfo.GetIndexByID(id_);
  if (!p.second) {
    VSYNC_LOG_WARN("View info does not contain self node ID " << id_);
    return false;
  }

  VSYNC_LOG_INFO("Load view: vid=" << vid << ", vinfo=" << vinfo);

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
    // Suppress this interest if this view info has been received before
    if (data_store_.find(n) != data_store_.end()) return;

    Interest i(n, kViewInfoInterestLifetime);
    VSYNC_LOG_TRACE("Send: i.name=" << n);
    face_.expressInterest(i, std::bind(&Node::ProcessViewInfo, this, _1, _2),
                          [](const Interest&, const lp::Nack&) {},
                          std::bind(&Node::OnInterestTimeout, this, _1, 0));
  }
}

void Node::ProcessViewInfo(const Interest& vinterest, const Data& vinfo) {
  const auto& n = vinfo.getName();
  VSYNC_LOG_TRACE("Recv: d.name=" << n);
  if (n.size() != vinterest.getName().size()) {
    VSYNC_LOG_WARN("Invalid view info name " << n);
    return;
  }

  const auto& content = vinfo.getContent();
  ViewInfo view_info;
  if (!view_info.Decode(content.value(), content.value_size())) {
    VSYNC_LOG_WARN("Cannot decode view info");
    return;
  }

  VSYNC_LOG_TRACE("Recv: " << view_info);

  // Store a local copy of view info data
  data_store_[n] = vinfo.shared_from_this();
  // TODO: verify view info using common trust anchor

  ViewID vid = ExtractViewID(n);
  if (vid.first > view_id_.first) {
    if (!LoadView(vid, view_info)) {
      VSYNC_LOG_WARN("Cannot load received view: vinfo=" << view_info);
      return;
    }

    // Cancel any leader election event
    leader_election_event_.cancel();
  } else if (is_leader_ &&
             ((vid.first < view_id_.first && vid.second != id_) ||
              (vid.first == view_id_.first && vid.second < id_))) {
    if (!view_info_.Merge(view_info) && vid.first < view_id_.first)
      // No need to do view change since there is no change to group membership
      // and current view number is higher than the received one. The node with
      // lower view number will move to higher view anyway.
      return;

    ++view_id_.first;
    VSYNC_LOG_INFO("Move to new view: vid=" << view_id_
                                            << ", vinfo=" << view_info_);
    ResetState();
    PublishViewInfo();
    PublishHeartbeat();
  }
}

void Node::PublishViewInfo() {
  auto n = MakeViewInfoName(view_id_);
  VSYNC_LOG_TRACE("Publish: d.name=" << n << ", vinfo=" << view_info_);
  std::string content;
  view_info_.Encode(content);
  std::shared_ptr<Data> d = std::make_shared<Data>(n);
  d->setFreshnessPeriod(time::seconds(3600));
  d->setContent(reinterpret_cast<const uint8_t*>(content.data()),
                content.size());
  d->setContentType(kViewInfo);
  key_chain_.sign(*d, signingWithSha256());
  data_store_[n] = d;
}

std::tuple<std::shared_ptr<const Data>, ViewID, VersionVector>
Node::PublishData(const std::string& content, uint32_t type) {
  uint64_t seq = ++vector_clock_[idx_];
  vector_clock_change_signal_(idx_, vector_clock_);

  recv_window_[id_].Insert(seq);

  auto n = MakeDataName(prefix_, id_, seq);
  std::shared_ptr<Data> data = std::make_shared<Data>(n);
  data->setFreshnessPeriod(time::seconds(3600));

  // Add view id and version vector for user data
  auto vv = GenerateDataVV();
  proto::Content content_proto;
  content_proto.set_view_num(view_id_.first);
  content_proto.set_leader_id(view_id_.second);
  auto* vv_proto = content_proto.mutable_vv();
  EncodeVV(vv, vv_proto);

  content_proto.set_user_data(content);
  const std::string& content_proto_str = content_proto.SerializeAsString();
  data->setContent(reinterpret_cast<const uint8_t*>(content_proto_str.data()),
                   content_proto_str.size());
  data->setContentType(type);
  key_chain_.sign(*data, signingWithSha256());

  data_store_[n] = data;
  auto& queue = causality_graph_[view_id_][id_];
  queue.insert({vv, data});
  // PrintCausalityGraph();

  VSYNC_LOG_TRACE("Publish: d.name=" << n << ", vid=" << view_id_
                                     << ", vv=" << vv);

  SendSyncInterest();

  return {data, view_id_, vv};
}

void Node::SendDataInterest(const Name& prefix, const NodeID& nid,
                            uint64_t seq) {
  auto in = MakeDataName(prefix, nid, seq);
  Interest inst(in, kDataInterestLifetime);
  VSYNC_LOG_TRACE("Send: i.name=" << in);
  face_.expressInterest(inst, std::bind(&Node::OnRemoteData, this, _2),
                        [](const Interest&, const lp::Nack&) {},
                        std::bind(&Node::OnInterestTimeout, this, _1, 0));
}

void Node::OnDataInterest(const Interest& interest) {
  const auto& n = interest.getName();
  VSYNC_LOG_TRACE("Recv: i.name=" << n);
  auto iter = data_store_.find(n);
  if (iter == data_store_.end()) {
    VSYNC_LOG_WARN("Unknown data name: " << n);
    // TODO: send L7 nack based on the sequence number in the Interest name
  } else {
    VSYNC_LOG_TRACE("Send: d.name=" << iter->second->getName());
    face_.put(*iter->second);
  }
}

void Node::SendSyncInterest() {
  util::Sha256 hasher;
  hasher.update(reinterpret_cast<const uint8_t*>(vector_clock_.data()),
                vector_clock_.size() * sizeof(vector_clock_[0]));
  auto digest = hasher.toString();

  auto n = MakeSyncInterestName(view_id_, digest);

  PublishVector(n);

  VSYNC_LOG_TRACE("Send: i.name=" << n);
  Interest i(n, kSyncInterestLifetime);
  face_.expressInterest(i, [](const Interest&, const Data&) {},
                        [](const Interest&, const lp::Nack&) {},
                        std::bind(&Node::OnInterestTimeout, this, _1, 0));
}

void Node::OnInterestTimeout(const Interest& interest, int retry_count) {
  VSYNC_LOG_TRACE("Timeout: i.name=" << interest.getName());
  if (retry_count > kInterestMaxRetrans) return;
  VSYNC_LOG_TRACE("Retrans: i.name=" << interest.getName());
  // TBD: increase interest lifetime exponentially?
  face_.expressInterest(
      interest, [](const Interest&, const Data&) {},
      [](const Interest&, const lp::Nack&) {},
      std::bind(&Node::OnInterestTimeout, this, _1, retry_count + 1));
}

void Node::SendSyncReply(const Name& n) {
  std::shared_ptr<Data> data = std::make_shared<Data>(n);
  data->setFreshnessPeriod(time::milliseconds(50));
  std::string vv_encode;
  EncodeVV(vector_clock_, vv_encode);
  data->setContent(reinterpret_cast<const uint8_t*>(vv_encode.data()),
                   vv_encode.size());
  data->setContentType(kSyncReply);
  key_chain_.sign(*data, signingWithSha256());
  face_.put(*data);
}

void Node::OnSyncInterest(const Interest& interest) {
  const auto& n = interest.getName();
  VSYNC_LOG_TRACE("Recv: i.name=" << n);

  // Check sync interest name size
  if (n.size() != kSyncPrefix.size() + 4) {
    VSYNC_LOG_WARN("Invalid sync interest name: " << n);
    return;
  }

  auto vi = ExtractViewID(n);
  auto digest = ExtractVectorDigest(n);

  auto dispatcher = n.get(-4).toUri();
  if (dispatcher == "vinfo") {
    auto iter = data_store_.find(n);
    if (iter != data_store_.end()) {
      VSYNC_LOG_TRACE("Send: d.name=" << iter->second->getName());
      face_.put(*iter->second);
    }
  } else if (dispatcher == "digest") {
    // Generate sync reply to notify receipt of sync interest
    SendSyncReply(n);

    // Check view id
    if (vi != view_id_) {
      DoViewChange(vi);
      return;
    }

    SendVectorInterest(n);
  } else if (dispatcher == "vector") {
    auto iter = data_store_.find(n);
    if (iter != data_store_.end()) {
      VSYNC_LOG_TRACE("Send: d.name=" << iter->second->getName());
      face_.put(*iter->second);
    }
  } else {
    VSYNC_LOG_WARN("Unknown dispatch tag in interest name: " << dispatcher);
  }
}

void Node::SendVectorInterest(const Name& sync_interest_name) {
  auto n = MakeVectorInterestName(sync_interest_name);
  // Ignore sync interest if the vector data has been fetched before
  if (data_store_.find(n) != data_store_.end()) return;

  VSYNC_LOG_TRACE("Send: i.name=" << n);
  Interest i(n, kVectorInterestLifetime);
  face_.expressInterest(i, std::bind(&Node::ProcessVector, this, _2),
                        [](const Interest&, const lp::Nack&) {},
                        std::bind(&Node::OnInterestTimeout, this, _1, 0));
}

void Node::PublishVector(const Name& sync_interest_name) {
  auto n = MakeVectorInterestName(sync_interest_name);
  VSYNC_LOG_TRACE("Publish: d.name=" << n
                                     << ", vector_clock=" << vector_clock_);

  std::shared_ptr<Data> data = std::make_shared<Data>(n);
  data->setFreshnessPeriod(time::seconds(3600));
  std::string vv_encode;
  EncodeVV(vector_clock_, vv_encode);
  data->setContent(reinterpret_cast<const uint8_t*>(vv_encode.data()),
                   vv_encode.size());
  data->setContentType(kVectorClock);
  key_chain_.sign(*data, signingWithSha256());

  data_store_[n] = data;
  // TODO: implement garbage collection to purge old vector data
}

void Node::ProcessVector(const Data& data) {
  const auto& n = data.getName();
  VSYNC_LOG_TRACE("Recv: d.name=" << n.toUri());

  if (data.getContentType() != kVectorClock) {
    VSYNC_LOG_WARN("Wrong content type for vector data"
                   << data.getContentType());
    return;
  }

  auto vi = ExtractViewID(n);
  if (vi != view_id_) {
    VSYNC_LOG_INFO("Ignore version vector from different view: " << vi);
    return;
  }

  // Parse version vector
  const auto& content = data.getContent();
  auto vv = DecodeVV(content.value(), content.value_size());

  // Detect invalid version vector
  if (vv.size() != vector_clock_.size()) {
    VSYNC_LOG_INFO("Ignore version vector of different size: " << vv.size());
    return;
  }

  VSYNC_LOG_TRACE("Recv: vector_clock=" << vv);

  if (vv[idx_] > vector_clock_[idx_]) {
    VSYNC_LOG_INFO(
        "Ignore vector clock with larger self sequence number: " << vv);
  }

  // Process version vector
  VersionVector old_vv = vector_clock_;
  vector_clock_ = Merge(old_vv, vv);
  vector_clock_change_signal_(idx_, vector_clock_);

  VSYNC_LOG_INFO("Update: view_id=" << view_id_
                                    << ", vector_clock=" << vector_clock_);

  for (std::size_t i = 0; i != vv.size(); ++i) {
    if (i == idx_) continue;

    auto nid = view_info_.GetIDByIndex(i);
    if (!nid.second)
      throw Error("Cannot get node ID for index " + std::to_string(i));

    auto pfx = view_info_.GetPrefixByIndex(i);
    if (!pfx.second)
      throw Error("Cannot get node prefix for index " + std::to_string(i));

    if (old_vv[i] < vv[i]) {
      for (uint64_t seq = old_vv[i] + 1; seq <= vv[i]; ++seq) {
        SendDataInterest(pfx.first, nid.first, seq);
      }
    }
  }

  // TBD: send sync interest if the vector has changed?
}

void Node::OnRemoteData(const Data& data) {
  const auto& n = data.getName();
  if (n.size() < 2) {
    VSYNC_LOG_WARN("Invalid data name: " << n);
    return;
  }

  if (data_store_.find(n) != data_store_.end()) {
    // A node may receive duplicate data if it sends multiple Interests for the
    // same data. NDN-CXX will not merge the duplicate pending Interests.
    VSYNC_LOG_WARN("Duplicate data received: d.name=" << n);
    return;
  }

  const auto& content = data.getContent();
  proto::Content content_proto;
  if (content_proto.ParseFromArray(content.value(), content.value_size())) {
    ViewID vi = {content_proto.view_num(), content_proto.leader_id()};
    VSYNC_LOG_TRACE("Recv: d.name=" << n << ", vid=" << vi);

    auto nid = ExtractNodeID(n);
    auto seq = ExtractSequenceNumber(n);

    UpdateReceiveWindow(data, nid, seq);

    // Store a local copy of received data
    data_store_[n] = data.shared_from_this();

    auto content_type = data.getContentType();
    if (content_type == kHeartbeat) {
      ProcessHeartbeat(vi, nid);
    } else if (content_type == kUserData) {
      auto vv = DecodeVV(content_proto.vv());
      auto& queue = causality_graph_[vi][nid];
      queue.insert({vv, data.shared_from_this()});
      // PrintCausalityGraph();
      data_signal_(data.shared_from_this(), content_proto.user_data(), vi, vv);
    }
  } else {
    VSYNC_LOG_WARN("Invalid content format: d.name=" << n);
  }
}

void Node::UpdateReceiveWindow(const Data& data, const NodeID& nid,
                               uint64_t seq) {
  auto& win = recv_window_[nid];

  // Insert the new seq number into the receive window
  VSYNC_LOG_TRACE("Insert into recv_window[" << nid << "]: seq=" << seq);
  win.Insert(seq);

  // Check for missing data
  const auto& missing_seq_intervals = win.CheckForMissingData(seq);
  if (missing_seq_intervals.empty()) {
    VSYNC_LOG_TRACE("No missing data from node " << nid << " before seq num "
                                                 << seq);
    return;
  }

  auto pfx = ExtractNodePrefix(data.getName());
  for (auto iter = boost::icl::elements_begin(missing_seq_intervals);
       iter != boost::icl::elements_end(missing_seq_intervals); ++iter) {
    SendDataInterest(pfx, nid, *iter);
  }
}

VersionVector Node::GenerateDataVV() const {
  VersionVector vv(view_info_.Size());
  for (std::size_t i = 0; i != view_info_.Size(); ++i) {
    if (i == idx_) {
      vv[i] = vector_clock_[i] - 1;
    } else {
      auto nid = view_info_.GetIDByIndex(i);
      if (!nid.second) {
        throw Error("Cannot get node id for index " + std::to_string(i));
      }
      auto iter = recv_window_.find(nid.first);
      if (iter == recv_window_.end()) {
        throw Error("Cannot get recv window for node id " + nid.first);
      }
      vv[i] = iter->second.LowerBound();
    }
  }
  return vv;
}

void Node::PublishHeartbeat() {
  // Schedule next heartbeat event before publishing heartbeat message
  heartbeat_event_ = scheduler_.scheduleEvent(
      kHeartbeatInterval +
          time::milliseconds(heartbeat_random_delay_(rengine_)),
      [this] { PublishHeartbeat(); });

  PublishData("Heartbeat", kHeartbeat);
}

void Node::ProcessHeartbeat(const ViewID& vid, const NodeID& nid) {
  if (vid != view_id_) {
    VSYNC_LOG_INFO("Ignore heartbeat for non-current view id " << vid);
    return;
  }

  auto index = view_info_.GetIndexByID(nid);
  if (!index.second) {
    VSYNC_LOG_WARN("Unkown node id in received heartbeat: " << nid);
    return;
  }

  VSYNC_LOG_INFO("Recv: HEARTBEAT from node " << nid << " [idx=" << index.first
                                              << ']');
  last_heartbeat_[index.first] = time::steady_clock::now();
}

void Node::DoHealthcheck() {
  // Schedule next healthcheck event before doing any work
  healthcheck_event_ = scheduler_.scheduleEvent(kHealthcheckInterval,
                                                [this] { DoHealthcheck(); });

  auto now = time::steady_clock::now();
  if (is_leader_) {
    std::unordered_set<NodeID> dead_nodes;
    for (std::size_t i = 0; i != view_info_.Size(); ++i) {
      if (i == idx_) continue;

      if (last_heartbeat_[i] + kHeartbeatTimeout < now) {
        auto p = view_info_.GetIDByIndex(i);
        if (!p.second)
          throw Error("Cannot find node ID for index " + std::to_string(i));

        dead_nodes.insert(p.first);
        VSYNC_LOG_INFO("Found dead node: " << p.first);
      }
    }
    if (dead_nodes.size() >= kViewChangeThreshold) {
      view_info_.Remove(dead_nodes);
      ++view_id_.first;
      ResetState();
      PublishViewInfo();
      VSYNC_LOG_INFO("Move to new view: view_id=" << view_id_
                                                  << ", vinfo=" << view_info_);
      PublishHeartbeat();
    }
  } else {
    const auto& leader_id = view_id_.second;
    auto p = view_info_.GetIndexByID(leader_id);
    if (!p.second)
      throw Error("Cannot find node index for leader " + leader_id);

    if (last_heartbeat_[p.first] + kHeartbeatTimeout < now) {
      VSYNC_LOG_INFO("Found dead leader: " << leader_id);
      // Start leader election timer
      leader_election_event_ = scheduler_.scheduleEvent(
          time::milliseconds(leader_election_random_delay_(rengine_)),
          [this] { ProcessLeaderElectionTimeout(); });
    }
  }
}

void Node::ProcessLeaderElectionTimeout() {
  VSYNC_LOG_INFO("Leader election timer goes off");
  // Remove leader from the view
  view_info_.Remove({view_id_.second});
  // Set self as leader for the new view
  view_id_ = {view_id_.first + 1, id_};
  ResetState();
  PublishViewInfo();
  VSYNC_LOG_INFO("Move to new view: view_id=" << view_id_
                                              << ", vinfo=" << view_info_);
  PublishHeartbeat();
}

void Node::PrintCausalityGraph() const {
  VSYNC_LOG_DEBUG("CausalGraph:");
  for (const auto& p : causality_graph_) {
    VSYNC_LOG_DEBUG(" ViewID=(" << p.first.first << ',' << p.first.second
                                << "):");
    for (const auto& pvv_queue : p.second) {
      VSYNC_LOG_DEBUG("  NodeID=" << pvv_queue.first << ':');
      VSYNC_LOG_DEBUG("   Queue=[");
      for (const auto& pvv : pvv_queue.second) {
        VSYNC_LOG_DEBUG("      " << pvv.first << ':' << pvv.second->getName());
      }
      VSYNC_LOG_DEBUG("         ]");
    }
  }
}

}  // namespace vsync
}  // namespace ndn
