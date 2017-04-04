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

  ResetState();

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
  VSYNC_LOG_INFO("Move to new view: vid=" << view_id_
                                          << ", vinfo=" << view_info_);

  if (is_leader_) PublishViewInfo();

  vector_clock_.clear();
  vector_clock_.resize(view_info_.Size());

  last_heartbeat_.clear();
  auto now = time::steady_clock::now();
  last_heartbeat_.resize(view_info_.Size(), now);

  // Preserve sequence numbers for the nodes that survived the view change
  for (std::size_t i = 0; i < view_info_.Size(); ++i) {
    auto nid = view_info_.GetIDByIndex(i).first;
    auto rw = recv_window_[nid];
    vector_clock_[i] = rw.UpperBound();
  }
  vector_clock_change_signal_(idx_, vector_clock_);

  // Update snapshot to the latest sequence numbers in receive windows
  for (const auto& prw : recv_window_) {
    auto& s = snapshot_[prw.first];
    uint64_t seq = prw.second.UpperBound();
    if (s < seq) s = seq;
  }
  PublishNodeSnapshot();

  node_snapshot_bitmap_.clear();
  node_snapshot_bitmap_.resize(view_info_.Size(), false);
  node_snapshot_bitmap_[idx_] = true;
  if (view_info_.Size() == 1) {
    // We are the only node in this view
    VSYNC_LOG_INFO("Snapshot up to view " << view_id_ << " is complete: {");
    for (const auto& p : snapshot_) {
      VSYNC_LOG_INFO("  " << p.first << ':' << p.second << ',');
    }
    VSYNC_LOG_INFO('}');
    // TODO: publish group snapshot
  }
}

bool Node::LoadView(const ViewID& vid, const ViewInfo& vinfo) {
  auto p = vinfo.GetIndexByID(id_);
  if (!p.second) {
    VSYNC_LOG_WARN("View info does not contain self node ID " << id_);
    return false;
  }

  idx_ = p.first;
  view_id_ = vid;
  view_info_ = vinfo;
  ResetState();

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
    face_.expressInterest(
        i, std::bind(&Node::ProcessViewInfo, this, _1, _2),
        [](const Interest&, const lp::Nack&) {},
        std::bind(&Node::OnViewInfoInterestTimeout, this, _1, 0));
  }
}

void Node::OnViewInfoInterestTimeout(const Interest& interest,
                                     int retry_count) {
  VSYNC_LOG_TRACE("Timeout: i.name=" << interest.getName());
  if (retry_count > kInterestMaxRetrans) return;
  VSYNC_LOG_TRACE("Retrans: i.name=" << interest.getName());
  // TBD: increase interest lifetime exponentially?
  face_.expressInterest(
      interest, std::bind(&Node::ProcessViewInfo, this, _1, _2),
      [](const Interest&, const lp::Nack&) {},
      std::bind(&Node::OnViewInfoInterestTimeout, this, _1, retry_count + 1));
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
    ResetState();
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

std::shared_ptr<const Data> Node::PublishData(const std::string& content,
                                              uint32_t type) {
  uint64_t seq = ++vector_clock_[idx_];
  vector_clock_change_signal_(idx_, vector_clock_);

  recv_window_[id_].Insert(seq);

  auto n = MakeDataName(prefix_, id_, seq);
  std::shared_ptr<Data> data = std::make_shared<Data>(n);
  data->setFreshnessPeriod(time::seconds(3600));
  data->setContent(reinterpret_cast<const uint8_t*>(content.data()),
                   content.size());
  data->setContentType(type);
  key_chain_.sign(*data, signingWithSha256());

  data_store_[n] = data;

  VSYNC_LOG_TRACE("Publish: d.name=" << n << ", content_type=" << type
                                     << ", view_id=" << view_id_
                                     << ", vector_clock=" << vector_clock_);

  SendSyncInterest();

  return data;
}

void Node::SendDataInterest(const Name& prefix, const NodeID& nid,
                            uint64_t seq) {
  auto in = MakeDataName(prefix, nid, seq);
  Interest inst(in, kDataInterestLifetime);
  VSYNC_LOG_TRACE("Send: i.name=" << in);
  face_.expressInterest(inst, std::bind(&Node::OnRemoteData, this, _2),
                        [](const Interest&, const lp::Nack&) {},
                        std::bind(&Node::OnDataInterestTimeout, this, _1, 0));
}

void Node::OnDataInterestTimeout(const Interest& interest, int retry_count) {
  VSYNC_LOG_TRACE("Timeout: i.name=" << interest.getName());
  if (retry_count > kInterestMaxRetrans) return;
  VSYNC_LOG_TRACE("Retrans: i.name=" << interest.getName());
  // TBD: increase interest lifetime exponentially?
  face_.expressInterest(
      interest, std::bind(&Node::OnRemoteData, this, _2),
      [](const Interest&, const lp::Nack&) {},
      std::bind(&Node::OnDataInterestTimeout, this, _1, retry_count + 1));
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
  face_.expressInterest(
      i,
      [](const Interest& inst, const Data& ack) {
        VSYNC_LOG_TRACE("Recv: sync interest ack name=" << ack.getName());
      },
      [](const Interest&, const lp::Nack&) {},
      std::bind(&Node::OnSyncInterestTimeout, this, _1, 0));
}

void Node::OnSyncInterestTimeout(const Interest& interest, int retry_count) {
  VSYNC_LOG_TRACE("Timeout: i.name=" << interest.getName());
  if (retry_count > kInterestMaxRetrans) return;
  VSYNC_LOG_TRACE("Retrans: i.name=" << interest.getName());
  // TBD: increase interest lifetime exponentially?
  face_.expressInterest(
      interest,
      [](const Interest& inst, const Data& ack) {
        VSYNC_LOG_TRACE("Recv: sync interest ack name=" << ack.getName());
      },
      [](const Interest&, const lp::Nack&) {},
      std::bind(&Node::OnSyncInterestTimeout, this, _1, retry_count + 1));
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
                        std::bind(&Node::OnVectorInterestTimeout, this, _1, 0));
}

void Node::OnVectorInterestTimeout(const Interest& interest, int retry_count) {
  VSYNC_LOG_TRACE("Timeout: i.name=" << interest.getName());
  if (retry_count > kInterestMaxRetrans) return;
  VSYNC_LOG_TRACE("Retrans: i.name=" << interest.getName());
  // TBD: increase interest lifetime exponentially?
  face_.expressInterest(
      interest, std::bind(&Node::ProcessVector, this, _2),
      [](const Interest&, const lp::Nack&) {},
      std::bind(&Node::OnVectorInterestTimeout, this, _1, retry_count + 1));
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

  auto pfx = ExtractNodePrefix(n);
  auto nid = ExtractNodeID(n);
  auto seq = ExtractSequenceNumber(n);

  UpdateReceiveWindow(pfx, nid, seq);

  // Store a local copy of received data
  data_store_[n] = data.shared_from_this();

  auto content_type = data.getContentType();
  switch (content_type) {
    case kHeartbeat:
      ProcessHeartbeat(data.getContent(), nid);
      break;
    case kUserData:
      data_signal_(data.shared_from_this());
      break;
    case kNodeSnapshot:
      ProcessNodeSnapshot(data.getContent(), nid);
      break;
    default:
      VSYNC_LOG_WARN("Unknown content type in remote data: " << content_type);
  }
}

void Node::UpdateReceiveWindow(const Name& pfx, const NodeID& nid,
                               uint64_t seq) {
  auto& win = recv_window_[nid];

  // Insert the new seq number into the receive window
  VSYNC_LOG_TRACE("Insert into recv_window[" << nid << "]: seq=" << seq);
  win.Insert(seq);

  // Check for missing data before the new seq number
  const auto& missing_seq_intervals = win.CheckForMissingData(seq);
  if (missing_seq_intervals.empty()) {
    VSYNC_LOG_TRACE("No missing data from node " << nid << " before seq num "
                                                 << seq);
    return;
  }

  for (auto iter = boost::icl::elements_begin(missing_seq_intervals);
       iter != boost::icl::elements_end(missing_seq_intervals); ++iter) {
    SendDataInterest(pfx, nid, *iter);
  }
}

void Node::PublishHeartbeat() {
  // Schedule next heartbeat event before publishing heartbeat message
  heartbeat_event_ = scheduler_.scheduleEvent(
      kHeartbeatInterval +
          time::milliseconds(heartbeat_random_delay_(rengine_)),
      [this] { PublishHeartbeat(); });

  proto::Heartbeat hb_proto;
  hb_proto.set_view_num(view_id_.first);
  hb_proto.set_leader_id(view_id_.second);
  PublishData(hb_proto.SerializeAsString(), kHeartbeat);
}

void Node::ProcessHeartbeat(const Block& content, const NodeID& nid) {
  proto::Heartbeat hb_proto;
  if (!hb_proto.ParseFromArray(content.value(), content.value_size())) {
    VSYNC_LOG_WARN("Invalid heartbeat content format: nid=" << nid);
    return;
  }

  if (hb_proto.view_num() != view_id_.first ||
      hb_proto.leader_id() != view_id_.second) {
    VSYNC_LOG_INFO("Ignore heartbeat for non-current view id {"
                   << hb_proto.view_num() << "," << hb_proto.leader_id()
                   << "} from nid=" << nid);
    return;
  }

  auto index = view_info_.GetIndexByID(nid);
  if (!index.second) {
    VSYNC_LOG_WARN("Unkown node id in received heartbeat: nid=" << nid);
    return;
  }

  VSYNC_LOG_INFO("Recv: HEARTBEAT, nid="
                 << nid << " [index=" << index.first << "], view_id={"
                 << hb_proto.view_num() << "," << hb_proto.leader_id() << '}');
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
}

void Node::PublishNodeSnapshot() {
  proto::Snapshot ss_proto;
  ss_proto.set_view_num(view_id_.first);
  ss_proto.set_leader_id(view_id_.second);
  for (const auto& p : snapshot_) {
    auto* entry = ss_proto.add_entry();
    entry->set_nid(p.first);
    entry->set_seq(p.second);
  }
  PublishData(ss_proto.SerializeAsString(), kNodeSnapshot);
}

void Node::ProcessNodeSnapshot(const Block& content, const NodeID& nid) {
  proto::Snapshot ss_proto;
  if (!ss_proto.ParseFromArray(content.value(), content.value_size())) {
    VSYNC_LOG_WARN("Invalid node snapshot content format: nid=" << nid);
    return;
  }

  if (ss_proto.view_num() != view_id_.first ||
      ss_proto.leader_id() != view_id_.second) {
    VSYNC_LOG_INFO("Ignore node snapshot for non-current view id {"
                   << ss_proto.view_num() << "," << ss_proto.leader_id()
                   << "} from nid=" << nid);
    return;
  }

  auto index = view_info_.GetIndexByID(nid);
  if (!index.second) {
    VSYNC_LOG_WARN("Unkown node id in received node snapshot: nid=" << nid);
    return;
  }

  VSYNC_LOG_INFO("Recv: NodeSnapshot, nid="
                 << nid << " [index=" << index.first << "], view_id={"
                 << ss_proto.view_num() << "," << ss_proto.leader_id() << '}');

  for (int i = 0; i < ss_proto.entry_size(); ++i) {
    const auto& entry = ss_proto.entry(i);
    if (entry.nid().empty()) {
      VSYNC_LOG_WARN("Empty entry nid in received node snapshot: nid=" << nid);
      return;
    }
    auto& s = snapshot_[entry.nid()];
    if (s < entry.seq()) s = entry.seq();
    // TODO: check receive window and fetch missing data
    // TBD: how to figure out the node prefix of the missing data?
  }

  node_snapshot_bitmap_[index.first] = true;
  for (bool b : node_snapshot_bitmap_) {
    if (!b) return;
  }
  VSYNC_LOG_INFO("Snapshot up to view " << view_id_ << " is complete: {");
  for (const auto& p : snapshot_) {
    VSYNC_LOG_INFO("  " << p.first << ':' << p.second << ',');
  }
  VSYNC_LOG_INFO('}');
  // TODO: publish group snapshot
}

}  // namespace vsync
}  // namespace ndn
