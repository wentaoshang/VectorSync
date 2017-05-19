/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <random>
#include <stdexcept>

#include <ndn-cxx/util/digest.hpp>

#include "logging.hpp"
#include "node.hpp"
#include "vsync-helper.hpp"

VSYNC_LOG_DEFINE(ndn.vsync.Node);

namespace ndn {
namespace vsync {

// VectorSync parameters

static const int kInterestMaxRetrans = 5;

static time::milliseconds kSyncInterestLifetime = time::milliseconds(1000);
static time::milliseconds kViewInfoInterestLifetime = time::milliseconds(1000);
static time::milliseconds kDataInterestLifetime = time::milliseconds(1000);

void SetInterestLifetime(const time::milliseconds sync_interest_lifetime,
                         const time::milliseconds data_interest_lifetime) {
  kSyncInterestLifetime = sync_interest_lifetime;
  kViewInfoInterestLifetime = kDataInterestLifetime = data_interest_lifetime;
}

static constexpr time::milliseconds kSyncReplyFreshnessPeriod =
    time::milliseconds(5);

static constexpr time::milliseconds kDataInterestMaxDelay =
    time::milliseconds(20);

static time::seconds kHeartbeatInterval = time::seconds(60);
static constexpr time::milliseconds kHeartbeatMaxDelay =
    time::milliseconds(100);
static time::seconds kHeartbeatTimeout = 3 * kHeartbeatInterval;
static time::seconds kHealthcheckInterval = kHeartbeatInterval;
// Leader election timeout MUST be smaller than healthcheck interval
static time::seconds kLeaderElectionTimeoutMax = time::seconds(3);

void SetHeartbeatInterval(const time::seconds heartbeat_interval,
                          const time::seconds leader_election_timeout) {
  if (heartbeat_interval <= leader_election_timeout)
    throw std::invalid_argument("Invalid heartbeat interval");
  kHeartbeatInterval = kHealthcheckInterval = heartbeat_interval;
  kHeartbeatTimeout = 3 * kHeartbeatInterval;
  kLeaderElectionTimeoutMax = leader_election_timeout;
}

// Leader will only perform view change when the number of dead members exceeds
// kViewChangeThreadhold.
static const size_t kViewChangeThreshold = 1;

// end of VectorSync parameters

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
      data_interest_random_delay_(
          0, time::milliseconds(kDataInterestMaxDelay).count()),
      heartbeat_random_delay_(10,
                              time::milliseconds(kHeartbeatMaxDelay).count()),
      leader_election_random_delay_(
          100, time::milliseconds(kLeaderElectionTimeoutMax).count()),
      heartbeat_event_(scheduler_),
      healthcheck_event_(scheduler_),
      leader_election_event_(scheduler_) {}

void Node::Start() {
  face_.setInterestFilter(
      kSyncPrefix, std::bind(&Node::OnSyncInterest, this, _2),
      [this](const Name&, const std::string& reason) {
        throw Error("Failed to register vsync prefix: " + reason);
      });

  face_.setInterestFilter(
      Name(prefix_).append(id_), std::bind(&Node::OnDataInterest, this, _2),
      [this](const Name&, const std::string& reason) {
        throw Error("Failed to register data prefix: " + reason);
      });

  recv_window_[id_].first = prefix_.toUri();
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
    auto pfx = view_info_.GetPrefixByIndex(i).first;
    auto& rw = recv_window_[nid];
    if (rw.first.empty()) rw.first = pfx.toUri();
    vector_clock_[i] = rw.second.UpperBound();
  }
  vector_clock_change_signal_(idx_, vector_clock_);

  // Update snapshot to the latest sequence numbers in receive windows
  for (const auto& prw : recv_window_) {
    const auto& nid = prw.first;
    const auto& rw = prw.second.second;
    auto& s = snapshot_[nid];
    uint64_t seq = rw.UpperBound();
    if (s < seq) s = seq;
  }
  PublishNodeSnapshot();

  node_snapshot_bitmap_.clear();
  node_snapshot_bitmap_.resize(view_info_.Size(), false);
  node_snapshot_bitmap_[idx_] = true;
  if (view_info_.Size() == 1) {
    // We are the only node in this view
    PublishGroupSnapshot();
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

void Node::DoViewChange(const ViewID& vid,
                        std::shared_ptr<const Interest> pending_sync_interest) {
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
        i,
        std::bind(&Node::ProcessViewInfo, this, _1, _2, pending_sync_interest),
        [](const Interest&, const lp::Nack&) {},
        std::bind(&Node::OnViewInfoInterestTimeout, this, _1, 0,
                  pending_sync_interest));
  }
}

void Node::OnViewInfoInterestTimeout(
    const Interest& interest, int retry_count,
    std::shared_ptr<const Interest> pending_sync_interest) {
  VSYNC_LOG_TRACE("Timeout: i.name=" << interest.getName()
                                     << ", retry_count=" << retry_count);
  if (retry_count > kInterestMaxRetrans) return;
  VSYNC_LOG_TRACE("Retrans: i.name=" << interest.getName());
  Interest i(interest.getName(), kViewInfoInterestLifetime);
  // TBD: increase interest lifetime exponentially?
  face_.expressInterest(
      i, std::bind(&Node::ProcessViewInfo, this, _1, _2, pending_sync_interest),
      [](const Interest&, const lp::Nack&) {},
      std::bind(&Node::OnViewInfoInterestTimeout, this, _1, retry_count + 1,
                pending_sync_interest));
}

void Node::ProcessViewInfo(
    const Interest& vinterest, const Data& vinfo,
    std::shared_ptr<const Interest> pending_sync_interest) {
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
    // Move to higher view using received view info
    if (!LoadView(vid, view_info)) {
      VSYNC_LOG_WARN("Cannot load received view: vinfo=" << view_info);
      return;
    }

    // Cancel any leader election event
    leader_election_event_.cancel();

    // Process pending sync interest
    SendDataInterest(pending_sync_interest->getName());
  } else if (is_leader_ &&
             ((vid.first < view_id_.first && vid.second != id_) ||
              (vid.first == view_id_.first && vid.second < id_))) {
    if (!view_info_.Merge(view_info) && vid.first < view_id_.first) {
      // No need to do view change since there is no change to group membership
      // and current view number is higher than the received one. The node with
      // lower view number will move to higher view anyway.
      VSYNC_LOG_INFO(
          "Received view info has no new member and we are in a higher view: "
          << "received vinfo = " << view_info << ", our vid = " << view_id_);
      return;
    }

    ++view_id_.first;
    ResetState();

    // Process pending sync interest
    SendDataInterest(pending_sync_interest->getName());
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

  recv_window_[id_].second.Insert(seq);

  auto n = MakeDataName(prefix_, id_, seq);
  std::shared_ptr<Data> data = std::make_shared<Data>(n);
  data->setFreshnessPeriod(time::seconds(3600));

  proto::Content content_proto;
  content_proto.set_view_num(view_id_.first);
  content_proto.set_leader_id(view_id_.second);
  EncodeVV(vector_clock_, content_proto.mutable_vv());
  content_proto.set_content(content);
  const std::string& content_proto_str = content_proto.SerializeAsString();
  data->setContent(reinterpret_cast<const uint8_t*>(content_proto_str.data()),
                   content_proto_str.size());
  data->setContentType(type);
  key_chain_.sign(*data, signingWithSha256());

  data_store_[n] = data;

  VSYNC_LOG_TRACE("Publish: d.name=" << n << ", content_type=" << type
                                     << ", view_id=" << view_id_
                                     << ", vector_clock=" << vector_clock_);

  // (Re)schedule next heartbeat event before publishing data
  heartbeat_event_ = scheduler_.scheduleEvent(
      kHeartbeatInterval +
          time::milliseconds(heartbeat_random_delay_(rengine_)),
      [this] { PublishHeartbeat(); });

  SendSyncInterest();
  if (lossy_mode_) {
    scheduler_.scheduleEvent(time::milliseconds(10),
                             [this] { SendSyncInterest(); });
    scheduler_.scheduleEvent(time::milliseconds(20),
                             [this] { SendSyncInterest(); });
  }

  return data;
}

void Node::SendDataInterest(const Name& sync_interest_name) {
  auto nid = sync_interest_name.get(-4).toUri();
  uint64_t seq = ExtractSequenceNumber(sync_interest_name);

  auto p_idx = view_info_.GetIndexByID(nid);
  if (!p_idx.second) {
    VSYNC_LOG_INFO("Unknown node id in sync interest name: " << nid);
    return;
  }
  auto p_pfx = view_info_.GetPrefixByIndex(p_idx.first);
  if (!p_pfx.second) {
    VSYNC_LOG_ERROR("Cannot get prefix for node id " << nid);
    return;
  }

  SendDataInterest(p_pfx.first, nid, seq);
}

void Node::SendDataInterest(const Name& prefix, const NodeID& nid,
                            uint64_t seq) {
  auto in = MakeDataName(prefix, nid, seq);
  Interest inst(in, kDataInterestLifetime);
  VSYNC_LOG_TRACE("Send: i.name=" << in);
  face_.expressInterest(inst, std::bind(&Node::OnRemoteData, this, _2),
                        [](const Interest&, const lp::Nack&) {},
                        std::bind(&Node::OnDataInterestTimeout, this, _1, 0));
  // TODO: use link when fetching missing data
}

void Node::OnDataInterestTimeout(const Interest& interest, int retry_count) {
  VSYNC_LOG_TRACE("Timeout: i.name=" << interest.getName()
                                     << ", retry_count=" << retry_count);
  if (retry_count > kInterestMaxRetrans) return;
  VSYNC_LOG_TRACE("Retrans: i.name=" << interest.getName());
  Interest i(interest.getName(), kDataInterestLifetime);
  // TBD: increase interest lifetime exponentially?
  face_.expressInterest(
      i, std::bind(&Node::OnRemoteData, this, _2),
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
  uint64_t seq = vector_clock_[idx_];
  auto n = MakeSyncInterestName(id_, view_id_, seq);

  Interest i(n, kSyncInterestLifetime);
  i.setMustBeFresh(true);
  if (lossy_mode_) i.setInterestLifetime(time::milliseconds(5));

  face_.expressInterest(
      i,
      [](const Interest& inst, const Data& ack) {
        VSYNC_LOG_TRACE("Recv: sync interest ack name=" << ack.getName());
        // TODO: update sync state with the version vector in the reply
      },
      [](const Interest&, const lp::Nack&) {},
      std::bind(&Node::OnSyncInterestTimeout, this, _1, 0));
  VSYNC_LOG_TRACE("Send: i.name=" << n);
}

void Node::OnSyncInterestTimeout(const Interest& interest, int retry_count) {
  VSYNC_LOG_TRACE("Timeout: i.name=" << interest.getName()
                                     << ", retry_count=" << retry_count);
  if (lossy_mode_) return;  // do not handle interest timeout in lossy mode
  if (retry_count > kInterestMaxRetrans) return;

  VSYNC_LOG_TRACE("Retrans: i.name=" << interest.getName());
  Interest i(interest.getName(), kSyncInterestLifetime);
  i.setMustBeFresh(true);
  // TBD: increase interest lifetime exponentially?

  face_.expressInterest(
      i,
      [](const Interest& inst, const Data& ack) {
        VSYNC_LOG_TRACE("Recv: sync interest ack name=" << ack.getName());
      },
      [](const Interest&, const lp::Nack&) {},
      std::bind(&Node::OnSyncInterestTimeout, this, _1, retry_count + 1));
}

void Node::SendSyncReply(const Name& n) {
  std::shared_ptr<Data> data = std::make_shared<Data>(n);
  data->setFreshnessPeriod(kSyncReplyFreshnessPeriod);
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
  if (n.size() < kSyncPrefix.size() + 4) {
    VSYNC_LOG_WARN("Invalid sync interest name: " << n);
    return;
  }

  auto vi = ExtractViewID(n);

  auto dispatcher = n.get(kSyncPrefix.size()).toUri();
  if (dispatcher == "vinfo") {
    auto iter = data_store_.find(n);
    if (iter != data_store_.end()) {
      VSYNC_LOG_TRACE("Send: d.name=" << iter->second->getName());
      face_.put(*iter->second);
    }
  } else if (dispatcher == "notify") {
    // Generate sync reply to notify receipt of sync interest
    SendSyncReply(n);

    // Check view id
    if (vi != view_id_) {
      DoViewChange(vi, interest.shared_from_this());
      return;
    }

    // Add a random delay before responding to sync interest
    scheduler_.scheduleEvent(
        time::milliseconds(data_interest_random_delay_(rengine_)),
        [this, n] { SendDataInterest(n); });

  } else {
    VSYNC_LOG_WARN("Unknown dispatch tag in interest name: " << dispatcher);
  }
}

void Node::ProcessState(const NodeID& nid, uint64_t seq, const ViewID& vid,
                        const VersionVector& vv) {
  // Check view id
  if (vid != view_id_) {
    VSYNC_LOG_INFO("Ignore version vector from different view: " << vid);
    return;
  }

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

  // Fetch missing data
  for (std::size_t i = 0; i != vv.size(); ++i) {
    if (i == idx_) continue;

    // Compare old_vv[i] with vv[i] before calculating "old_vv[i] + 1" to avoid
    // unsigned integer overflow (not possible in practice since we use 64-bit)
    if (old_vv[i] < vv[i]) {
      auto nid_pair = view_info_.GetIDByIndex(i);
      if (!nid_pair.second)
        throw Error("Cannot get node ID for index " + std::to_string(i));

      auto pfx_pair = view_info_.GetPrefixByIndex(i);
      if (!pfx_pair.second)
        throw Error("Cannot get node prefix for index " + std::to_string(i));

      if (nid == nid_pair.first) {
        if (vv[i] != seq) {
          throw Error("Node " + nid + " did not announce latest data: " +
                      std::to_string(vv[i]));
        }
      } else {
        for (uint64_t s = old_vv[i] + 1; s <= vv[i]; ++s) {
          SendDataInterest(pfx_pair.first, nid_pair.first, s);
        }
      }
    }
  }
}

void Node::OnRemoteData(const Data& data) {
  const auto& n = data.getName();
  if (n.size() < 2) {
    VSYNC_LOG_WARN("Invalid data name: " << n);
    return;
  }

  VSYNC_LOG_TRACE("Recv: d.name=" << n);

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

  const auto& content = data.getContent();
  proto::Content content_proto;
  if (!content_proto.ParseFromArray(content.value(), content.value_size())) {
    VSYNC_LOG_WARN("Invalid data content format: nid=" << nid);
    return;
  }

  ViewID vid = {content_proto.view_num(), content_proto.leader_id()};
  ProcessHeartbeat(vid, nid);

  auto vv = DecodeVV(content_proto.vv());
  ProcessState(nid, seq, vid, vv);

  auto content_type = data.getContentType();
  switch (content_type) {
    case kHeartbeat:
      // Nothing to do
      VSYNC_LOG_TRACE("Recv: Heartbeat packet from nid=" << nid);
      break;
    case kUserData:
      data_signal_(data.shared_from_this());
      break;
    case kNodeSnapshot:
      ProcessNodeSnapshot(content_proto.content(), nid);
      break;
    default:
      VSYNC_LOG_WARN("Unknown content type in remote data: " << content_type);
  }
}

void Node::UpdateReceiveWindow(const Name& pfx, const NodeID& nid,
                               uint64_t seq) {
  auto& entry = recv_window_[nid];
  if (entry.first.empty()) entry.first = pfx.toUri();
  auto& win = entry.second;

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

void Node::PublishHeartbeat() { PublishData("HELLO", kHeartbeat); }

void Node::ProcessHeartbeat(const ViewID& vid, const NodeID& nid) {
  if (vid != view_id_) {
    VSYNC_LOG_INFO("Ignore heartbeat for non-current view id "
                   << vid << " from nid=" << nid);
    return;
  }

  auto index = view_info_.GetIndexByID(nid);
  if (!index.second) {
    VSYNC_LOG_WARN("Unkown node id for heartbeat: nid=" << nid);
    return;
  }

  VSYNC_LOG_INFO("Update membership: nid=" << nid << " [index=" << index.first
                                           << "], view_id=" << vid);
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
    auto iter = recv_window_.find(p.first);
    if (iter == recv_window_.end()) {
      VSYNC_LOG_DEBUG("Node ID " << p.first << " not found in recv window");
      return;
    }
    const auto& prefix = iter->second.first;
    if (prefix.empty()) {
      VSYNC_LOG_DEBUG("Node prefix for ID " << p.first
                                            << " is not set in recv window");
      return;
    }
    auto* entry = ss_proto.add_entry();
    entry->set_prefix(prefix);
    entry->set_nid(p.first);
    entry->set_seq(p.second);
  }
  PublishData(ss_proto.SerializeAsString(), kNodeSnapshot);
}

void Node::ProcessNodeSnapshot(const std::string& content, const NodeID& nid) {
  proto::Snapshot ss_proto;
  if (!ss_proto.ParseFromArray(content.data(), content.size())) {
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

    // Check receive window and fetch the latest missing data
    // After receiving the latest data, VectorSync will fetch
    // all previous missing data automatically
    // Do not update receive window state right now
    if (entry.seq() > 0) {
      const auto iter = recv_window_.find(entry.nid());
      if (iter == recv_window_.end() ||
          iter->second.second.UpperBound() < entry.seq())
        SendDataInterest(entry.prefix(), entry.nid(), entry.seq());
    }
  }

  node_snapshot_bitmap_[index.first] = true;
  for (bool b : node_snapshot_bitmap_) {
    if (!b) return;
  }
  // Snapshot complete
  if (is_leader_) PublishGroupSnapshot();
}

void Node::PublishGroupSnapshot() {
  VSYNC_LOG_INFO("Snapshot up to view " << view_id_ << " is complete: {");
  for (const auto& p : snapshot_) {
    VSYNC_LOG_INFO("  " << p.first << ':' << p.second << ',');
  }
  VSYNC_LOG_INFO('}');

  proto::Snapshot ss_proto;
  ss_proto.set_view_num(view_id_.first);
  ss_proto.set_leader_id(view_id_.second);
  for (const auto& p : snapshot_) {
    auto iter = recv_window_.find(p.first);
    if (iter == recv_window_.end()) {
      VSYNC_LOG_DEBUG("Node ID " << p.first << " not found in recv window");
      return;
    }
    const auto& prefix = iter->second.first;
    if (prefix.empty()) {
      VSYNC_LOG_DEBUG("Node prefix for ID " << p.first
                                            << " is not set in recv window");
      return;
    }
    auto* entry = ss_proto.add_entry();
    entry->set_prefix(prefix);
    entry->set_nid(p.first);
    entry->set_seq(p.second);
  }

  auto n = MakeSnapshotName(view_id_);
  VSYNC_LOG_TRACE("Publish: d.name=" << n);
  std::string content = ss_proto.SerializeAsString();
  std::shared_ptr<Data> d = std::make_shared<Data>(n);
  d->setFreshnessPeriod(time::seconds(3600));
  d->setContent(reinterpret_cast<const uint8_t*>(content.data()),
                content.size());
  d->setContentType(kGroupSnapshot);
  key_chain_.sign(*d, signingWithSha256());
  data_store_[n] = d;
}

}  // namespace vsync
}  // namespace ndn
