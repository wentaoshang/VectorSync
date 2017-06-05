/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <algorithm>
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

static constexpr time::milliseconds kDataFreshnessPeriod = time::seconds(3600);

static constexpr time::milliseconds kSyncReplyFreshnessPeriod =
    time::milliseconds(5);

static time::milliseconds kHeartbeatInterval = time::milliseconds(1000);
static constexpr time::milliseconds kHeartbeatMaxDelay =
    time::milliseconds(100);
static time::milliseconds kHeartbeatTimeout = 3 * kHeartbeatInterval;
static time::milliseconds kHealthcheckInterval = kHeartbeatInterval;

void SetHeartbeatInterval(const time::milliseconds heartbeat_interval) {
  kHeartbeatInterval = kHealthcheckInterval = heartbeat_interval;
  kHeartbeatTimeout = 3 * kHeartbeatInterval;
}

// Leader will only perform view change when the number of dead members exceeds
// kViewChangeThreadhold.
static const size_t kViewChangeThreshold = 1;

// end of VectorSync parameters

Node::Node(Face& face, Scheduler& scheduler, KeyChain& key_chain,
           const Name& nid, uint32_t seed)
    : face_(face),
      scheduler_(scheduler),
      key_chain_(key_chain),
      nid_(nid),
      vid_({1, nid}),
      vinfo_({{nid}}),
      rengine_(seed),
      heartbeat_random_delay_(10,
                              time::milliseconds(kHeartbeatMaxDelay).count()),
      heartbeat_event_(scheduler_),
      healthcheck_event_(scheduler_) {
  recv_window_[nid_] = {};
}

void Node::Start() {
  face_.setInterestFilter(
      kSyncPrefix, std::bind(&Node::OnSyncInterest, this, _2),
      [this](const Name&, const std::string& reason) {
        throw Error("Failed to register vsync prefix: " + reason);
      });

  face_.setInterestFilter(
      nid_, std::bind(&Node::OnDataInterest, this, _2),
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
  idx_ = vinfo_.GetIndexByID(nid_).first;
  is_leader_ = vid_.leader_name == nid_;
  view_change_signal_(vid_, vinfo_, is_leader_);
  VSYNC_LOG_INFO("Move to new view: vid=" << vid_ << ", vinfo=" << vinfo_);

  if (is_leader_) PublishViewInfo();

  vv_.clear();
  vv_.resize(vinfo_.Size());

  last_heartbeat_.clear();
  auto now = time::steady_clock::now();
  last_heartbeat_.resize(vinfo_.Size(), now);

  // Preserve sequence numbers for the nodes that survived the view change
  for (std::size_t i = 0; i < vinfo_.Size(); ++i) {
    auto nid = vinfo_.GetIDByIndex(i).first;
    auto& rw = recv_window_[nid];
    vv_[i] = rw.UpperBound();
  }
  vector_change_signal_(idx_, vv_);
  /*
  // Update snapshot to the latest sequence numbers in receive windows
  for (const auto& prw : recv_window_) {
    const auto& nid = prw.first;
    const auto& rw = prw.second;
    auto& s = snapshot_[nid];
    uint64_t seq = rw.UpperBound();
    if (s < seq) s = seq;
  }
  PublishNodeSnapshot();

  node_snapshot_bitmap_.clear();
  node_snapshot_bitmap_.resize(vinfo_.Size(), false);
  node_snapshot_bitmap_[idx_] = true;
  if (vinfo_.Size() == 1) {
    // We are the only node in this view
    PublishGroupSnapshot();
  }
  */
}

bool Node::LoadView(const ViewID& vid, const ViewInfo& vinfo) {
  auto lp = vinfo.GetIndexByID(vid.leader_name);
  if (!lp.second)
    throw Error("Leader " + vid.leader_name.toUri() + " not in vinfo");
  if (lp.first != vinfo.Size() - 1)
    throw Error("Leader " + vid.leader_name.toUri() +
                " is not ordered highest by name in vinfo");

  auto p = vinfo.GetIndexByID(nid_);
  if (!p.second) {
    VSYNC_LOG_WARN("View info does not contain self node name " << nid_);
    return false;
  }

  idx_ = p.first;
  vid_ = vid;
  vinfo_ = vinfo;
  ResetState();

  return true;
}

void Node::DoViewChange(const ViewID& vid) {
  const auto& view_num = vid.view_num;
  const auto& leader_name = vid.leader_name;
  if (view_num > vid_.view_num || (is_leader_ && nid_ > leader_name)) {
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
  VSYNC_LOG_TRACE("Timeout: i.name=" << interest.getName()
                                     << ", retry_count=" << retry_count);
  if (retry_count > kInterestMaxRetrans) return;
  VSYNC_LOG_TRACE("Retrans: i.name=" << interest.getName());
  Interest i(interest.getName(), kViewInfoInterestLifetime);
  // TBD: increase interest lifetime exponentially?
  face_.expressInterest(
      i, std::bind(&Node::ProcessViewInfo, this, _1, _2),
      [](const Interest&, const lp::Nack&) {},
      std::bind(&Node::OnViewInfoInterestTimeout, this, _1, retry_count + 1));
}

void Node::ProcessViewInfo(const Interest& vinterest, const Data& vinfo) {
  const auto& n = vinfo.getName();
  VSYNC_LOG_TRACE("Recv: d.name=" << n);

  const auto& content = vinfo.getContent();
  ViewInfo view_info;
  if (!view_info.Decode(content.value(), content.value_size())) {
    VSYNC_LOG_WARN("Cannot decode view info");
    return;
  }

  ViewID vid = ExtractViewID(n);
  VSYNC_LOG_TRACE("Recv: vid=" << vid << ", vinfo=" << view_info);

  // Store a local copy of view info data
  data_store_[n] = vinfo.shared_from_this();
  // TODO: verify view info using common trust anchor

  if (vid.view_num > vid_.view_num &&
      (!is_leader_ || (is_leader_ && nid_ < vid.leader_name))) {
    // Move to higher-numbered view using received view info if we are not
    // leader, or if we are leader but has a smaller name
    if (!LoadView(vid, view_info)) {
      VSYNC_LOG_WARN("Cannot load received view: vinfo=" << view_info);
      return;
    }

    // Process pending state vector published in the new view
  } else if (is_leader_ && nid_ > vid.leader_name) {
    // Merge the two views if we are leader with a larger name
    bool r = vinfo_.Merge(view_info);
    if (!r && vid.view_num < vid_.view_num) {
      // No need to do view change since there is no change to group
      // membership and current view number is higher than the received one.
      // The node with lower view number will move to higher view anyway.
      VSYNC_LOG_INFO(
          "Received view info has no new member and we are in a higher view: "
          << "received vinfo = " << view_info << ", our vid = " << vid_);
      return;
    }

    vid_.view_num = std::max(vid_.view_num, vid.view_num) + 1;
    ResetState();
  }
}  // namespace vsync

void Node::PublishViewInfo() {
  auto n = MakeViewInfoName(vid_);
  VSYNC_LOG_TRACE("Publish: d.name=" << n << ", vinfo=" << vinfo_);
  std::string content;
  vinfo_.Encode(content);
  std::shared_ptr<Data> d = std::make_shared<Data>(n);
  d->setFreshnessPeriod(kDataFreshnessPeriod);
  d->setContent(reinterpret_cast<const uint8_t*>(content.data()),
                content.size());
  d->setContentType(kViewInfo);
  key_chain_.sign(*d);
  data_store_[n] = d;
}

std::shared_ptr<const Data> Node::PublishData(const std::string& content,
                                              uint32_t type) {
  uint64_t seq = ++vv_[idx_];
  vector_change_signal_(idx_, vv_);

  recv_window_[nid_].Insert(seq);

  auto n = MakeDataName(nid_, seq);
  std::shared_ptr<Data> data = std::make_shared<Data>(n);
  data->setFreshnessPeriod(kDataFreshnessPeriod);

  proto::Content content_proto;
  content_proto.set_view_num(vid_.view_num);
  content_proto.set_leader_name(vid_.leader_name.toUri());
  EncodeVV(vv_, content_proto.mutable_vv());
  content_proto.set_content(content);
  const std::string& content_proto_str = content_proto.SerializeAsString();
  data->setContent(reinterpret_cast<const uint8_t*>(content_proto_str.data()),
                   content_proto_str.size());
  data->setContentType(type);
  key_chain_.sign(*data);

  data_store_[n] = data;

  VSYNC_LOG_TRACE("Publish: d.name=" << n << ", content_type=" << type
                                     << ", view_id=" << vid_
                                     << ", vector=" << vv_);

  // (Re)schedule next heartbeat event before publishing data
  heartbeat_event_ = scheduler_.scheduleEvent(
      kHeartbeatInterval +
          time::milliseconds(heartbeat_random_delay_(rengine_)),
      [this] { PublishHeartbeat(); });

  SendSyncInterest();

  return data;
}

void Node::SendDataInterest(const Name& data_name) {
  Interest inst(data_name, kDataInterestLifetime);
  VSYNC_LOG_TRACE("Send: i.name=" << data_name);
  face_.expressInterest(inst, std::bind(&Node::OnRemoteData, this, _2),
                        [](const Interest&, const lp::Nack&) {},
                        std::bind(&Node::OnDataInterestTimeout, this, _1, 0));
  // TODO: use link when fetching missing data
}

void Node::SendDataInterest(const Name& nid, uint64_t seq) {
  auto dn = MakeDataName(nid, seq);
  SendDataInterest(dn);
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
  uint64_t seq = vv_[idx_];
  auto n = MakeSyncInterestName(vid_, nid_, seq);

  Interest i(n, kSyncInterestLifetime);
  i.setMustBeFresh(true);

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
  EncodeVV(vv_, vv_encode);
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
  if (n.size() < kSyncPrefix.size() + 3) {
    VSYNC_LOG_WARN("Invalid sync interest name: " << n);
    return;
  }

  auto dispatcher = n.get(kSyncPrefix.size()).toUri();
  if (dispatcher == "vinfo") {
    auto iter = data_store_.find(n);
    if (iter != data_store_.end()) {
      VSYNC_LOG_TRACE("Send: d.name=" << iter->second->getName());
      face_.put(*iter->second);
    }
  } else if (dispatcher == "vid") {
    // Generate sync reply to notify receipt of sync interest
    SendSyncReply(n);

    // Fetch data
    auto dn = ExtractDataName(n);
    if (!dn.empty()) SendDataInterest(dn);

    // Check view id
    auto vi = ExtractViewID(n);
    if (vi != vid_) DoViewChange(vi);
  } else {
    VSYNC_LOG_WARN("Unknown dispatch tag in interest name: " << dispatcher);
  }
}

void Node::ProcessState(const Name& nid, uint64_t seq, const ViewID& vid,
                        const VersionVector& vv) {
  // Check view id
  if (vid != vid_) {
    VSYNC_LOG_INFO("Ignore version vector from different view: " << vid);
    return;
  }

  // Detect invalid version vector
  if (vv.size() != vv_.size()) {
    VSYNC_LOG_INFO("Ignore version vector of different size: " << vv);
    return;
  }

  VSYNC_LOG_TRACE("Recv: vector=" << vv);

  if (vv[idx_] > vv_[idx_]) {
    VSYNC_LOG_INFO(
        "Ignore vector clock with larger self sequence number: " << vv);
  }

  // Process version vector
  VersionVector old_vv = vv_;
  vv_ = Join(old_vv, vv);
  vector_change_signal_(idx_, vv_);
  VSYNC_LOG_INFO("Update: view_id=" << vid_ << ", vector=" << vv_);

  // Fetch missing data
  for (std::size_t i = 0; i != vv_.size(); ++i) {
    if (i == idx_) continue;

    // Compare old_vv[i] with vv_[i] before calculating "old_vv[i] + 1" to avoid
    // unsigned integer overflow (not possible in practice since we use 64-bit)
    if (old_vv[i] < vv_[i]) {
      auto nid_pair = vinfo_.GetIDByIndex(i);
      if (!nid_pair.second)
        throw Error("Cannot get node ID for index " + std::to_string(i));

      if (nid == nid_pair.first) {
        if (vv_[i] != seq) {
          throw Error(
              "Node " + nid.toUri() +
              " did not announce latest data: " + std::to_string(vv_[i]));
        }
      } else {
        for (uint64_t s = old_vv[i] + 1; s <= vv_[i]; ++s) {
          SendDataInterest(nid_pair.first, s);
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

  auto nid = ExtractNodeID(n);
  auto seq = ExtractSequenceNumber(n);

  UpdateReceiveWindow(nid, seq);

  // Store a local copy of received data
  data_store_[n] = data.shared_from_this();

  const auto& content = data.getContent();
  proto::Content content_proto;
  if (!content_proto.ParseFromArray(content.value(), content.value_size())) {
    VSYNC_LOG_WARN("Invalid data content format: nid=" << nid);
    return;
  }

  ViewID vid = {content_proto.view_num(), content_proto.leader_name()};
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
      // ProcessNodeSnapshot(content_proto.content(), nid);
      break;
    default:
      VSYNC_LOG_WARN("Unknown content type in remote data: " << content_type);
  }
}

void Node::UpdateReceiveWindow(const Name& nid, uint64_t seq) {
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

  // TODO: let application decide whether to fetch missing data?
  for (auto iter = boost::icl::elements_begin(missing_seq_intervals);
       iter != boost::icl::elements_end(missing_seq_intervals); ++iter) {
    SendDataInterest(nid, *iter);
  }
}

void Node::PublishHeartbeat() { PublishData("HELLO", kHeartbeat); }

void Node::ProcessHeartbeat(const ViewID& vid, const Name& nid) {
  if (vid != vid_) {
    VSYNC_LOG_INFO("Ignore heartbeat for non-current view id "
                   << vid << " from nid=" << nid);
    return;
  }

  auto index = vinfo_.GetIndexByID(nid);
  if (!index.second) {
    VSYNC_LOG_WARN("Ignore heartbeat from unkown node: nid=" << nid);
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

  // Check heartbeat status of every node
  auto now = time::steady_clock::now();

  std::unordered_set<Name> dead_nodes;
  for (std::size_t i = 0; i != vinfo_.Size(); ++i) {
    if (i == idx_) continue;

    if (last_heartbeat_[i] + kHeartbeatTimeout < now) {
      auto p = vinfo_.GetIDByIndex(i);
      if (!p.second)
        throw Error("Cannot find node ID for index " + std::to_string(i));

      dead_nodes.insert(p.first);
      VSYNC_LOG_INFO("Found dead node: " << p.first);
    }
  }

  if (is_leader_) {
    if (dead_nodes.size() >= kViewChangeThreshold) {
      vinfo_.Remove(dead_nodes);
      ++vid_.view_num;
      ResetState();
    }
  } else {
    const auto& leader_name = vid_.leader_name;

    if (dead_nodes.find(leader_name) != dead_nodes.end()) {
      VSYNC_LOG_INFO("Current leader " << leader_name << " is dead");
      // Find next live member with highest-ordered name
      assert(vinfo_.Size() > 1);
      std::size_t i = vinfo_.Size();
      for (; i != 0; --i) {
        if (i - 1 == idx_ || last_heartbeat_[i - 1] + kHeartbeatTimeout >= now)
          break;
      }

      if (i - 1 == idx_) {
        // We are the next leader
        VSYNC_LOG_INFO("Take leader role");
        vinfo_.Remove(dead_nodes);
        vid_ = {vid_.view_num + 1, nid_};
        ResetState();
      }
    }
  }
}

/*
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
*/
}  // namespace vsync
}  // namespace ndn
