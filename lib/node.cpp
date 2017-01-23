/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <random>

#include <boost/log/trivial.hpp>

#include <ndn-cxx/util/digest.hpp>

#include "node.hpp"
#include "vsync-helper.hpp"

namespace ndn {
namespace vsync {

Node::Node(Face& face, Scheduler& scheduler, KeyChain& key_chain,
           const NodeID& nid, const Name& prefix, Node::DataCb on_data)
    : face_(face),
      scheduler_(scheduler),
      key_chain_(key_chain),
      id_(name::Component(nid).toUri()),
      prefix_(prefix),
      view_id_({1, nid}),
      view_info_({{nid, prefix}}),
      data_cb_(std::move(on_data)),
      rengine_(rdevice_()),
      rdist_(100, time::milliseconds(kLeaderElectionTimeoutMax).count()),
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

  heartbeat_event_ = scheduler_.scheduleEvent(kHeartbeatInterval,
                                              [this] { PublishHeartbeat(); });

  healthcheck_event_ = scheduler_.scheduleEvent(kHealthcheckInterval,
                                                [this] { DoHealthcheck(); });
}

void Node::ResetState() {
  idx_ = view_info_.GetIndexByID(id_).first;
  is_leader_ = view_id_.second == id_;

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
}

bool Node::LoadView(const ViewID& vid, const ViewInfo& vinfo) {
  auto p = vinfo.GetIndexByID(id_);
  if (!p.second) {
    BOOST_LOG_TRIVIAL(debug) << "View info does not contain self node ID "
                             << id_;
    return false;
  }

  BOOST_LOG_TRIVIAL(trace) << "Load view: vid=" << ToString(vid)
                           << ",vinfo=" << vinfo;

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

    Interest i(n, time::seconds(4));
    BOOST_LOG_TRIVIAL(trace) << "Send: i.name=" << n.toUri();
    face_.expressInterest(i, std::bind(&Node::ProcessViewInfo, this, _1, _2),
                          [](const Interest&, const lp::Nack&) {},
                          [](const Interest&) {});
  }
}

void Node::ProcessViewInfo(const Interest& vinterest, const Data& vinfo) {
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

  // Store a local copy of view info data
  data_store_[n] = vinfo.shared_from_this();
  // TODO: verify view info using common trust anchor

  ViewID vid = ExtractViewID(n);
  if (vid.first > view_id_.first) {
    if (!LoadView(vid, view_info)) {
      BOOST_LOG_TRIVIAL(debug) << "Cannot load received view: vinfo="
                               << view_info;
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
    BOOST_LOG_TRIVIAL(trace) << "Move to new view: vid=" << ToString(view_id_)
                             << ",vinfo=" << view_info_;
    ResetState();
    PublishViewInfo();
    PublishHeartbeat();
  }
}

void Node::PublishViewInfo() {
  auto n = MakeViewInfoName(view_id_);
  BOOST_LOG_TRIVIAL(trace) << "Publish: d.name=" << n.toUri()
                           << ",vinfo=" << view_info_;
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

VersionVector Node::PublishData(const std::string& content, uint32_t type) {
  uint64_t seq = ++vector_clock_[idx_];
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

  BOOST_LOG_TRIVIAL(trace) << "Publish: d.name=" << n.toUri()
                           << ", vid=" << ToString(view_id_)
                           << ", vv=" << ToString(vv);

  SendSyncInterest();

  return vv;
}

void Node::SendDataInterest(const Name& prefix, const NodeID& nid,
                            uint64_t seq) {
  auto in = MakeDataName(prefix, nid, seq);
  Interest inst(in, time::milliseconds(1000));
  BOOST_LOG_TRIVIAL(trace) << "Send: i.name=" << in.toUri();
  face_.expressInterest(inst, std::bind(&Node::OnRemoteData, this, _2),
                        [](const Interest&, const lp::Nack&) {},
                        [](const Interest&) {});
  // TODO: retransmit interest upon timeout
}

void Node::OnDataInterest(const Interest& interest) {
  const auto& n = interest.getName();
  BOOST_LOG_TRIVIAL(trace) << "Recv: i.name=" << n.toUri();
  auto iter = data_store_.find(n);
  if (iter == data_store_.end()) {
    BOOST_LOG_TRIVIAL(debug) << "Unknown data name: " << n.toUri();
    // TODO: send L7 nack based on the sequence number in the Interest name
  } else {
    BOOST_LOG_TRIVIAL(trace) << "Send: d.name="
                             << iter->second->getName().toUri();
    face_.put(*iter->second);
  }
}

void Node::SendSyncInterest() {
  util::Sha256 hasher;
  hasher.update(reinterpret_cast<const uint8_t*>(vector_clock_.data()),
                vector_clock_.size() * sizeof(vector_clock_[0]));
  auto digest = hasher.toString();

  auto n = MakeSyncInterestName(view_id_, digest);

  PublishVector(n, digest);

  BOOST_LOG_TRIVIAL(trace) << "Send: i.name=" << n.toUri();
  Interest i(n, time::milliseconds(1000));
  face_.expressInterest(i, [](const Interest&, const Data&) {},
                        [](const Interest&, const lp::Nack&) {},
                        [](const Interest&) {});
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
  BOOST_LOG_TRIVIAL(trace) << "Recv: i.name=" << n.toUri();

  // Check sync interest name size
  if (n.size() != kSyncPrefix.size() + 4) {
    BOOST_LOG_TRIVIAL(error) << "Invalid sync interest name: " << n.toUri();
    return;
  }

  auto vi = ExtractViewID(n);
  auto digest = ExtractVectorDigest(n);

  auto dispatcher = n.get(-4).toUri();
  if (dispatcher == "vinfo") {
    auto iter = data_store_.find(n);
    if (iter != data_store_.end()) {
      BOOST_LOG_TRIVIAL(trace) << "Send: d.name="
                               << iter->second->getName().toUri();
      face_.put(*iter->second);
    }
  } else if (dispatcher == "digest") {
    // Generate sync reply to purge pending sync Interest
    // TBD: should we leave vectorsync interest in the PIT?
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
      BOOST_LOG_TRIVIAL(trace) << "Send: d.name="
                               << iter->second->getName().toUri();
      face_.put(*iter->second);
    }
  } else {
    BOOST_LOG_TRIVIAL(info) << "Unknown dispatch tag in interest name: "
                            << dispatcher;
  }
}

void Node::SendVectorInterest(const Name& sync_interest_name) {
  auto n = MakeVectorInterestName(sync_interest_name);
  // Ignore sync interest if the vector data has been fetched before
  if (data_store_.find(n) != data_store_.end()) return;

  BOOST_LOG_TRIVIAL(trace) << "Send: i.name=" << n.toUri();
  Interest i(n, time::milliseconds(1000));
  face_.expressInterest(i, std::bind(&Node::ProcessVector, this, _2),
                        [](const Interest&, const lp::Nack&) {},
                        [](const Interest&) {});
}

void Node::PublishVector(const Name& sync_interest_name,
                         const std::string& digest) {
  auto n = MakeVectorInterestName(sync_interest_name);
  BOOST_LOG_TRIVIAL(trace) << "Publish: d.name=" << n.toUri()
                           << ",vector_clock=" << ToString(vector_clock_);

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
  BOOST_LOG_TRIVIAL(trace) << "Recv: d.name=" << n.toUri();

  if (data.getContentType() != kVectorClock) {
    BOOST_LOG_TRIVIAL(info) << "Wrong content type for vector data";
    return;
  }

  auto vi = ExtractViewID(n);
  if (vi != view_id_) {
    BOOST_LOG_TRIVIAL(info) << "Ignore version vector from different view: "
                            << ToString(vi);
    return;
  }

  // Parse version vector
  const auto& content = data.getContent();
  auto vv = DecodeVV(content.value(), content.value_size());

  // Detect invalid version vector
  if (vv.size() != vector_clock_.size()) {
    BOOST_LOG_TRIVIAL(info) << "Ignore version vector of different size: "
                            << vv.size();
    return;
  }

  BOOST_LOG_TRIVIAL(trace) << "Recv: vector_clock=" << ToString(vv);

  if (vv[idx_] > vector_clock_[idx_]) {
    BOOST_LOG_TRIVIAL(info)
        << "Ignore version vector with larger sequence number for ourselves: "
        << ToString(vv);
  }

  // Process version vector
  VersionVector old_vv = vector_clock_;
  vector_clock_ = Merge(old_vv, vv);

  BOOST_LOG_TRIVIAL(trace) << "Update: view_id=" << ToString(view_id_)
                           << ",vector_clock=" << ToString(vector_clock_);

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
    BOOST_LOG_TRIVIAL(error) << "Invalid data name: " << n.toUri();
    return;
  }

  const auto& content = data.getContent();
  proto::Content content_proto;
  if (content_proto.ParseFromArray(content.value(), content.value_size())) {
    ViewID vi = {content_proto.view_num(), content_proto.leader_id()};
    BOOST_LOG_TRIVIAL(trace) << "Recv: d.name=" << n.toUri()
                             << ", vid=" << ToString(vi);

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
      if (data_cb_) data_cb_(content_proto.user_data(), vi, vv);
    }
  } else {
    BOOST_LOG_TRIVIAL(error) << "Invalid content format: d.name=" << n.toUri();
  }
}

void Node::UpdateReceiveWindow(const Data& data, const NodeID& nid,
                               uint64_t seq) {
  auto& win = recv_window_[nid];

  // Insert the new seq number into the receive window
  BOOST_LOG_TRIVIAL(trace) << "Insert into recv_window[" << nid
                           << "]: seq=" << seq;
  win.Insert(seq);

  // Check for missing data
  const auto& missing_seq_intervals = win.CheckForMissingData(seq);
  if (missing_seq_intervals.empty()) {
    BOOST_LOG_TRIVIAL(trace) << "No missing data from node " << nid
                             << " before seq num " << seq;
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
  heartbeat_event_ = scheduler_.scheduleEvent(kHeartbeatInterval,
                                              [this] { PublishHeartbeat(); });

  PublishData("Heartbeat", kHeartbeat);
}

void Node::ProcessHeartbeat(const ViewID& vid, const NodeID& nid) {
  if (vid != view_id_) {
    BOOST_LOG_TRIVIAL(debug) << "Ignore heartbeat for non-current view id "
                             << ToString(vid);
    return;
  }

  auto index = view_info_.GetIndexByID(nid);
  if (!index.second) {
    BOOST_LOG_TRIVIAL(debug) << "Unkown node id in received heartbeat: " << nid;
    return;
  }

  BOOST_LOG_TRIVIAL(trace) << "Recv: HEARTBEAT from node " << nid
                           << " [idx=" << index.first << ']';
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
        BOOST_LOG_TRIVIAL(trace) << "Node " << p.first << " is dead";
      }
    }
    if (dead_nodes.size() >= kViewChangeThreshold) {
      view_info_.Remove(dead_nodes);
      ++view_id_.first;
      ResetState();
      PublishViewInfo();
      BOOST_LOG_TRIVIAL(debug)
          << "Move to new view: view_id=" << ToString(view_id_)
          << ",vinfo=" << view_info_;
      PublishHeartbeat();
    }
  } else {
    const auto& leader_id = view_id_.second;
    auto p = view_info_.GetIndexByID(leader_id);
    if (!p.second)
      throw Error("Cannot find node index for leader " + leader_id);

    if (last_heartbeat_[p.first] + kHeartbeatTimeout < now) {
      BOOST_LOG_TRIVIAL(debug) << "Leader " << leader_id << " is dead";
      // Start leader election timer
      leader_election_event_ =
          scheduler_.scheduleEvent(time::milliseconds(rdist_(rengine_)),
                                   [this] { ProcessLeaderElectionTimeout(); });
    }
  }
}

void Node::ProcessLeaderElectionTimeout() {
  BOOST_LOG_TRIVIAL(trace) << "Leader election timer goes off";
  // Remove leader from the view
  view_info_.Remove({view_id_.second});
  // Set self as leader for the new view
  view_id_ = {view_id_.first + 1, id_};
  ResetState();
  PublishViewInfo();
  BOOST_LOG_TRIVIAL(debug) << "Move to new view: view_id=" << ToString(view_id_)
                           << ",vinfo=" << view_info_;
  PublishHeartbeat();
}

void Node::PrintCausalityGraph() const {
  BOOST_LOG_TRIVIAL(trace) << "CausalGraph:";
  for (const auto& p : causality_graph_) {
    BOOST_LOG_TRIVIAL(trace) << " ViewID=(" << p.first.first << ","
                             << p.first.second << "):";
    for (const auto& pvv_queue : p.second) {
      BOOST_LOG_TRIVIAL(trace) << "  NodeID=" << pvv_queue.first << ":";
      BOOST_LOG_TRIVIAL(trace) << "   Queue=[";
      for (const auto& pvv : pvv_queue.second) {
        BOOST_LOG_TRIVIAL(trace) << "      " << ToString(pvv.first) << ":"
                                 << pvv.second->getName().toUri();
      }
      BOOST_LOG_TRIVIAL(trace) << "         ]";
    }
  }
}

}  // namespace vsync
}  // namespace ndn
