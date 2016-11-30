/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <random>

#include <boost/log/trivial.hpp>

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
      kVsyncPrefix, std::bind(&Node::OnSyncInterest, this, _2),
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

  version_vector_.clear();
  version_vector_.resize(view_info_.Size());

  last_heartbeat_.clear();
  auto now = time::steady_clock::now();
  last_heartbeat_.resize(view_info_.Size(), now);

  auto& graph = causality_graph_[view_id_];

  for (std::size_t i = 0; i < view_info_.Size(); ++i) {
    auto nid = view_info_.GetIDByIndex(i).first;
    recv_window_.insert({nid, {}});
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

  // TODO: verify view info using common trust anchor

  ViewID vid = ExtractViewID(n);
  if (vid.first > view_id_.first) {
    if (!LoadView(vid, view_info)) {
      BOOST_LOG_TRIVIAL(debug) << "Cannot load received view " << view_info;
      return;
    }

    // Cancel any leader election event
    leader_election_event_.cancel();

    // Store a local copy of view info data
    data_store_[n] = vinfo.shared_from_this();
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
    PublishViewInfo();
    PublishHeartbeat();
  }
  BOOST_LOG_TRIVIAL(debug) << "Move to new view: id=(" << view_id_.first << ","
                           << view_id_.second << "), vinfo=" << view_info_;
}

void Node::PublishViewInfo() {
  auto n = MakeViewInfoName(view_id_);
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

void Node::PublishData(const std::string& content, uint32_t type) {
  uint64_t seq = ++version_vector_[idx_];
  auto n = MakeDataName(prefix_, id_, view_id_, seq);

  if (seq == 1) {
    // The first data packet with sequence number 1 contains the view id and
    // sequence number of the last data packet published in the previous view.
    std::string ldi_wire;
    EncodeESN(last_data_info_, ldi_wire);
    std::shared_ptr<Data> d = std::make_shared<Data>(n);
    d->setFreshnessPeriod(time::seconds(3600));
    d->setContent(reinterpret_cast<const uint8_t*>(ldi_wire.data()),
                  ldi_wire.size());
    d->setContentType(kLastDataInfo);
    key_chain_.sign(*d, signingWithSha256());
    data_store_[n] = d;

    BOOST_LOG_TRIVIAL(trace) << "Publish: " << n.toUri()
                             << " with last data info " << last_data_info_;

    last_data_info_ = {view_id_, seq};
    seq = ++version_vector_[idx_];
    n = MakeDataName(prefix_, id_, view_id_, seq);
  }

  std::shared_ptr<Data> data = std::make_shared<Data>(n);
  data->setFreshnessPeriod(time::seconds(3600));
  // Add version vector tag for user data
  auto vv = GenerateDataVV();
  proto::Content content_proto;
  auto* vv_proto = content_proto.mutable_vv();
  EncodeVV(vv, vv_proto);
  content_proto.set_user_data(content);
  const std::string& content_proto_str = content_proto.SerializeAsString();
  data->setContent(reinterpret_cast<const uint8_t*>(content_proto_str.data()),
                   content_proto_str.size());
  data->setContentType(type);
  key_chain_.sign(*data, signingWithSha256());

  data_store_[n] = data;
  last_data_info_ = {view_id_, seq};
  auto& queue = causality_graph_[view_id_][id_];
  queue.insert({vv, data});
  PrintCausalityGraph();

  BOOST_LOG_TRIVIAL(trace) << "Publish: " << n.toUri();

  SendSyncInterest();
}

void Node::SendDataInterest(const Name& prefix, const NodeID& nid,
                            const ViewID& vid, uint64_t seq) {
  auto in = MakeDataName(prefix, nid, vid, seq);
  Interest inst(in, time::milliseconds(1000));
  BOOST_LOG_TRIVIAL(trace) << "Ftch: i.name=" << in.toUri();
  face_.expressInterest(inst, std::bind(&Node::OnRemoteData, this, _2),
                        [](const Interest&, const lp::Nack&) {},
                        [](const Interest&) {});
}

void Node::OnDataInterest(const Interest& interest) {
  const auto& n = interest.getName();
  auto iter = data_store_.find(n);
  if (iter == data_store_.end()) {
    BOOST_LOG_TRIVIAL(debug) << "Unrecognized data interest: " << n.toUri();
    // TODO: send L7 nack based on the sequence number in the Interest name
  } else {
    face_.put(*iter->second);
  }
}

void Node::SendSyncInterest() {
  auto n = MakeVsyncInterestName(view_id_, version_vector_);
  BOOST_LOG_TRIVIAL(trace) << "Send: vi=(" << view_id_.first << ","
                           << view_id_.second
                           << "), vv=" << ToString(version_vector_);
  Interest i(n, time::milliseconds(1000));
  face_.expressInterest(i, [](const Interest&, const Data&) {},
                        [](const Interest&, const lp::Nack&) {},
                        [](const Interest&) {});
}

void Node::SendSyncReply(const Name& n) {
  std::shared_ptr<Data> data = std::make_shared<Data>(n);
  data->setFreshnessPeriod(time::milliseconds(50));
  std::string vv_encode;
  EncodeVV(version_vector_, vv_encode);
  data->setContent(reinterpret_cast<const uint8_t*>(vv_encode.data()),
                   vv_encode.size());
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
  if (n.size() != kVsyncPrefix.size() + 3) {
    BOOST_LOG_TRIVIAL(error) << "Invalid sync interest name: " << n.toUri();
    return;
  }

  auto vi = ExtractViewID(n);
  auto vv = ExtractVersionVector(n);

  BOOST_LOG_TRIVIAL(trace) << "Recv: vi=(" << vi.first << "," << vi.second
                           << "),vv=" << ToString(vv);

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

  // Process version vector
  VersionVector old_vv = version_vector_;
  version_vector_ = Merge(old_vv, vv);

  BOOST_LOG_TRIVIAL(trace) << "Updt: vi=(" << view_id_.first << ","
                           << view_id_.second
                           << "), vv=" << ToString(version_vector_);

  // Generate sync reply to purge pending sync Interest
  SendSyncReply(n);

  for (std::size_t i = 0; i < vv.size(); ++i) {
    if (i == idx_) continue;

    auto nid = view_info_.GetIDByIndex(i);
    if (!nid.second)
      throw Error("Cannot get node ID for index " + std::to_string(i));

    auto pfx = view_info_.GetPrefixByIndex(i);
    if (!pfx.second)
      throw Error("Cannot get node prefix for index " + std::to_string(i));

    if (old_vv[i] < vv[i]) {
      for (uint64_t seq = old_vv[i] + 1; seq <= vv[i]; ++seq) {
        SendDataInterest(pfx.first, nid.first, vi, seq);
      }
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
  auto vi = ExtractViewID(n);
  auto seq = ExtractSequenceNumber(n);

  UpdateReceiveWindow(data, nid, vi, seq);

  // Store a local copy of received data
  data_store_[n] = data.shared_from_this();

  auto content_type = data.getContentType();
  if (content_type == kHeartbeat) {
    ProcessHeartbeat(vi, nid);
  } else if (content_type == kUserData) {
    const auto& content = data.getContent();
    proto::Content content_proto;
    if (content_proto.ParseFromArray(content.value(), content.value_size())) {
      auto vv = DecodeVV(content_proto.vv());
      auto& queue = causality_graph_[vi][nid];
      queue.insert({vv, data.shared_from_this()});
      PrintCausalityGraph();
      // if (data_cb_) data_cb_(content_proto.ShortDebugString());
      if (data_cb_) data_cb_(content_proto.user_data());
    }
  }
}

void Node::UpdateReceiveWindow(const Data& data, const NodeID& nid,
                               const ViewID& vi, uint64_t seq) {
  auto& win = recv_window_[nid];

  // Insert the new seq number into the receive window
  BOOST_LOG_TRIVIAL(trace) << "Insert into recv_window[" << nid << "]: vi=("
                           << vi.first << "," << vi.second << "),seq=" << seq;
  win.Insert({vi, seq});

  // Check last data info
  if (seq == 1 || data.getContentType() == kLastDataInfo) {
    // Extract info about last data in the previous round
    const auto& content = data.getContent();
    auto p = DecodeESN(content.value(), content.value_size());
    if (!p.second) {
      BOOST_LOG_TRIVIAL(debug) << "Cannot decode last data info from node idx "
                               << index;
      return;
    }

    auto ldi = p.first;
    BOOST_LOG_TRIVIAL(trace) << "Recv last data info: " << ToString(ldi)
                             << " from node idx " << index;

    auto p2 = win.CheckForMissingData(ldi, vi);
    if (!p2.second) {
      BOOST_LOG_TRIVIAL(debug) << "Failed to check missing data for node idx "
                               << index << ", vid=(" << ldi.vi.first << ","
                               << ldi.vi.second << ")";
      return;
    }

    const auto& missing_seq_intervals = p2.first;
    if (missing_seq_intervals.empty()) {
      BOOST_LOG_TRIVIAL(debug) << "No missing data from node idx " << index
                               << ", vid=(" << ldi.vi.first << ","
                               << ldi.vi.second << ")";
      return;
    }

    auto pfx = ExtractNodePrefix(data.getName());
    for (auto iter = boost::icl::elements_begin(missing_seq_intervals);
         iter != boost::icl::elements_end(missing_seq_intervals); ++iter) {
      SendDataInterest(pfx, nid, ldi.vi, *iter);
    }
  }
}

VersionVector Node::GenerateDataVV() const {
  VersionVector vv(view_info_.Size());
  for (std::size_t i = 0; i != view_info_.Size(); ++i) {
    if (i == idx_) {
      vv[i] = version_vector_[i] - 1;
    } else {
      auto nid = view_info_.GetIDByIndex(i);
      if (!nid.second) {
        throw Error("Cannot get node id for index " + std::to_string(i));
      }
      auto iter = recv_window_.find(nid.first);
      if (iter == recv_window_.end()) {
        throw Error("Cannot get recv window for node id " + nid.first);
      }
      vv[i] = iter->second.LastAckedData(view_id_.first);
    }
  }
  return vv;
}

void Node::PublishHeartbeat() {
  // Schedule next heartbeat event before publishing heartbeat message
  heartbeat_event_ = scheduler_.scheduleEvent(kHeartbeatInterval,
                                              [this] { PublishHeartbeat(); });

  BOOST_LOG_TRIVIAL(trace) << "Send HEARTBEAT";
  PublishData({'H', 'E', 'A', 'R', 'T', 'B', 'E', 'A', 'T', 0}, kHeartbeat);
}

void Node::ProcessHeartbeat(const ViewID& vid, const NodeID& nid) {
  if (vid != view_id_) {
    BOOST_LOG_TRIVIAL(debug) << "Ignore heartbeat for non-current view id ("
                             << vid.first << "," << vid.second << ")";
    return;
  }

  auto index = view_info_.GetIndexByID(nid);
  if (!index.second) {
    BOOST_LOG_TRIVIAL(debug) << "Unkown node id in received heartbeat: " << nid;
    return;
  }

  BOOST_LOG_TRIVIAL(trace) << "Recv HEARTBEAT from node idx " << index.first;
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
        BOOST_LOG_TRIVIAL(trace) << "Found dead node ID: " << p.first;
      }
    }
    if (dead_nodes.size() >= kViewChangeThreshold) {
      view_info_.Remove(dead_nodes);
      ++view_id_.first;
      ResetState();
      PublishViewInfo();
      BOOST_LOG_TRIVIAL(debug) << "Move to new view: id=(" << view_id_.first
                               << "," << view_id_.second
                               << "), vinfo=" << view_info_;
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

void Node::PrintCausalityGraph() const {
  BOOST_LOG_TRIVIAL(trace) << "CausalGraph:";
  for (const auto& p : causality_graph_) {
    BOOST_LOG_TRIVIAL(trace) << " VID=(" << p.first.first << ","
                             << p.first.second << "):";
    for (const auto& pvv_queue : p.second) {
      BOOST_LOG_TRIVIAL(trace) << "  NID=" << pvv_queue.first << ":";
      BOOST_LOG_TRIVIAL(trace) << "   Q=[";
      for (const auto& pvv : pvv_queue.second) {
        BOOST_LOG_TRIVIAL(trace) << "      " << ToString(pvv.first) << ":"
                                 << pvv.second->getName().toUri();
      }
      BOOST_LOG_TRIVIAL(trace) << "     ]";
    }
  }
}

}  // namespace vsync
}  // namespace ndn
