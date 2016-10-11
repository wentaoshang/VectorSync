/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */

#include <boost/log/trivial.hpp>

#include "node.hpp"
#include "vsync-helper.hpp"

namespace ndn {
namespace vsync {

Node::Node(Face& face, KeyChain& key_chain, const NodeID& nid, const Name& prefix,
           Node::DataCb on_data)
  : face_(face)
  , key_chain_(key_chain)
  , id_(name::Component(nid).toUri())
  , prefix_(prefix)
  , view_id_({0, nid})
  , view_info_({{nid, prefix}})
  , last_data_info_({{0, "-"}, 0, 0})
  , data_cb_(std::move(on_data)) {
  version_vector_.resize(1);
  recv_window_.resize(1);

  face_.setInterestFilter(VsyncPrefix, std::bind(&Node::OnSyncInterest, this, _2),
                          [this](const Name&, const std::string& reason) {
                            throw "Failed to register vsync prefix: " + reason;
                          });
  Name p(prefix);
  p.append(id_);
  face_.setInterestFilter(p, std::bind(&Node::OnDataInterest, this, _2),
                          [this](const Name&, const std::string& reason) {
                            throw "Failed to register data prefix: " + reason;
                          });
}

void Node::LoadView(const ViewID& vid, const ViewInfo& vinfo) {
  auto p = vinfo.GetIndexByID(id_);
  if (!p.second) {
    BOOST_LOG_TRIVIAL(debug) << "View info does not contain node ID " << id_;
    return;
  }

  view_id_ = vid;
  idx_ = p.first;
  view_info_ = vinfo;
  round_number_ = 0;
  version_vector_.clear();
  version_vector_.resize(vinfo.Size());
  recv_window_.clear();
  recv_window_.resize(vinfo.Size());
}

void Node::PublishData(const std::vector<uint8_t>& content) {
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
    key_chain_.sign(*d, signingWithSha256());
    data_store_[n] = d;

    BOOST_LOG_TRIVIAL(trace) << "Publish: " << n.toUri()
                             << " with last data info " << ToString(last_data_info_);

    seq = ++version_vector_[idx_];
    n = MakeDataName(prefix_, id_, view_id_, round_number_, seq);
  }

  std::shared_ptr<Data> data = std::make_shared<Data>(n);
  data->setFreshnessPeriod(time::seconds(3600));
  data->setContent(&content[0], content.size());
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

  BOOST_LOG_TRIVIAL(trace) << "Send: vi = (" << view_id_.first << "," << view_id_.second
                           << "), rn = " << round_number_ << ", vv = " << ToString(version_vector_);

  Interest i(n, time::milliseconds(1000));
  face_.expressInterest(i, [](const Interest&, const Data&) {},
                        [](const Interest&, const lp::Nack&) {},
                        [](const Interest&) {});

  // Generate sync reply to purge pending sync Interest
  std::shared_ptr<Data> data = std::make_shared<Data>(n);
  data->setFreshnessPeriod(time::milliseconds(50));
  data->setContent(&version_vector_[0], version_vector_.size());
  key_chain_.sign(*data, signingWithSha256());
  face_.put(*data);
}

void Node::OnSyncInterest(const Interest& interest) {
  const auto& n = interest.getName();
  if (n.size() != VsyncPrefix.size() + 4) {
    BOOST_LOG_TRIVIAL(error) << "Invalid sync interest name: " << n.toUri();
    return;
  }

  auto vi = ExtractViewID(n);
  auto rn = ExtractRoundNumber(n);
  auto vv = ExtractVersionVector(n);

  BOOST_LOG_TRIVIAL(trace) << "Recv: vi = (" << vi.first << "," << vi.second
                           << "), rn = " << rn << ", vv = " << ToString(vv);

  // Detect invalid sync Interest
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

  // Process round number
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

  BOOST_LOG_TRIVIAL(trace) << "Updt: vi = (" << view_id_.first << "," << view_id_.second
                           << "), rn = " << round_number_ << ", vv = " << ToString(version_vector_);

  for (size_t i = 0; i < vv.size(); ++i) {
    if (i == idx_) continue;

    auto nid = view_info_.GetIDByIndex(i);
    if (!nid.second) continue;

    auto pfx = view_info_.GetPrefixByIndex(i);
    if (!pfx.second) continue;

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

  auto sp = data.shared_from_this();
  data_store_[n] = sp;
  if (data_cb_)
    data_cb_(sp);

  UpdateReceiveWindow(data);
}

void Node::UpdateReceiveWindow(const Data& data) {
  const auto& n = data.getName();
  auto nid = ExtractNodeID(n);

  auto index = view_info_.GetIndexByID(nid);
  if (!index.second) {
    BOOST_LOG_TRIVIAL(debug) << "Unkown node ID in received data: "
                             << nid;
    return;
  }

  auto vi = ExtractViewID(n);
  auto rn = ExtractRoundNumber(n);
  auto seq = ExtractSequenceNumber(n);

  if (seq == 1) {
    // Extract info about last data in the previous round
    auto content = data.getContent();
    auto p = DecodeLastDataInfo(content.value(), content.value_size());
    if (!p.second) {
      BOOST_LOG_TRIVIAL(debug) << "Cannot decode last data info";
    } else {
      BOOST_LOG_TRIVIAL(trace) << "Decode last data info: " << ToString(p.first);
    }
  }

  // Add the new seq number into the receive window
  auto& win = recv_window_[index.first];
  ESN esn{vi, rn, seq};
  BOOST_LOG_TRIVIAL(trace) << "Insert into recv_window_[" << index.first << "]: "
                           << ToString(esn);
  win.insert(esn);
  // Try to slide the window forward
  auto iter = win.begin();
  auto inext = std::next(iter);
  while (inext != win.end()) {
    if (!IsNext(*iter, *inext)) break;
    iter = inext;
    inext = std::next(iter);
  }
  BOOST_LOG_TRIVIAL(trace) << "Slide recv_window_[" << index.first << "] from "
                           << ToString(*win.begin()) << " to " << ToString(*iter);
  win.erase(win.begin(), iter);
}

} // namespace vsync
} // namespace ndn
