/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <iostream>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "vsync.hpp"

namespace ndn {
namespace vsync {
namespace examples {

class KeyValueStore {
 public:
  struct Value {
    std::string val;
    VersionVector vv;
  };

  struct ValueCompare {
    bool operator()(const Value& l, const Value& r) const {
      auto comp = VVCompare();
      return comp(l.vv, r.vv);
    }
  };

  using ValueQueue = std::set<Value, ValueCompare>;

  KeyValueStore(const NodeID& nid, const Name& prefix, uint32_t seed)
      : face_(io_service_),
        scheduler_(io_service_),
        node_(face_, scheduler_, key_chain_, nid, prefix, seed),
        rengine_(seed),
        rdist_(2000, 5000) {
    node_.ConnectDataSignal(
        std::bind(&KeyValueStore::OnData, this, _1, _2, _3, _4));
  }

  void Start() { face_.processEvents(); }

  void PrepareTestRun() {
    scheduler_.scheduleEvent(time::milliseconds(rdist_(rengine_)),
                             [this] { Put("/conf/id", node_.GetNodeID()); });

    scheduler_.scheduleEvent(time::seconds(8), [this] {
      std::vector<Value> values;
      GetLatest("/conf/id", values);
      std::cout << "/conf/id: [" << std::endl;
      for (const auto& v : values) {
        std::cout << "  " << v.val << ",vv=" << v.vv << std::endl;
      }
      std::cout << "]" << std::endl;
    });
  }

  void Put(const std::string& key, const std::string& value) {
    VersionVector vv;
    std::tie(std::ignore, std::ignore, vv) =
        node_.PublishData(key + ':' + value);
    kvs_[key].insert({value, vv});
    std::cout << "Put " << key << ':' << value << ",vv=" << vv << std::endl;
  }

  void GetLatest(const std::string& key, std::vector<Value>& values) const {
    auto iter = kvs_.find(key);
    if (iter == kvs_.end()) return;
    const auto& vals = iter->second;
    if (vals.empty()) return;
    auto range = vals.equal_range(*vals.rbegin());
    for (auto i = range.first; i != range.second; ++i) {
      values.push_back(*i);
    }
  }

  void PrintStore() const {
    std::cout << "PrintStore[nid=" << node_.GetNodeID() << "]:" << std::endl;
    std::cout << '{' << std::endl;
    for (const auto& p : kvs_) {
      std::cout << "  " << p.first << ":[" << std::endl;
      for (const auto& v : p.second) {
        std::cout << "    " << v.val << ',' << v.vv << std::endl;
      }
    }
    std::cout << '}' << std::endl;
  }

 private:
  void OnData(std::shared_ptr<const Data> data, const std::string& content,
              const ViewID& vi, const VersionVector& vv) {
    std::cout << "Upcall OnData: name=" << data->getName() << std::endl;

    auto sep = content.find_first_of(':');
    if (sep == std::string::npos) {
      std::cout << "Cannot parse key:value pair: " << content << std::endl;
      return;
    }

    std::string key = content.substr(0, sep);
    std::string value = content.substr(sep + 1);
    kvs_[key].insert({value, vv});
    std::cout << "Recv " << key << ':' << value << ",vi=" << vi << ",vv=" << vv
              << std::endl;
  }

  boost::asio::io_service io_service_;
  Face face_;
  Scheduler scheduler_;
  KeyChain key_chain_;
  Node node_;

  std::unordered_map<std::string, ValueQueue> kvs_;

  std::mt19937 rengine_;
  std::uniform_int_distribution<> rdist_;
};

int main(int argc, char* argv[]) {
  // Create a simple view with three nodes
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " [node_id] [node_prefix]"
              << std::endl;
    return -1;
  }

  NodeID nid = argv[1];
  Name prefix(argv[2]);

  std::random_device rdevice;

  KeyValueStore kv_store(nid, prefix, static_cast<uint32_t>(rdevice()));
  kv_store.PrepareTestRun();
  kv_store.Start();
  return 0;
}

}  // namespace examples
}  // namespace vsync
}  // namespace ndn

int main(int argc, char* argv[]) {
  return ndn::vsync::examples::main(argc, argv);
}
