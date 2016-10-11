/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */

#include <functional>
#include <iostream>
#include <random>

#include "vsync.hpp"

namespace ndn {
namespace vsync {
namespace examples {

class SimpleNode {
public:
  SimpleNode(const NodeID& nid, const Name& prefix, const ViewInfo& vinfo)
    : face_(io_service_)
    , scheduler_(io_service_)
    , node_(face_, key_chain_, nid, prefix,
            std::bind(&SimpleNode::OnData, this, _1))
    , rengine_(rdevice_())
    , rdist_(200, 1000) {
    node_.LoadView({1, "A"}, vinfo);
  }

  void Start() {
    scheduler_.scheduleEvent(time::milliseconds(rdist_(rengine_)),
			     [this] { PublishData(); });
    face_.processEvents();
  }

private:
  void OnData(std::shared_ptr<const Data> data) {
    std::cout << "Recv: d.name=" << data->getName().toUri() << std::endl;
  }

  void PublishData() {
    node_.PublishData({1,2,3,4,5,6,7,8,9,10});
    scheduler_.scheduleEvent(time::milliseconds(rdist_(rengine_)),
			     [this] { PublishData(); });
  }

  boost::asio::io_service io_service_;
  Face face_;
  Scheduler scheduler_;
  KeyChain key_chain_;
  Node node_;

  std::random_device rdevice_;
  std::mt19937 rengine_;
  std::uniform_int_distribution<> rdist_;
};

int main(int argc, char* argv[]) {
  // Create a simple view with three nodes
  ViewInfo vinfo({{"A", Name("/test")}, {"B", Name("/test")}, {"C", Name("/test")}});
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " [A|B|C]" << std::endl;
    return -1;
  }

  NodeID nid = argv[1];
  if (nid != "A" && nid != "B" && nid != "C") {
    std::cerr << "Usage: " << argv[0] << " [A|B|C]" << std::endl;
    return -1;
  }

  auto index = vinfo.GetIndexByID(nid);
  assert(index.second);

  auto prefix = vinfo.GetPrefixByIndex(index.first);
  assert(prefix.second);

  SimpleNode node(nid, prefix.first, vinfo);
  node.Start();
  return 0;
}

} // namespace examples
} // namespace vsync
} // namespace ndn

int main(int argc, char* argv[]) {
  return ndn::vsync::examples::main(argc, argv);
}
