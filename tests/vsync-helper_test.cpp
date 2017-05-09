/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <boost/test/unit_test.hpp>

#include <ndn-cxx/util/digest.hpp>

#include "vsync-helper.hpp"

BOOST_AUTO_TEST_SUITE(TestVsyncHelper);

using namespace ndn::vsync;

BOOST_AUTO_TEST_CASE(MergeVV) {
  VersionVector v1{1, 0, 5};
  VersionVector v2{2, 4, 1};
  auto r1 = ndn::vsync::Merge(v1, v2);
  BOOST_TEST(r1 == VersionVector({2, 4, 5}), boost::test_tools::per_element());

  VersionVector v3{1, 0};
  auto r2 = ndn::vsync::Merge(v1, v3);
  BOOST_TEST(r2 == VersionVector());
}

BOOST_AUTO_TEST_CASE(VVEncodeDecode) {
  VersionVector v1{1, 5, 2, 4, 3};
  std::string out;
  EncodeVV(v1, out);
  VersionVector v2 = DecodeVV(out.data(), out.size());
  BOOST_TEST(v1 == v2, boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(Names) {
  NodeID nid = "A";
  ViewID vid{1, nid};
  uint64_t seq = 55;

  auto n1 = MakeSyncInterestName(nid, vid, seq);
  auto vid1 = ExtractViewID(n1);
  BOOST_TEST(vid1.first == vid.first);
  BOOST_TEST(vid1.second == vid.second);
  auto s1 = ExtractSequenceNumber(n1);
  BOOST_TEST(s1 == seq);

  auto n2 = MakeDataName("/test", nid, seq);
  auto pfx = ExtractNodePrefix(n2);
  BOOST_TEST(pfx == ndn::Name("/test"));
  auto nid1 = ExtractNodeID(n2);
  BOOST_TEST(nid == nid1);
  auto seq1 = ExtractSequenceNumber(n2);
  BOOST_TEST(seq == seq1);
}

BOOST_AUTO_TEST_SUITE_END();
