/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */

#include <boost/test/unit_test.hpp>

#include "vsync-helper.hpp"

BOOST_AUTO_TEST_SUITE(TestVsyncHelper);

using namespace ndn::vsync;

BOOST_AUTO_TEST_CASE(Merge) {
  VersionVector v1{1, 0, 5};
  VersionVector v2{2, 4, 1};
  auto r1 = ndn::vsync::Merge(v1, v2);
  BOOST_TEST(r1 == VersionVector({2, 4, 5}), boost::test_tools::per_element());

  VersionVector v3{1, 0};
  auto r2 = ndn::vsync::Merge(v1, v3);
  BOOST_TEST(r2 == VersionVector());
}

BOOST_AUTO_TEST_CASE(Names) {
  ViewID vid{1, "A"};
  uint64_t rn = 6;
  VersionVector vv{1, 2, 1, 2, 4};
  auto n1 = MakeVsyncInterestName(vid, rn, vv);
  auto vid1 = ExtractViewID(n1);
  BOOST_TEST(vid1.first == vid.first);
  BOOST_TEST(vid1.second == vid.second);
  auto rn1 = ExtractRoundNumber(n1);
  BOOST_TEST(rn1 == rn);
  auto vv1 = ExtractVersionVector(n1);
  BOOST_TEST(vv1 == vv, boost::test_tools::per_element());

  uint8_t seq = 55;
  NodeID nid = "A";
  auto n2 = MakeDataName("/", nid, vid, rn, seq);
  auto nid1 = ExtractNodeID(n2);
  BOOST_TEST(nid == nid1);
  auto seq1 = ExtractSequenceNumber(n2);
  BOOST_TEST(seq == seq1);
}

BOOST_AUTO_TEST_CASE(EncodeDecode) {
  ESN esn = {{0x12345678, "ABC"}, 0x1234, 0x12};
  std::string buf;
  EncodeESN(esn, buf);

  auto p1 = DecodeESN(buf.data(), buf.size());
  BOOST_TEST(p1.second == true);
  BOOST_TEST(p1.first.vi.first == esn.vi.first);
  BOOST_TEST(p1.first.vi.second == esn.vi.second);
  BOOST_TEST(p1.first.rn == esn.rn);
  BOOST_TEST(p1.first.seq == esn.seq);
}

BOOST_AUTO_TEST_SUITE_END();
