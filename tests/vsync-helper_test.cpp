/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */

#include <boost/test/unit_test.hpp>

#include "vsync-helper.hpp"

BOOST_AUTO_TEST_SUITE(TestVsyncHelper);

using namespace ndn::vsync;

BOOST_AUTO_TEST_CASE(Merge) {
  VersionVector v1{1,0,5};
  VersionVector v2{2,4,1};
  auto r1 = ndn::vsync::Merge(v1, v2);
  BOOST_TEST(r1 == VersionVector({2,4,5}),
             boost::test_tools::per_element());

  VersionVector v3{1,0};
  auto r2 = ndn::vsync::Merge(v1, v3);
  BOOST_TEST(r2 == VersionVector());
}

BOOST_AUTO_TEST_CASE(Names) {
  ViewID vid{1, "A"};
  uint64_t rn = 6;
  VersionVector vv{1,2,1,2,4};
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

BOOST_AUTO_TEST_CASE(Encoding) {
  ESN esn = {{0x12345678, "ABC"}, 0x1234, 0x12};
  std::vector<uint8_t> buf;
  EncodeLastDataInfo(esn, buf);
  std::vector<uint8_t> expected{4, 0x12, 0x34, 0x56, 0x78, 3, 'A', 'B', 'C',
      2, 0x12, 0x34, 1, 0x12};
  BOOST_TEST(buf == expected, boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(Decoding) {
  std::vector<uint8_t> buf{4, 0x12, 0x34, 0x56, 0x78, 3, 'A', 'B', 'C',
      2, 0x12, 0x34, 1, 0x12};
  ESN expected = {{0x12345678, "ABC"}, 0x1234, 0x12};
  auto p1 = DecodeLastDataInfo(buf.data(), buf.size());
  BOOST_TEST(p1.second == true);
  BOOST_TEST(p1.first.vi.first == expected.vi.first);
  BOOST_TEST(p1.first.vi.second == expected.vi.second);
  BOOST_TEST(p1.first.rn == expected.rn);
  BOOST_TEST(p1.first.seq == expected.seq);

  std::vector<uint8_t> buf_bad1{4, 0x12, 0x34};
  auto p2 = DecodeLastDataInfo(buf_bad1.data(), buf_bad1.size());
  BOOST_TEST(p2.second == false);

  std::vector<uint8_t> buf_bad2{4, 0x12, 0x34, 0x56, 0x78, 3, 'A', 'B', 'C'};
  p2 = DecodeLastDataInfo(buf_bad2.data(), buf_bad2.size());
  BOOST_TEST(p2.second == false);
}

BOOST_AUTO_TEST_SUITE_END();
