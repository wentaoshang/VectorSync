/* -*- Mode:C++; c-file-style:"google"; indent-tabs-mode:nil; -*- */

#include <boost/test/unit_test.hpp>

#include <string>

#include <ndn-cxx/name.hpp>

#include "view-info.hpp"

using ndn::vsync::ViewInfo;

BOOST_AUTO_TEST_SUITE(TestViewInfo);

BOOST_AUTO_TEST_CASE(InitAndSize) {
  ViewInfo vinfo1;
  BOOST_CHECK_EQUAL(vinfo1.Size(), 0U);

  ViewInfo vinfo2(
      {{ndn::Name("/test/A")}, {ndn::Name("/test/B")}, {ndn::Name("/test/C")}});
  BOOST_CHECK_EQUAL(vinfo2.Size(), 3U);

  BOOST_CHECK_THROW(ViewInfo({{ndn::Name()}}), ViewInfo::Error);

  std::string invalid(256, 'A');
  std::string invalid2("/");
  invalid2.append(invalid);
  BOOST_CHECK_THROW(ViewInfo({{ndn::Name(invalid2)}}), ViewInfo::Error);
}

BOOST_AUTO_TEST_CASE(GetInfo) {
  ViewInfo vinfo(
      {{ndn::Name("/test/A")}, {ndn::Name("/test/B")}, {ndn::Name("/test/C")}});

  auto p1 = vinfo.GetIndexByID("/test/A");
  BOOST_CHECK_EQUAL(p1.first, 0U);
  BOOST_CHECK_EQUAL(p1.second, true);

  p1 = vinfo.GetIndexByID("/D");
  BOOST_CHECK_EQUAL(p1.second, false);

  auto p2 = vinfo.GetIDByIndex(2);
  BOOST_CHECK_EQUAL(p2.first, ndn::Name("/test/C"));
  BOOST_CHECK_EQUAL(p2.second, true);

  p2 = vinfo.GetIDByIndex(5);
  BOOST_CHECK_EQUAL(p2.second, false);
}

BOOST_AUTO_TEST_CASE(Order) {
  ViewInfo vinfo({{ndn::Name("/test/B")},
                  {ndn::Name("/test/A")},
                  {ndn::Name("/test/C")},
                  {ndn::Name("/test/E")},
                  {ndn::Name("/test/D")}});

  auto p0 = vinfo.GetIDByIndex(0);
  BOOST_CHECK_EQUAL(p0.first, ndn::Name("/test/A"));
  BOOST_CHECK_EQUAL(p0.second, true);

  auto p1 = vinfo.GetIDByIndex(1);
  BOOST_CHECK_EQUAL(p1.first, ndn::Name("/test/B"));
  BOOST_CHECK_EQUAL(p1.second, true);

  auto p2 = vinfo.GetIDByIndex(2);
  BOOST_CHECK_EQUAL(p2.first, ndn::Name("/test/C"));
  BOOST_CHECK_EQUAL(p2.second, true);

  auto p3 = vinfo.GetIDByIndex(3);
  BOOST_CHECK_EQUAL(p3.first, ndn::Name("/test/D"));
  BOOST_CHECK_EQUAL(p3.second, true);

  auto p4 = vinfo.GetIDByIndex(4);
  BOOST_CHECK_EQUAL(p4.first, ndn::Name("/test/E"));
  BOOST_CHECK_EQUAL(p4.second, true);

  auto p5 = vinfo.GetIDByIndex(5);
  BOOST_CHECK_EQUAL(p5.second, false);
}

BOOST_AUTO_TEST_CASE(EncodeDecode) {
  ViewInfo original(
      {{ndn::Name("/test/A")}, {ndn::Name("/test/B")}, {ndn::Name("/test/C")}});
  std::string out;
  original.Encode(out);

  ViewInfo vinfo;
  BOOST_TEST(vinfo.Decode(out.data(), out.size()));
  BOOST_TEST(vinfo == original);
}

BOOST_AUTO_TEST_CASE(Merge) {
  ViewInfo vinfo(
      {{ndn::Name("/test/A")}, {ndn::Name("/test/B")}, {ndn::Name("/test/C")}});
  ViewInfo vinfo1({{ndn::Name("/test/E")}, {ndn::Name("/test/D")}});
  ViewInfo expected({{ndn::Name("/test/A")},
                     {ndn::Name("/test/B")},
                     {ndn::Name("/test/C")},
                     {ndn::Name("/test/D")},
                     {ndn::Name("/test/E")}});
  BOOST_CHECK_EQUAL(vinfo.Merge(vinfo1), true);
  BOOST_TEST(expected == vinfo);
  BOOST_CHECK_EQUAL(vinfo.Merge(vinfo1), false);
  BOOST_TEST(expected == vinfo);
}

BOOST_AUTO_TEST_CASE(Remove) {
  ViewInfo vinfo({{ndn::Name("/test/A")},
                  {ndn::Name("/test/B")},
                  {ndn::Name("/test/C")},
                  {ndn::Name("/test/E")},
                  {ndn::Name("/test/D")}});

  std::unordered_set<ndn::Name> to_be_removed{"/test/B", "/test/D"};
  ViewInfo expected(
      {{ndn::Name("/test/A")}, {ndn::Name("/test/C")}, {ndn::Name("/test/E")}});
  vinfo.Remove(to_be_removed);
  BOOST_TEST(expected == vinfo);
  vinfo.Remove({ndn::Name("/F")});
  BOOST_TEST(expected == vinfo);
}

BOOST_AUTO_TEST_SUITE_END();
