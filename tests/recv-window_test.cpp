/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */

#include <boost/test/unit_test.hpp>

#include <string>

#include "recv-window.hpp"

using ndn::vsync::ReceiveWindow;

BOOST_AUTO_TEST_SUITE(TestReceiveWindow);

BOOST_AUTO_TEST_CASE(Init) {
  ReceiveWindow rw;
  BOOST_CHECK_EQUAL(rw.Insert({{0, "A"}, 1, 1}), false);
  BOOST_CHECK_EQUAL(rw.Insert({{1, ""}, 1, 1}), false);
  BOOST_CHECK_EQUAL(rw.Insert({{1, "A"}, 0, 1}), false);
  BOOST_CHECK_EQUAL(rw.Insert({{1, "A"}, 1, 0}), false);
  BOOST_CHECK_EQUAL(rw.Insert({{1, "A"}, 1, 256}), false);

  BOOST_CHECK_EQUAL(rw.Insert({{1, "A"}, 1, 1}), true);
  BOOST_CHECK_EQUAL(rw.Insert({{1, "A"}, 1, 3}), true);

  BOOST_CHECK_EQUAL(rw.Insert({{2, "B"}, 1, 1}), true);

  BOOST_CHECK_EQUAL(rw.Insert({{2, "B1"}, 1, 2}), false);
}

BOOST_AUTO_TEST_CASE(CheckForMissingData) {
  ReceiveWindow rw;
  rw.Insert({{1, "A"}, 1, 1});
  rw.Insert({{1, "A"}, 1, 3});
  auto intervals = rw.CheckForMissingData({{1, "A"}, 1, 3}, {1, "A"}, 2);
  ReceiveWindow::SeqNumIntervalSet expected1;
  expected1.insert(ReceiveWindow::SeqNumInterval(2));
  BOOST_CHECK_EQUAL(intervals, expected1);

  ReceiveWindow::SeqNumIntervalSet expected2;
  intervals = rw.CheckForMissingData({{1, "A"}, 1, 50}, {1, "A"}, 2);
  BOOST_CHECK_EQUAL(intervals, expected2);
  intervals = rw.CheckForMissingData({{1, "A"}, 1, 3}, {2, "A"}, 2);
  BOOST_CHECK_EQUAL(intervals, expected2);
  intervals = rw.CheckForMissingData({{1, "A"}, 1, 3}, {1, "A"}, 6);
  BOOST_CHECK_EQUAL(intervals, expected2);

  rw.Insert({{1, "A"}, 1, 2});
  intervals = rw.CheckForMissingData({{1, "A"}, 1, 3}, {1, "A"}, 2);
  BOOST_CHECK_EQUAL(intervals, expected2);

  expected2.insert(ReceiveWindow::SeqNumInterval::closed(1, 50));
  intervals = rw.CheckForMissingData({{1, "A"}, 2, 50}, {1, "A"}, 3);
  BOOST_CHECK_EQUAL(intervals, expected2);

  rw.Insert({{1, "A"}, 2, 10});
  rw.Insert({{1, "A"}, 2, 11});
  intervals = rw.CheckForMissingData({{1, "A"}, 2, 50}, {1, "A"}, 3);
  ReceiveWindow::SeqNumIntervalSet expected3;
  expected3.insert(ReceiveWindow::SeqNumInterval::right_open(1, 10));
  expected3.insert(ReceiveWindow::SeqNumInterval::closed(12, 50));
  BOOST_CHECK_EQUAL(intervals, expected3);
}

BOOST_AUTO_TEST_SUITE_END();
