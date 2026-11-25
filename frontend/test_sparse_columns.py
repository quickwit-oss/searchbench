#!/usr/bin/env python3
"""sparse_columns.py: sparse rule and the per-column timeout cap.

    python3 -m unittest -v test_sparse_columns   (from frontend/)
"""
import unittest

import sparse_columns as sc


def row(result, **extra):
    return {"system": "x", "dataset": "d", "result": result, **extra}


class CapOf(unittest.TestCase):
    def test_default_and_row_cap(self):
        self.assertEqual(sc.cap_of(row([])), 60)
        self.assertEqual(sc.cap_of(row([], timeout_cap=600)), 600)
        self.assertEqual(sc.cap_of(row([], timeout_cap=None)), 60)


class Coverage(unittest.TestCase):
    def test_60_s_column_unchanged(self):
        r = row([[None] * 3, [1, 0.5, 0.5], [60, 60, 60], [59, 61, 0.5]])
        self.assertEqual(sc.coverage(r), (1, 3))

    def test_cold_only_column_finishes_below_its_cap(self):
        r = row([[None] * 3, [190.7, None, None], [595.9, None, None], [600, None, None]], timeout_cap=600)
        self.assertEqual(sc.coverage(r), (2, 3))

    def test_cold_only_tries_at_the_60_s_default_still_finish_below_60(self):
        self.assertEqual(sc.coverage(row([[59, None, None], [61, None, None]])), (1, 2))

    def test_a_missing_hot_try_does_not_finish_when_others_ran(self):
        self.assertEqual(sc.coverage(row([[1, None, None], [1, 1, 1]])), (1, 2))


class Mark(unittest.TestCase):
    def test_other_cap_is_excluded_with_a_reason(self):
        r = row([[190.7, None, None], [600, None, None]], timeout_cap=600)
        sc.mark([r])
        self.assertEqual(r["sparse"], {"finished": 1, "supported": 2, "reason": "600 s cap; not comparable"})

    def test_mostly_finished_600_s_column_is_still_excluded(self):
        r = row([[1, None, None]] * 9 + [[600, None, None]], timeout_cap=600)
        sc.mark([r])
        self.assertEqual(r["sparse"]["reason"], "600 s cap; not comparable")

    def test_default_cap_rows_untouched(self):
        dense, explicit = row([[1, 0.5, 0.5]]), row([[1, 0.5, 0.5]], timeout_cap=60)
        sc.mark([dense, explicit])
        self.assertNotIn("sparse", dense)
        self.assertNotIn("sparse", explicit)

    def test_sparse_rule_unchanged(self):
        r = row([[1, 0.5, 0.5]] + [[60, 60, 60]] * 3)
        sc.mark([r])
        self.assertEqual(r["sparse"], {"finished": 1, "supported": 4})


if __name__ == "__main__":
    unittest.main()
