"""
This is a unittest test case for the reducer function of the job BytesCounterJob.
"""

import unittest
from bytes_counter_job import BytesCounterJob, ValueFormat


class ReducerTests(unittest.TestCase):
    def test_only_single_requests(self):
        """
        This test checks if the reducer works correctly for some simple input data of 2 single requests.
        """
        key = 'ip1'
        values = [ValueFormat(100, 1), ValueFormat(200, 1)]
        test_job = BytesCounterJob()
        self.assertEqual(next(test_job.reducer(key, values)), (None, "ip1,150.0,300"))

    def test_some_single_and_some_multiple_requests(self):
        """
        This test checks if the reducer works correctly for a bit more complicated input data of a single request and
        3 multiple ones.
        """
        key = 'ip1'
        values = [ValueFormat(500, 5), ValueFormat(600, 6), ValueFormat(100, 1), ValueFormat(300, 8)]
        test_job = BytesCounterJob()
        self.assertEqual(next(test_job.reducer(key, values)), (None, "ip1,75.0,1500"))


if __name__ == '__main__':
    unittest.main()
