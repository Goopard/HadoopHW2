"""
This is a unittest test case for the mapper function of the job BytesCounterJob.
"""

import unittest
from bytes_counter_job import BytesCounterJob


class MapperTests(unittest.TestCase):
    def test_good_request(self):
        """
        This test checks if the mapper works correctly for the line with all required parts correct.
        """
        test_job = BytesCounterJob()
        line = 'ip1 - - [24/Apr/2011:08:02:28 -0400] "GET /~strabal/grease/photo2/910-11.jpg HTTP/1.1" 200 53694 "-" ' \
               '"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)"'
        self.assertEqual(next(test_job.mapper('test_mod', line)), ('ip1', (53694, 1)))

    def test_bad_request(self):
        """
        This text checks if the mapper works correctly for the line with bytes part of the request incorrect.
        """
        test_job = BytesCounterJob()
        line = 'ip1 - - [24/Apr/2011:08:02:28 -0400] "GET /~strabal/grease/photo2/910-11.jpg HTTP/1.1" 200 - "-" ' \
               '"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)"'
        self.assertEqual(next(test_job.mapper('test_mod', line)), ('ip1', (0, 1)))


if __name__ == '__main__':
    unittest.main()
