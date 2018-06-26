"""
This is a unittest testcase for the job BytesCounterJob.
"""

import unittest

from io import BytesIO

from bytes_counter_job import BytesCounterJob


class JobTest(unittest.TestCase):
    def setUp(self):
        with open('test_inputs\\test_five_correct_lines', 'rb') as file:
            self.first_test_stdin = BytesIO(file.read())
        with open('test_inputs\\test_three_correct_lines_and_two_broken', 'rb') as file:
            self.second_test_stdin = BytesIO(file.read())

    def test_five_correct_lines(self):
        """
        This test checks if the job works correctly for five correct lines.
        """
        test_job = BytesCounterJob()
        test_job.sandbox(stdin=self.first_test_stdin)
        result = []
        with test_job.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                key, value = test_job.parse_output_line(line)
                result.append(value)
        self.assertEqual(result, ['ip1,21062.666666666668,63188\n', 'ip67,321.0,321\n', 'ip68,13600.0,13600\n'])

    def test_three_correct_lines_and_two_broken(self):
        """
        This test checks if the job works correctly for 3 correct lines and 2 lines that are somehow broken: for example,
        they can miss some required data.
        """
        test_job = BytesCounterJob()
        test_job.sandbox(stdin=self.second_test_stdin)
        result = []
        with test_job.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                key, value = test_job.parse_output_line(line)
                result.append(value)
        self.assertEqual(result, ['ip25,12550.0,12550\n', 'ip60,12550.0,12550\n', 'ip81,286.0,286\n'])

    def test_five_correct_lines_counter(self):
        """
        This test checks if the job's counters works correctly for the input of 5 correct lines.
        """
        test_job = BytesCounterJob()
        test_job.sandbox(stdin=self.first_test_stdin)
        with test_job.make_runner() as runner:
            runner.run()
            result = runner.counters()
        self.assertEqual(result, [{'Browsers': {'YandexImages': 3, 'Baiduspider': 2}}])

    def test_three_correct_lines_and_two_broken_lines_counters(self):
        """
        This test checks if the job's counter works correctly for the input of 5 correct lines.
        """
        test_job = BytesCounterJob()
        test_job.sandbox(stdin=self.second_test_stdin)
        with test_job.make_runner() as runner:
            runner.run()
            result = runner.counters()
        self.assertEqual(result, [{'Browsers': {'Baiduspider': 2, 'bingbot': 1}, 'ERRORS': {'ERRORS': 2}}])


if __name__ == '__main__':
    unittest.main()
