"""
This module contains the job for counting the average and total amounts of bytes of traffic for each ip adress based on
some log file.
"""

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol, PickleProtocol
import re
import user_agents
from operator import attrgetter


class ValueFormat:
    def __init__(self, byte_count, number):
        self.byte_count = byte_count
        self.number = number

    def __repr__(self):
        return '({}, {})'.format(self.byte_count, self.number)

    def __eq__(self, other):
        return self.byte_count == other.byte_count and self.number == other.number

    def __add__(self, other):
        if isinstance(other, ValueFormat):
            return ValueFormat(self.byte_count + other.byte_count, self.number + other.number)


class BytesCounterJob(MRJob):
    """
    This job calculates the average and total amounts of bytes of traffic for each ip adress based of come log file.
    """

    REGEXP = re.compile(
        "(?P<ip>ip\S*) (- -) (\[.*\]) (\".*?\") (\S*) (?P<byte_count>\S*) (\".*?\") (?P<user_agent>\".*?\")")

    OUTPUT_PROTOCOL = RawValueProtocol

    INTERNAL_PROTOCOL = PickleProtocol

    def hadoop_output_format(self):
        """
        This method changes the output format of data if the argument --snappy is enabled.
        """
        if self.options.snappy:
            return 'org.apache.hadoop.mapred.SequenceFileOutputFormat'

    def jobconf(self):
        """
        This method configures the job's parameters. Adds some parameters to the usual configuration in case argument
        --snappy is enabled.
        """
        conf = super().jobconf()
        if self.options.snappy:
            enable_compression_options = {'mapred.output.compress': 'true',
                                          'mapred.output.compression.codec': 'org.apache.hadoop.io.compress.SnappyCodec',
                                          'mapred.output.compression.type': 'BLOCK'}
            conf.update(enable_compression_options)
        return conf

    def configure_options(self):
        """
        This method configures the options of the job. It adds the argument --snappy which is responsible for the data
        compression.
        """
        super(MRJob, self).configure_options()
        self.add_passthrough_option('--snappy', action='store_true')

    def mapper(self, _, line):
        """
        This is a mapper function of the job. It parses some given line of the input file using the special regular
        expression to extract ip adress, amount of bytes and the user agent used in this request (It also increments the
        corresponding counter).
        """
        if line:
            if re.fullmatch(self.REGEXP, line):
                line_values = re.match(self.REGEXP, line).groupdict()
                user_agent = user_agents.parse(line_values['user_agent']).browser.family
                self.increment_counter('Browsers', user_agent, 1)
                ip = line_values['ip']
                try:
                    byte_count = int(line_values['byte_count'])
                except ValueError:
                    byte_count = 0
                yield ip, ValueFormat(byte_count, 1)
            else:
                self.increment_counter('ERRORS', 'ERRORS', 1)

    def combiner(self, key, values):
        """
        This is a combiner function of the job, which unites the results recieved from some mappers.
        """
        yield key, sum(values, ValueFormat(0, 0))

    def reducer(self, key, values):
        """
        This is a reducer function of the job, which calculates the average amount of bytes per request and total bytes
        of traffic for each ip address.
        """
        result = sum(values, ValueFormat(0, 0))
        avg = result.byte_count / result.number if result.number != 0 else 0
        yield None, "{},{},{}".format(key, avg, result.byte_count)


if __name__ == '__main__':
    BytesCounterJob.run()
