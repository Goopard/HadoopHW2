"""
This module contains the job for counting the average and total amounts of bytes of traffic for each ip adress based on
some log file.
"""

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
import re
import user_agents


class BytesCounterJob(MRJob):
    """
    This job calculates the average and total amounts of bytes of traffic for each ip adress based of come log file.
    """

    OUTPUT_PROTOCOL = RawValueProtocol

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
            regexp = "(ip\S*) (- -) (\[.*\]) (\".*?\") (\S*) (\S*) (\".*?\") (\".*?\")"
            if re.fullmatch(regexp, line):
                groups = re.split(regexp, line)
                user_agent = user_agents.parse(groups[8]).browser.family
                if _ != 'test_mod':
                    self.increment_counter('Browsers', user_agent, 1)
                ip = groups[1]
                try:
                    byte_count = int(groups[6])
                except ValueError:
                    byte_count = 0
                yield ip, (byte_count, 1)
            else:
                if _ != 'test_mod':
                    self.increment_counter('ERRORS', 'ERRORS', 1)

    def combiner(self, key, values):
        """
        This is a combiner function of the job, which unites the results recieved from some mappers.
        """
        values = list(values)
        yield key, (sum([value[0] for value in values]), len(values))

    def reducer(self, key, values):
        """
        This is a reducer function of the job, which calculates the average amount of bytes per request and total bytes
        of traffic for each ip address.
        """
        values = list(values)
        total = sum([value[0] for value in values])
        num = sum([value[1] for value in values])
        avg = total / num if num != 0 else 0
        yield None, "{},{},{}".format(key, avg, total)


if __name__ == '__main__':
    BytesCounterJob.run()
