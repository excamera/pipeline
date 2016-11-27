# -*- coding: utf-8 -*-

import sys
from optparse import OptionParser

class CmdLineParser(object):
  def __init__(self):
    parser = OptionParser(usage="usage: %prog [options]",
                          version="%prog 1.0")
    parser.add_option("-i", "--input-bucket",
                      dest="input_bucket",
                      default="sintel-1k-png16",
                      help="Input bucket in S3")
    parser.add_option("-p", "--input-prefix",
                      dest="input_prefix",
                      default="sintel-1k-png16/",
                      help="Input prefix in S3")
    parser.add_option("-o", "--output-bucket",
                      dest="output_bucket",
                      default="sintel-1k-png16",
                      help="Output bucket in S3")
    parser.add_option("-t", "--output-prefix",
                      dest="output_prefix",
                      default="sintel-1k-png16-output/",
                      help="Output prefix in S3")
    parser.add_option("-r", "--operator",
                      dest="operator",
                      default="grayscale",
                      help="Image / Scene / Video Operator")
    parser.add_option("-n", "--concurrent-lambdas",
                      dest="concurrent_lambdas",
                      default="10",
                      help="No of lambdas in parallel")
    parser.add_option("-u", "--user-spec",
                      dest="user_spec",
                      default="user1.json",
                      help="User Spec in json format")
    parser.add_option("-f", "--file",
                      dest="filename",
                      default="job1.json",
                      help="Job Spec in json format (TBC)")
    (self.options, self.args) = parser.parse_args()

  def get_options_and_args(self):
    return (self.options, self.args)
