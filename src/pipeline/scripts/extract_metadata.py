import logging
import subprocess
import boto3
import xmltodict, json
from optparse import OptionParser

SIGNED_URL_EXPIRATION = 300     # The number of seconds that the Signed URL is valid
logger = logging.getLogger('boto3')
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Loop through records provided by S3 Event trigger
    logger.info("Working on bucket-key in S3...")
    # Extract the Key and Bucket names for the asset uploaded to S3
    key = event['key']
    bucket = event['bucket']
    logger.info("Bucket: {} \t Key: {}".format(bucket, key))
    # Generate a signed URL for the uploaded asset
    signed_url = get_signed_url(SIGNED_URL_EXPIRATION, bucket, key)
    logger.info("Signed URL: {}".format(signed_url))
    # Launch MediaInfo
    # Pass the signed URL of the uploaded asset to MediaInfo as an input
    # MediaInfo will extract the technical metadata from the asset
    # The extracted metadata will be outputted in XML format and
    # stored in the variable xml_output
    xml_output = subprocess.check_output(["mediainfo", "--full", "--output=XML", signed_url])
    logger.info("Output: {}".format(xml_output))

    o = xmltodict.parse(xml_output)
    return json.dumps(o)

def get_signed_url(expires_in, bucket, obj):
    """
    Generate a signed URL
    :param expires_in:  URL Expiration time in seconds
    :param bucket:
    :param obj:         S3 Key name
    :return:            Signed URL
    """
    s3_cli = boto3.client("s3")
    presigned_url = s3_cli.generate_presigned_url('get_object',
                                                  Params = {'Bucket': bucket,
                                                            'Key': obj},
                                                  ExpiresIn = expires_in)
    return presigned_url

parser = OptionParser(usage="usage: %prog [options]",
                          version="%prog 1.0")
parser.add_option("-b", "--bucket",
                  dest="bucket",
                  default="excamera-ffmpeg-input",
                  help="Input bucket in S3")
parser.add_option("-k", "--key",
                  dest="key",
                  default="input.mp4",
                  help="Input prefix in S3")

(options, args) = parser.parse_args()
event = {
  'bucket' : options.bucket,
  'key' : options.key
}
json_metadata = lambda_handler(event, {})
print (json_metadata)
