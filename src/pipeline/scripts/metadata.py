import skvideo.io
import skvideo.datasets
import simplejson as json
import sys

def extract_metadata(video):
  metadata = skvideo.io.ffprobe(video)
  print (metadata.keys())
  json_metadata = json.dumps(metadata["video"], indent=4)
  return json_metadata

if len(sys.argv) >= 2:
 video = sys.argv[1]
else:
 print ("Video Input is missing.")
 sys.exit()

print (extract_metadata(video))
