from sprocket.config import settings
from sprocket.util.misc import read_pem


def get_default_event():
    return {
        "mode": 1
        , "port": settings['tracker_port']
        , "addr": None  # tracker will fill this in for us
        , "nonblock": 0
        # , 'cacert': libmu.util.read_pem(settings['cacert_file']) if 'cacert_file' in settings else None
        , 'srvcrt': read_pem(settings['srvcrt_file']) if 'srvcrt_file' in settings else None
        , 'srvkey': read_pem(settings['srvkey_file']) if 'srvkey_file' in settings else None
        , 'lambda_function': settings['default_lambda_function']
    }
