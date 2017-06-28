#!/usr/bin/python


lambda_function_map = {
    'decode': {'name': 'lambda_test_RQMGR4Gb',
               'downloadCmd': [(None, 'run:./ffmpeg -y -ss {starttime} -t {duration} -i "{in_URL}" -f image2 -c:v png -r 24 '
                                    '-start_number {start_number} ##TMPDIR##/%08d-filtered.png'),
                             ('OK:RETVAL(0)', None)],
               'filterLoop': False,
               'filterCmd': [None],
               'uploadCmd': [(None, "set:fromfile:##TMPDIR##/{number}-filtered.{extension}"),
                             ("OK:SET", "set:outkey:{out_dir}/{number}.{extension}"),
                             ("OK:SET", "upload:"),
                             ('OK:UPLOAD(', None)
                             ],
               'outputFmt': 'frames'
    },

    'grayscale': {'name': 'lambda_test_RQMGR4Gb',
                'downloadCmd': [(None, 'set:inkey:{in_dir}/{number}.{extension}'),
                                ('OK:SET', 'set:targfile:##TMPDIR##/{number}.{extension}'),
                                ('OK:SET', 'retrieve:'),
                                ('OK:RETRIEV', None)],
                'filterLoop': False,
                'filterCmd': [(None, 'run:./ffmpeg -framerate 24 -start_number {start_number} -i ##TMPDIR##/%08d.png '
                                     '-vf hue=s=0 -c:a copy -safe 0 -start_number {start_number} ##TMPDIR##/%08d-filtered.png'),
                              ('OK:RETVAL(0)', None)],
                'uploadCmd': [(None, "set:fromfile:##TMPDIR##/{number}-filtered.{extension}"),
                             ("OK:SET", "set:outkey:{out_dir}/{number}.{extension}"),
                             ("OK:SET", "upload:"),
                             ('OK:UPLOAD(', None)
                             ],
                'outputFmt': 'frames'
    },

    'encode': {'name': 'lambda_test_RQMGR4Gb',
                'downloadCmd': [(None, 'set:inkey:{in_dir}/{number}.{extension}'),
                                ('OK:SET', 'set:targfile:##TMPDIR##/{number}.{extension}'),
                                ('OK:SET', 'retrieve:'),
                                ('OK:RETRIEV', None)],
                'filterLoop': False,
                'filterCmd': [(None, 'run:./ffmpeg -framerate 24 -start_number {start_number} -i ##TMPDIR##/%08d.png '
                               '-c:v libx264 -pix_fmt yuv420p ##TMPDIR##/{number}-filtered.mp4'),
                              ('OK:RETVAL(0)', None)],
                'uploadCmd': [(None, "set:fromfile:##TMPDIR##/{number}-filtered.{extension}"),
                             ("OK:SET", "set:outkey:{out_dir}/{number}.{extension}"),
                             ("OK:SET", "upload:"),
                             ('OK:UPLOAD(', None)
                             ],
                'outputFmt': 'range'
              },

    'encode_dash': {'name': 'lambda_test_RQMGR4Gb',
                    'downloadCmd': [(None, 'set:inkey:{in_dir}/{number}.{extension}'),
                                    ('OK:SET', 'set:targfile:##TMPDIR##/{number}.{extension}'),
                                    ('OK:SET', 'retrieve:'),
                                    ('OK:RETRIEV', None)],
                    'filterLoop': False,
                    'filterCmd': [(None, 'run:./ffmpeg -framerate 24 -start_number {start_number} -i ##TMPDIR##/%08d.png '
                                   '-c:v libx264 -pix_fmt yuv420p ##TMPDIR##/{number}.mp4'),
                                  ('OK:RETVAL(0)', 'run:cd ##TMPDIR## && $OLDPWD/MP4Box -dash 1000 -rap -segment-name seg_{number}_ ##TMPDIR##/{number}.mp4#video:id=video ##TMPDIR##/{number}.mp4#audio:id=audio && cd -'),
                                  ('OK:RETVAL(0)', 'run:python amend_m4s.py ##TMPDIR##/seg_{number}_1.m4s {number}'),
                                  ('OK:RETVAL(0)', None)],
                    'uploadCmd': [(None, "set:fromfile:##TMPDIR##/seg_{number}_1.m4s"),
                                  ("OK:SET", "set:outkey:{out_dir}/seg_{number}_1.m4s"),
                                  ("OK:SET", "upload:"),
                                  ('OK:UPLOAD(', "set:fromfile:##TMPDIR##/{number}_dash.mpd"),
                                  ("OK:SET", "set:outkey:{out_dir}/{number}_dash.mpd"),
                                  ("OK:SET", "upload:"),
                                  ('OK:UPLOAD(', "set:fromfile:##TMPDIR##/{number}_dash_init.mp4"),
                                  ("OK:SET", "set:outkey:{out_dir}/{number}_dash_init.mp4"),
                                  ("OK:SET", "upload:"),
                                  ('OK:UPLOAD(', None),
                                 ],
                    'outputFmt': 'range'
                   }

}
