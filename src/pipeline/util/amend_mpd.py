#!/usr/bin/python

import xml.etree.ElementTree as ET

from pipeline.util.media_probe import get_signed_URI

def amend_mpd(init_mpd, duration, baseURL, num_m4s):
    prefix = "urn:mpeg:dash:schema:mpd:2011"
    ET.register_namespace('', prefix)
    prefix = '{' + prefix + '}'
    root = ET.fromstring(init_mpd)
    
    hours = int(duration)/3600
    minutes = int(duration%3600)/60
    seconds = duration - hours * 3600 - minutes * 60
    formatted_duration = 'PT%dH%dM%.3fS' % (hours, minutes, seconds)

    root.set('mediaPresentationDuration', formatted_duration)

    ET.SubElement(root, prefix+'BaseURL')
    root.find(prefix+'BaseURL').text = get_signed_URI(baseURL).split('?')[0]

    period = root.find(prefix+'Period')
    period.set('duration', formatted_duration)

    signed_init = get_signed_URI(baseURL + '00000001_dash_init.mp4').split('/')[-1]
    adaptset = period.find(prefix+'AdaptationSet')
    adaptset.find(prefix+'SegmentList').find(prefix+'Initialization').set('sourceURL', signed_init)

    for rep in adaptset.findall(prefix+'Representation'):
        seglist = rep.find(prefix+'SegmentList')
        while len(seglist.getchildren()) != 0:
            seglist.remove(seglist.getchildren()[0])
        for i in range(num_m4s):
            signed_chunk = get_signed_URI(baseURL + ('seg_%08d_1.m4s' % (i+1))).split('/')[-1]
            ET.SubElement(seglist, prefix+'SegmentURL', attrib={'media': signed_chunk})

    return ET.tostring(root)