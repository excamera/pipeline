from collections import OrderedDict

__all__ = ['plot_stack']


def preprocess(lines, cmd_of_interest="", send_only=False):
    assert lines[0].split(',')[1].strip() == 'starting pipeline'
    assert lines[-1].split(',')[1].strip() == 'pipeline finished'

    start_ts = float(lines[0].split(',')[0])
    data = OrderedDict()
    for i in xrange(1, len(lines)-1):  # first, last lines excluded
        fields = lines[i].split(',', 4)
        ts = float(fields[0].strip())
        lineage = fields[1].strip()
        op = fields[2].strip()
        msg = fields[3].strip()
        if lineage == '0' or (send_only and op != 'send'):
            continue
        if lineage not in data:
            data[lineage] = []
        if msg.startswith(cmd_of_interest):
            data[lineage].append((ts-start_ts, op+':'+msg[:10]))

    return data


def precise_time(lines, cmd_of_interest):
    """
        assumption: recv msg is right after the corresponding send msg (for the same lineage)
    """
    assert lines[0].split(',')[1].strip() == 'starting pipeline'
    assert lines[-1].split(',')[1].strip() == 'pipeline finished'

    data = dict()
    current = dict()
    for i in xrange(1, len(lines)-1):  # first, last lines excluded
        fields = lines[i].split(',', 4)
        ts = float(fields[0].strip())
        lineage = fields[1].strip()
        op = fields[2].strip()
        msg = fields[3].strip()
        if lineage == '0':
            continue
        if lineage in current:
            start_record = current.pop(lineage)
            lineage_records = data.get(lineage, [])
            lineage_records.append({'msg': start_record['msg'][:10], 'duration': ts - start_record['ts']})
            data[lineage] = lineage_records
            continue
        if op == 'send' and msg.startswith(cmd_of_interest):
            current[lineage] = {'ts': ts, 'msg': msg}

    return data  # not ordered
