from collections import OrderedDict

__all__ = ['plot_stack']


def preprocess(lines, cmd_of_interest):
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
        if lineage == '0' or op != 'send':
            continue
        if lineage not in data:
            data[lineage] = []
        if msg.startswith(cmd_of_interest):
        #if True:
            data[lineage].append((ts-start_ts, msg[:10]))

    return data
