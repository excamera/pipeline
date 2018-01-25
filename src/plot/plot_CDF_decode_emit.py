import sys
from plot_CDF import plot_CDF

plot_CDF(sys.argv[1], lambda _,r: 'emit' in r['msg'] and r['op']=='send' and r['stage']=='decode', lambda _,r: 'EMIT' in r['msg'] and r['op']=='recv' and r['stage']=='decode')
