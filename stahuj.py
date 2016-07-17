import time
import requests
import os
import xmltodict

vstupy = 'vstupy/xml'
ind = os.path.join(vstupy, 'index.xml')
# if not os.path.isdir(vstupy):
#     os.mkdir(vstupy)
try: os.makedirs(vstupy)
except: pass

r = requests.get('http://data.smlouvy.gov.cz/index.xml')

if not r.ok:
    print('Nestáhlo se, zkus pozdějc')
    quite()

lm = time.strptime(r.headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z')
if os.path.isfile(ind) and time.mktime(lm) < os.path.getmtime(ind):
    print('Uz mame nejnovejsi data')
    quit()

with open(ind, 'wb') as f:
    f.write(r.content)

xin = xmltodict.parse(r.content)
dm = xin['index']['dump']

dumpy = [dm] if type(dm) is not list else dm

for d in dumpy:
    print('Stahuju %s/%s' % (d['mesic'], d['rok']), end='\r')
    tt = time.strptime(d['casGenerovani'][:-3]+'00', '%Y-%m-%dT%H:%M:%S%z')
    tt = time.mktime(tt)
    fn = os.path.split(d['odkaz'])[-1]
    tr = os.path.join(vstupy, fn)
    
    # mame soubor (a dostatecne novej?)
    if os.path.isfile(tr) and os.path.getmtime(tr) > tt: continue
    
    r = requests.get(d['odkaz'])
    if not r.ok:
        print('Nestáhlo %s, zkus znova' % fn)
        quit()
    
    with open(tr, 'wb') as f:
        f.write(r.content)
    
