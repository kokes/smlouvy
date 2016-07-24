"""
    Vezme XML soubory z ARES - zatím jen registru ekonomických subjektů - a
    vytáhne klíčové informace do CSV souboru.
"""

from glob import glob
import os
from collections import OrderedDict
import numpy as np
import pandas as pd
from lxml import etree

rdr = 'vstupy/ares/raw'
mod = 'res'

# extrakce základních informací z RES
dt = {
    'ico': 'D:ZAU/D:ICO',
    'of': 'D:ZAU/D:OF',
    'kpf': 'D:ZAU/D:PF/D:KPF',
    'npf': 'D:ZAU/D:PF/D:NPF',
    'dv': 'D:ZAU/D:DV',
    'dz': 'D:ZAU/D:DZ',
    # TODO: dodat adresy
    'esak': 'D:SU/D:Esa2010',
    'esa': 'D:SU/D:Esa2010t',
    'kpp': 'D:SU/D:KPP',
    'nacek': 'D:Nace/D:NACE', # NACE kód, ten název proměnný je koincidence :)
    'nace': 'D:Nace/D:Nazev_NACE' # jde o PRIMARNI nace, je jich pak hromada dalších
}
por = 'ico, of, kpf, npf, dv, dz, esak, esa, kpp, nacek, nace'.split(', ')
assert sorted(por) == sorted(dt.keys())

dt = OrderedDict((k, dt[k]) for k in por)


ret = OrderedDict((k, []) for k in dt)
fns = glob(os.path.join(rdr, mod, '*.xml'))

for fn in fns:
    et = etree.parse(fn).getroot()
    bd = et.find('.//SOAP-ENV:Body/*', namespaces=et.nsmap)

    els = bd.findall('.//D:Vypis_RES', namespaces=bd.nsmap)
    for el in els:
        for nm, pth in dt.items():
            d = el.find('./%s' % pth, namespaces=el.nsmap)
            ret[nm].append(d.text if d is not None else np.nan)


df = pd.DataFrame(ret)
df.sort_values(by='ico').to_csv(os.path.join(rdr, '..', 'res.csv'), encoding='utf8', index=False)

