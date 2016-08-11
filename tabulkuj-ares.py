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

for jj, fn in enumerate(fns):
    print('Zpracovavam %s: %d/%d' % (mod, jj+1, len(fns)), end='\r')
    et = etree.parse(fn).getroot()
    bd = et.find('.//SOAP-ENV:Body/*', namespaces=et.nsmap)

    els = bd.findall('.//D:Vypis_RES', namespaces=bd.nsmap)
    for el in els:
        for nm, pth in dt.items():
            d = el.find('./%s' % pth, namespaces=el.nsmap)
            ret[nm].append(d.text if d is not None else np.nan)


df = pd.DataFrame(ret)
df.sort_values(by='ico').to_csv(os.path.join(rdr, '..', 'res.csv'), encoding='utf8', index=False)


### Obchodni rejstrik
## ==================
# extrahuj z xml objektu dany propriety
# a returnuj dict()
def el_dict(el, mp, ns='D'):
    dt = dict.fromkeys(mp.keys())
    
    for nm,ad in mp.items():
        ll = el.find(('./%s:' % ns) + ('/%s:' % ns).join(ad.split('/')), namespaces = el.nsmap)
        dt[nm] = ll.text if ll is not None else np.nan
    return dt


# vlastnosti
allzud = OrderedDict.fromkeys(['aktualizace', 'vypis', 'stav', 'ico', 'zapis'])
for k in allzud: allzud[k] = []

# zakladni udaje
zud = {
    'aktualizace': 'UVOD/ADB',
    'vypis': 'UVOD/DVY',
    'stav': 'ZAU/S/SSU',
    'ico': 'ZAU/ICO',
    'zapis': 'ZAU/DZOR'
}
assert sorted(allzud.keys()) == sorted(list(zud.keys()))

# angazovane osoby
ang = {
    'akcionar': 'AKI/AKR', # akcionari
    'stat_organ': 'SO/CSO/C', # statutarni organy
    'prokura': 'PRO/PRA', # prokuristi
    'spolecnik': 'SSV/SS', # spolecnici s vkladem
    'doz_rada': 'DR/CDR/C' # dozorci rada (TODO: od/dod chybi, je na rodicovi)
}

# high level info o osobe
hli = {
    'kan': 'KAN',
    'funkce': 'F',
    'clen_zac': 'CLE/DZA',
    'clen_kon': 'CLE/DK',
    'funk_zac': 'VF/DZA',
    'funk_kon': 'VF/DK'
}
allfo = OrderedDict.fromkeys(['pid', 'od', 'do', 'kan', 'jman', 'funkce',                              'tp', 'j', 'p', 'tz', 'dn',                             'clen_zac', 'clen_kon', 'funk_zac', 'funk_kon'])
allpo = OrderedDict.fromkeys(['pid', 'od', 'do', 'kan', 'jman', 'funkce',                              'ico', 'izo', 'of', 'npf', 'ns',                              'clen_zac', 'clen_kon', 'funk_zac', 'funk_kon'])

for k in allfo: allfo[k] = []
for k in allpo: allpo[k] = []  


# Parsuj
mod = 'or'
fns = glob(os.path.join(rdr, mod, '*.xml'))

for jj, fn in enumerate(fns):
    print('Zpracovavam %s: %d/%d' % (mod, jj+1, len(fns)), end='\r')
    et = etree.parse(fn).getroot()

    bd = et.find('.//SOAP-ENV:Body/*', namespaces=et.nsmap)

    els = bd.findall('.//are:Odpoved/D:Vypis_OR', namespaces=bd.nsmap) # vypis vylouci neexistujici subjekty

    for el in els:
        # zakladni udaje
        udaje = el_dict(el, zud)

        for j,k in udaje.items():
            allzud[j].append(k)

        for nm, ad in ang.items():
            ll = el.findall('./D:' + '/D:'.join(ad.split('/')), namespaces=el.nsmap)
            if len(ll) == 0: continue

            for osb in ll:
                dt = dict()
                dt['pid'] = udaje['ico'] # ke ktery firme tahle osoba patri?
                dt['jman'] = nm # jmeno angazovanosti

                if nm not in ['doz_rada', 'stat_organ']:
                    dt['od'] = osb.attrib.get('dod', np.nan) # platnost od    
                    dt['do'] = osb.attrib.get('ddo', np.nan) # platnost do
                else:
                    # dozorci rada a stat. organy maj platnost o koren vyse
                    pr = osb.getparent()
                    dt['od'] = pr.attrib.get('dod', np.nan) # platnost od
                    dt['do'] = pr.attrib.get('ddo', np.nan) # platnost do

                assert not (type(dt['od']) == float and np.isnan(dt['od'])) # od vzdycky je

                dt.update(el_dict(osb, hli))

                fo = osb.find('./D:FO', osb.nsmap)
                # fyzicka osoba
                if fo is not None:
                    ind = 'TP, J, P, TZ, DN'.split(', ')
                    dt.update(el_dict(fo, {j.lower(): j for j in ind}))
                    
                    # kapitalizuj jmena
                    dt['j'] = dt['j'].title() if type(dt['j']) == str else dt['j']
                    dt['p'] = dt['p'].title() if type(dt['p']) == str else dt['p']

                    for j,k in dt.items(): allfo[j].append(k)

                # pravnicka osoba
                else:
                    po = osb.find('./D:PO', osb.nsmap)
                    assert po is not None # musi byt FO, nebo PO

                    ind = 'ICO, IZO, OF, NPF, SI/NS'.split(', ')
                    dt.update(el_dict(po, {j.lower(): j for j in ind}))

                    for j,k in dt.items(): allpo[j.split('/')[-1]].append(k)


dfzud = pd.DataFrame(allzud)
dfpo = pd.DataFrame(allpo)
dffo = pd.DataFrame(allfo)

dfzud.to_csv('vstupy/ares/or.csv', encoding='utf8', index=False)
dffo.to_csv('vstupy/ares/or_angos_fo.csv', encoding='utf8', index=False)
# dffo.to_excel('vstupy/ares/or_angos_fo.xlsx', encoding='utf8', index=False)
dfpo.to_csv('vstupy/ares/or_angos_po.csv', encoding='utf8', index=False)
