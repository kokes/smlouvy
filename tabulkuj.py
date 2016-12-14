from lxml import etree
import pandas as pd
import numpy as np
import os
from glob import glob

def tag(nd):
    return nd.tag.replace('{%s}' % nd.nsmap[None], '')

def node_dict(nd, excl=None):
    if nd.text is not None:
        return nd.text.strip()

    rt = dict()
    
    for j in nd.getchildren():
        tg = tag(j)
        if excl is not None and tg in excl: continue
            
        if tg not in rt:
            rt[tg] = node_dict(j)
            continue
        
        if type(rt[tg]) == list:
            rt[tg].append(node_dict(j))
        else:
            rt[tg] = [rt[tg]] + [node_dict(j)]
            
    return rt

## =============================================

vstupy = 'vstupy'

dt = [['idsml', 'idver', 'zverejneni', 'uzavreni', 'subjekt', 'ico', 'utvar', 'cena_bezdph',\
'cena_sdph', 'cena_cizi', 'cena_cizi_mena', 'predmet', 'navazny']]
kli =[['idsml', 'idver', 'ico', 'subjekt']]
subj = dict() # ico -> jmeno parovani, aby se daly odstranit jmenny duplikaty

fns = glob(os.path.join(vstupy, 'xml', 'dump*.xml'))

for j, fn in enumerate(fns):
    print('Zpracovavam (%d/%d): %s' % (j+1, len(fns), fn), end='\r')
    et = etree.parse(fn).getroot()

    for z in et.findall('.//{%s}zaznam' % et.nsmap[None]):
        #vl = node_dict(z, excl = set(['prilohy'])) # nahrada za xmltodict
        vl = node_dict(z) # nahrada za xmltodict

        if vl['platnyZaznam'] != '1': continue # verzování, neberem neplatný záznamy

        idsml = int(vl['identifikator']['idSmlouvy'])
        idver = int(vl['identifikator']['idVerze'])

        # objednatel
        icok = np.nan # ICO kupujiciho
        if 'ico' in vl['smlouva']['subjekt']:
          icok = vl['smlouva']['subjekt']['ico'].rjust(8, '0')
        
        if icok not in subj:
            subj[icok] = vl['smlouva']['subjekt']['nazev']

        dt.append([idsml, idver,
                     vl['casZverejneni'],
                     vl['smlouva']['datumUzavreni'],
                     subj[icok], icok,
                     vl['smlouva']['subjekt'].get('utvar', ''),
                     float(vl['smlouva'].get('hodnotaBezDph', np.nan)),
                     float(vl['smlouva'].get('hodnotaVcetneDph', np.nan)),
                     float(vl['smlouva'].get('ciziMena', {}).get('hodnota', np.nan)),
                     vl['smlouva'].get('ciziMena', {}).get('mena', np.nan),
                     vl['smlouva']['predmet'],
                     vl['smlouva'].get('navazanyZaznam', '')
                     ])

        # smluvni strana    
        sstr = vl['smlouva']['smluvniStrana']
        sstr = sstr if type(sstr) == list else [sstr]
        assert len(sstr) > 0

        for k, insm in enumerate(sstr):
            # obcas ICO neni
            icos = np.nan
            if 'ico' in insm:
                #icos = ''.join(insm['ico'].split())
                icos = insm['ico'].replace(' ', '')
                icos = icos.rjust(8, '0')
            
            if icos == icok: continue # nakupci mezi smluvnima stranama

            if 'ico' in insm and icos not in subj:
                subj[icos] = insm['nazev']

            kli.append([idsml, idver,
                      icos, 
                      insm['nazev'] if 'ico' not in insm else subj[icos]])

res = pd.DataFrame(dt[1:], columns=dt[0])
kli = pd.DataFrame(kli[1:], columns=kli[0])

# assert len(res.subjekt.unique()) == len(res.ico.unique()) # failne, az bude vic Lhot

# budem delat asi az v interpretaci
# res['cena'] = res[['cena_bezdph', 'cena_sdph']].T.apply(lambda x: np.nan if np.isnan(x).all() else np.nanmean(x))

# bude treba az v interpretaci
# res.uzavreni = pd.to_datetime(res.uzavreni)
# res.zverejneni = pd.to_datetime(res.zverejneni)

res.to_csv(os.path.join(vstupy, 'smlouvy.csv'), index=False, encoding='utf8')
# obcas se opakuji smluvni strany, tak je vyfiltrujem
kli.drop_duplicates().to_csv(os.path.join(vstupy, 'smluvni_strany.csv'), index=False, encoding='utf8')
