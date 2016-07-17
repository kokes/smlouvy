import xmltodict
import pandas as pd
import numpy as np
import requests
import os
from glob import glob

vstupy = 'vstupy'

fns = glob(os.path.join(vstupy, 'xml', 'dump*.xml'))

dt = [['id', 'zverejneni', 'uzavreni', 'subjekt', 'ico', 'bezdph', 'sdph', 'predmet', 'odkaz']]
kli =[['id', 'ico', 'nazev']]

mame = set()
subj = dict() # ico -> jmeno parovani, aby se daly odstranit jmenny duplikaty

for j, fn in enumerate(fns):
    print('Zpracovavam (%d/%d): %s' % (j, len(fns), fn), end='\r')
    with open(fn, encoding='utf8') as f:
        z = xmltodict.parse(f.read())['dump']['zaznam']

    for j, sml in enumerate(z):
        idsml = int(sml['identifikator']['idSmlouvy'])

        if idsml in mame: continue
        mame.add(idsml)

        # objednatel
        icok = sml['smlouva']['subjekt']['ico'] # ICO kupujiciho
        if icok not in subj:
            subj[icok] = sml['smlouva']['subjekt']['nazev']
            
        dt.append([idsml,
                     sml['casZverejneni'],
                     sml['smlouva']['datumUzavreni'],
                     subj[icok], icok,
                     float(sml['smlouva'].get('hodnotaBezDph', np.nan)),
                     float(sml['smlouva'].get('hodnotaVcetneDph', np.nan)),
                     sml['smlouva']['predmet'],
                     sml['odkaz']
                     ])

        # smluvni strana    
        sstr = sml['smlouva']['smluvniStrana']
        sstr = sstr if type(sstr) == list else [sstr]
        assert len(sstr) > 0

        for k, insm in enumerate(sstr):
            if insm.get('ico', np.nan) == icok: continue # nakupujeme od sami sebe?
            
            if 'ico' in insm and insm['ico'] not in subj:
                subj[insm['ico']] = insm['nazev']
                
            kli.append([idsml,
                      insm.get('ico', np.nan), # obcas ICO neni
                      insm['nazev'] if 'ico' not in insm else subj[insm['ico']]])


res = pd.DataFrame(dt[1:], columns=dt[0])
kli = pd.DataFrame(kli[1:], columns=kli[0])

assert len(res.subjekt.unique()) == len(res.ico.unique())

res['cena'] = res[['bezdph', 'sdph']].T.apply(lambda x: np.nan if np.isnan(x).all() else np.nanmean(x))

# bude treba az v interpretaci
# res.uzavreni = pd.to_datetime(res.uzavreni)
# res.zverejneni = pd.to_datetime(res.zverejneni)

res.to_csv(os.path.join(vstupy, 'smlouvy.csv'), index=False)
kli.to_csv(os.path.join(vstupy, 'smluvni_strany.csv'), index=False)
