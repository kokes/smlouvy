"""
    Na základě dat ze zpracovaných tabulek (smlouvy.csv, smluvni_strany.csv)
    stahuje data ze systému ARES k jednotlivým firmám. Prozatím pouze z registru
    ekonomických subjektů, ale jde přepnout i na stahování dat z obchodního
    rejstříku.

    POZOR: skript se automaticky přizpůsobí denní době a aktivuje limit vůči
           API MFČR. Když ale skript pustíte vícekrát, případně hodně načítáte
           ARES data přes ostatní programy (nebo web), může dojít k překročení
           limitu a blokaci.
"""

import requests
import pandas as pd
from lxml import etree
from glob import glob
import math
import os

# API limit
import pytz
from datetime import datetime

rdr = 'vstupy/ares/raw' # raw slozka 
mod = 'res' # TODO: parametrizuj
appurl = 'http://wwwinfo.mfcr.cz/cgi-bin/ares/xar.cgi'

try:
    os.makedirs(os.path.join(rdr, mod))
except:
    pass

def platne_ico(ico: str) -> bool:
    ico = str(ico).rjust(8, '0')
    if len(ico) > 8: return False
    
    sm = sum([int(el) * (8-j) for j, el in enumerate(list(ico[:-1]))])
    md = sm % 11
    df = 11 - md
    if md in [0,1]: df = 1-md

    return int(ico[-1]) == df

def gen_ares_req(ica: list, mod='res') -> str:
    
    if mod not in ['res', 'or']: raise ValueError('Mód může být jen RES nebo OR')
    if len(ica) > 100: raise ValueError('Můžeš poptávat jen 100 záznamů naráz')
    for j in ica:
        if type(j) == str and len(j) == 8 and j.isdigit(): continue
        raise ValueError('Neplatný IČO: %s' % j)
    
    email = 'vase_funkcni@e_mailova.adresa'
    
    pre = {
        'res': """<are:Ares_dotazy xmlns:are="http://wwwinfo.mfcr.cz/ares/xml_doc/schemas/ares/ares_request_orrg/v_1.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://wwwinfo.mfcr.cz/ares/xml_doc/schemas/ares/ares_request_orrg/v_1.0.0 http://wwwinfo.mfcr.cz/ares/xml_doc/schemas/ares/ares_request_orrg/v_1.0.0/ares_request_orrg.xsd" dotaz_datum_cas="2011-06-16T10:01:00" dotaz_pocet="100" dotaz_typ="Vypis_RES" vystup_format="XML" validation_XSLT="http://wwwinfo.mfcr.cz/ares/xml_doc/schemas/ares/ares_answer/v_1.0.0/ares_answer.xsl" user_mail="%s" answerNamespaceRequired="http://wwwinfo.mfcr.cz/ares/xml_doc/schemas/ares/ares_answer_res/v_1.0.3" Id="Ares_dotaz">""" % email,
        'or': """<are:Ares_dotazy xmlns:are="http://wwwinfo.mfcr.cz/ares/xml_doc/schemas/ares/ares_request_or/v_1.0.2" xmlns:dtt="http://wwwinfo.mfcr.cz/ares/xml_doc/schemas/ares/ares_datatypes/v_1.0.2" xmlns:udt="http://wwwinfo.mfcr.cz/ares/xml_doc/schemas/uvis_datatypes/v_1.0.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://wwwinfo.mfcr.cz/ares/xml_doc/schemas/ares/ares_request_or/v_1.0.2 http://wwwinfo.mfcr.cz/ares/xml_doc/schemas/ares/ares_request_or/v_1.0.2/ares_request_or_v_1.0.2.xsd" dotaz_datum_cas="2011-06-16T10:05:02" dotaz_pocet="100" dotaz_typ="Vypis_OR" vystup_format="XML" validation_XSLT="http://wwwinfo.mfcr.cz/ares/xml_doc/schemas/ares/ares_answer_or/v_1.0.0/ares_answer_or.xsl" user_mail="%s" answerNamespaceRequired="http://wwwinfo.mfcr.cz/ares/xml_doc/schemas/ares/ares_answer_or/v_1.0.3" Id="Ares_dotaz">""" % email
    }
    
    po = {
        'res': '</are:Ares_dotazy>',
        'or': '</are:Ares_dotazy>'
    }
    
    body = {
        'res': """<Dotaz>
                    <Pomocne_ID>%d</Pomocne_ID>
                    <ICO>%s</ICO>
                </Dotaz>""",
        'or': """<are:Dotaz>
                    <are:Pomocne_ID>%d</are:Pomocne_ID>
                    <are:ICO>%s</are:ICO>
                    <are:Rozsah>1</are:Rozsah>
                    </are:Dotaz>"""
        
    }

    ret = pre[mod]
    for j, ico in enumerate(ica):
        ret += body[mod] % (j, ico)

    return ret + po[mod]


# tyhle ICO potrebujem stahnout
a=pd.read_csv('vstupy/smlouvy.csv', dtype=str)
b=pd.read_csv('vstupy/smluvni_strany.csv', dtype=str)


ica = [str(j) for j in a.ico.unique().tolist() + b.ico.unique().tolist() if len(str(j)) == 8 and j.isdigit()]
ica = list(filter(lambda x: platne_ico(x), ica))
ica = set(ica)

# ale odeberem ty, co uz mame

for fn in glob(os.path.join(rdr, mod, '*.xml')):
    et = etree.parse(fn).getroot()
    bd = et.find('.//SOAP-ENV:Body/*', namespaces=et.nsmap)

    ii = bd.findall('.//D:ICO', namespaces=bd.nsmap)

    for j in ii:
        if j.text not in ica: continue
        ica.remove(j.text)

ica = list(ica)

# API limit
limit = 950 # 1000 pres den, 5000 v noci
tz = pytz.timezone('Europe/Prague')
prg = datetime.now(tz)
if prg.hour > 17 or prg.hour < 8:
    limit = 4950

if len(ica) > limit:
    print('Máme víc jak %d chybějících IČO, stahuju jen část' % limit)
    ica = ica[:limit]

# jde se stahovat

np = 100 # v postu
chunks = math.ceil(len(ica)/np) # pocet souboru

for pos in range(chunks):
    subs = ica[100*pos:100*(pos+1)]
    rt = gen_ares_req(subs, mod)
    r = requests.post(appurl, rt)
    assert r.ok
    
    # najdi vhodnej nazev souboru
    for j in range(10**6):
        tfn = os.path.join(rdr, mod, '%d.xml' %j)
        if not os.path.isfile(tfn): break

    # print(tfn)
    with open(tfn, 'wb') as ff:
        ff.write(r.content)

