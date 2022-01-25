import time
import multiprocessing
import sys
import time
import xml.etree.ElementTree as ET
import pandas as pd

input_filename = sys.argv[1]
print (input_filename)
NWKS  = int(sys.argv[2]) if len(sys.argv)>2 else 5
QSIZE = int(sys.argv[3]) if len(sys.argv)>3 else 1000

def parseRec(xRec):
    res = {}
    try:
        dRec = ET.fromstring(xRec)
    except Exception as e:
        print(f"ERRORE in parsing XML: {e}")
    else:
        try:
            sub = dRec.find('./sub')
            info = dRec.find('./info')
            #print (dRec, info, sub)
            res['timeOCS'] = info.get('time')
            #res['typeOfCard'] = sub.get('toc')
            #res['accountCategory'] = sub.get('acc')
            deb   = dRec.find("./policy/rate/bsk[@bn='DEBIT']")
            debco = dRec.find("./policy/rate/bsk[@bn='DEBITCO']")
            res['debitOld'] = float(deb.get('old')) if deb is not None else 0.0
            res['debitNew'] = float(deb.get('new')) if deb is not None else 0.0
            res['debitCOOld'] = float(debco.get('old')) if debco is not None else 0.0
            res['debitCONew'] = float(debco.get('new')) if debco is not None else 0.0
            #res['baskets'] = []
            #for basket in sub.findall("./bsk[@bn='DEBIT']"):
            #    basketName = basket.get('bn')
            #    basketValue = basket.get('bv')
            #    res['baskets'].append([basketName, basketValue])
            evt=dRec.find('./evt')
            #res['callDate']      = evt.get('cs')
            #res['callDuration']  = evt.get('cd')
            #res['costOfTheCall'] = evt.get('cost')
            #res['ratingBasket']  = evt.get('rb')
            tt = evt.find(".//CTp/..")
            res['traffic_type'] = tt.tag
            msc = evt.find('.//MSC')
            ct = evt.find('.//CTp')
            res['MSC'] = msc.text if msc is not None else '@None'
            res['call_type'] = ct.text if ct is not None else '@None'
        except Exception as e:
            print(f"ERROR in values extraction: {e}")

    #print(res)
    return res

counter_nrec = 0
counter_M1 = 0
counter_M2 = 0
counter_M3 = 0
counter_M4 = 0
counter_cost = 0.0
def add_to_counter(res):
    global counter_nrec, counter_cost, counter_M1, counter_M2, counter_M3, counter_M4
    counter_nrec += 1
    if res['debitOld'] >= 0.0 and  res['debitNew'] < 0.0:
        counter_M3 += 1
    if res['debitCOOld'] >= 0.0 and  res['debitCONew'] < 0.0:
        counter_M4 += 1
    #counter_cost += float(res['costOfTheCall'])

pool = multiprocessing.Pool(NWKS)
results = []

t0 = time.time()
with open(input_filename, 'r') as f:
    for record in f:
        #print(record)
        if not record.rstrip():
            continue
        r = pool.apply_async(parseRec, [record])
        results.append(r)

        # don't let the queue grow too long
        if len(results) == QSIZE:
            results[0].wait()

        while results and results[0].ready():
            r = results.pop(0)
            add_to_counter(r.get())

    for r in results:
        r.wait()
        add_to_counter(r.get())

t1 = time.time()

print (f"Number of recs: {counter_nrec} Total CostofCalls: {counter_cost:.2f} elab_time:{t1-t0:.4f}")
print (f"Counter M3: {counter_M3}")
print (f"Counter M4: {counter_M4}")

