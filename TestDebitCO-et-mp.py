import time
import multiprocessing
import sys
import time
import datetime
import xml.etree.ElementTree as ET
import pandas as pd

aggregationInterval = 5
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
            dti = info.get('time')
            # 20220120125038
            # 01234567890123
            dt = dti[:10] + ("00" + str(int(dti[10:12]) - (int(dti[10:12]) % aggregationInterval)))[-2:]
            dt = dt[0:4] + "-" + dt[4:6] + "-" + dt[6:8] + "T" + dt[8:10] + ":" + dt[10:12] + ":" + "00"
            ut = time.mktime(datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S").timetuple())
            res['timeOCS'] = dti
            res['time'] = ut

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

    print(res)
    return res

M1_data = pd.DataFrame()
M2_data = pd.DataFrame()
M3_data = pd.DataFrame()
M4_data = pd.DataFrame()
M1_list = []
M2_list = []
M3_list = []
M4_list = []
counter_nrec = 0
counter_M1 = {}
counter_M2 = {}
counter_M3 = 0
counter_M4 = 0

def add_to_counter(res):
    global counter_nrec, counter_M1, counter_M2, counter_M3, counter_M4, M1_list, M2_list, M3_list, M4_list

    counter_nrec += 1
    # METRIC M1 - Number of Traffic Events per Call Type
    M1_key = f"{res['time']}#{res['call_type']}"
    if M1_key not in counter_M1:
        counter_M1[M1_key] = {
                "TIME_OCS": res['time'],
                "CALL_TYPE": res['call_type'],
                "COUNT": 0
                }
    counter_M1[M1_key]["COUNT"]+= 1

    # METRIC M2 - Number of Traffic Events per Network Exchange (MSC)
    M2_key = f"{res['time']}#{res['MSC']}"
    if M2_key not in counter_M2:
        counter_M2[M2_key] = {
                "TIME_OCS": res['time'],
                "MSC": res['MSC'],
                "COUNT": 0
                }
    counter_M2[M2_key]["COUNT"]+= 1

    # METRIC M3 - Number of Traffic Events with DEBITO-OLD>=0 and DEBITO-NEW<0
    if res['debitOld'] >= 0.0 and  res['debitNew'] < 0.0:
        counter_M3 += 1
        M3_data = M3_list.append(res)

    # METRIC M4 - Number of Traffic Events with DEBITOCO-OLD>=0 and DEBITOCO-NEW<0
    if res['debitCOOld'] >= 0.0 and  res['debitCONew'] < 0.0:
        counter_M4 += 1
        M4_data = M4_list.append(res)


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

M1_data = pd.DataFrame(counter_M1.values())
M2_data = pd.DataFrame(counter_M2.values())
M3_data = pd.DataFrame(M3_list)
M4_data = pd.DataFrame(M4_list)
print (f"Number of recs: {counter_nrec} elab_time:{t1-t0:.4f}")
print ()
#print (f"Counter M1: {counter_M1}")
print (f"M1 data:\n{M1_data}")
print ()
#print (f"Counter M2: {counter_M2}")
print (f"M2 data:\n{M2_data}")
print ()
print (f"Counter M3: {counter_M3}")
print (f"M3 data:\n{M3_data}")
print ()
print (f"Counter M4: {counter_M4}")
print (f"M4 data:\n{M4_data}")
print ()

