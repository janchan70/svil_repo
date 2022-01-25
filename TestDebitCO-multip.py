import time
import multiprocessing
import sys
import time
import xmltodict

input_filename = sys.argv[1]
print (input_filename)
NWKS  = int(sys.argv[2]) if len(sys.argv)>2 else 5
QSIZE = int(sys.argv[3]) if len(sys.argv)>3 else 1000

def parseRec(xRec):
    dRec = xmltodict.parse(xRec)
    res = {}
    trf = dRec['trf']
    res['timeOCS'] = trf['info']['@time']
    res['typeOfCard'] = trf['sub']['@toc'] if '@toc' in trf['sub'] else "@None"
    res['accountCategory'] = trf['sub']['@acc'] if '@acc' in trf['sub'] else "@None"

    res['baskets'] = []
    for basket in trf['sub']['bsk']:
        basketName = basket['@bn']
        basketValue = basket['@bv']
        res['baskets'].append([basketName, basketValue])

    res['callDate'] = trf['evt']['@cs']
    res['callDuration'] = trf['evt']['@cd']
    res['costOfTheCall'] = trf['evt']['@cost']
    res['ratingBasket'] = trf['evt']['@rb']

    return res

counter_nrec = 0
counter_cost = 0.0
def add_to_counter(res):
    global counter_nrec, counter_cost
    counter_nrec += 1
    counter_cost += float(res['costOfTheCall'])

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

