import csv
import sys
import random
import time
import datetime
import socket
import StringIO
from datetime import datetime
# install kafka library using pip:
# $ pip install kafka-python
from kafka import KafkaProducer

#HOST, PORT = "localhost", 9191

# Create a socket (SOCK_STREAM means a TCP socket)
producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')


try:
    # writer = csv.writer(f)
    i = 0
    while True:

		t1 = time.time()
                newt1=t1*1000
                deviceReceiptTime1=int(newt1)
                
                art=random.randint(0, 9999)
                amount=random.choice(["999889", "2399812", "1199700", "11154"])
                sourceId=random.randint(1, 4)
		mcc=random.randint(1000, 2000)
		data1=random.randint(1000, 2300000)
                transactionType   = random.choice(["04", "05"])
		txnSubType   = random.choice(["U11", "U21", "U3", "U4"])
		channel   = random.choice(["POS"])
		(dt, micro) = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f').split('.')
		dt = "%s.%03d" % (dt, int(micro) / 1000)
		terminalId = random.choice(["K1", "K2", "K3", "K4","K11", "K21", "K31", "K41"])
		transactionResponseCode = "442"               
		channelTypeId="09"

                #data = ",".join(["b0dd3f13-fbca-"+str(deviceReceiptTime1)+"-49790bc3bb04~#~1~#~"+str(sourceId)+"~#~3~#~4011~#~5~#~"+str(dt)+"~#~"+str(amount)+"~#~"+channel+"~#~9~#~"+terminalId+"~#~366~#~12~#~13~#~14~#~15~#~16~#~"+transactionResponseCode+"~#~"+transactionType+"~#~"+channelTypeId+"~#~00~#~SUCCESS"])
                data = ",".join(["LVDT_f17-adc-"+"~T1:~"+str(dt)+"~#~"+str(deviceReceiptTime1)+"-49790bc3bb04~#~1~#~"+str(sourceId)+"~#~"+"~T2:~"+str(dt)+str(amount)+"~#~"+channel+"~#~9~#~"+terminalId+"~#~366~#~"+transactionResponseCode+"~#~"+transactionType+"~#~"+"~T3:~"+str(dt)+"~#~"+channelTypeId+"~#~00~#~SUCCESS"])

                producer.send('LP300CATs18',(data + "\n").encode())
                #print(data)
                i = i + 1
                time.sleep(1)
finally:
		print ' '
