#!/usr/bin/env python
# -*- coding: utf-8 -*-

import httplib, urllib,json
from datetime import datetime, timedelta
import time
import os
import requests
from requests.auth import HTTPBasicAuth

# !/usr/bin/env python
from pymodbus.constants import Endian
from pymodbus.datastore import ModbusSlaveContext, ModbusSequentialDataBlock, ModbusServerContext
from pymodbus.device import ModbusDeviceIdentification
from pymodbus_modicon import ModiconPayloadBuilder as BinaryPayloadBuilder
from pymodbus.server.sync import StartTcpServer

"""
Pymodbus Synchronous Client Examples
--------------------------------------------------------------------------

The following is an example of how to use the synchronous modbus client
implementation from pymodbus.

It should be noted that the client can also be used with
the guard construct that is available in python 2.5 and up::

    with ModbusClient('127.0.0.1') as client:
        result = client.read_coils(1,10)
        print result
"""
# --------------------------------------------------------------------------- #
# import the various server implementations
# --------------------------------------------------------------------------- #
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
# from pymodbus.client.sync import ModbusUdpClient as ModbusClient
# from pymodbus.client.sync import ModbusSerialClient as ModbusClient

# --------------------------------------------------------------------------- #
# configure the client logging
# --------------------------------------------------------------------------- #
import logging
from logging.handlers import RotatingFileHandler
import socket
import time
import sys
from threading import Thread
from random import randint



class Modbus_server(Thread):
    def __init__(self, host, port):
        super(Modbus_server, self).__init__()
        self.host = host
        self.port = port
        log.info("starting Modbus server on {}:{}".format(self.host, self.port))
        
        store = ModbusSlaveContext(
            di=ModbusSequentialDataBlock(0, [0] * 1),
            co=ModbusSequentialDataBlock(0, [0] * 1),
            hr=ModbusSequentialDataBlock(0, [0] * 5000),
            ir=ModbusSequentialDataBlock(0, [0] * 1))
        self.context = ModbusServerContext(slaves=store, single=True)

        self.identity = ModbusDeviceIdentification()
        self.identity.VendorName = 'Pymodbus'
        self.identity.ProductCode = 'PM'
        self.identity.VendorUrl = 'http://github.com/riptideio/pymodbus/'
        self.identity.ProductName = 'Pymodbus Server'
        self.identity.ModelName = 'Pymodbus Server'
        self.identity.MajorMinorRevision = '1.0'

    def run(self):
        StartTcpServer(self.context, identity=self.identity, address=(self.host, self.port))

class Modbus_client(Thread):
    def __init__(self, host, port):
        super(Modbus_client, self).__init__()
        self.host = host
        self.port = port
        self.client = ModbusClient(self.host, port=self.port)

    def run(self):
        print("starting Modbus client on {}:{}".format(self.host, self.port))
        self.client.connect()

    def write_to_registers(self, startingaddress, values):
        converted_values = []
        for value in values:
            if type(value) != float:
                try:
                    f_value = float(value)
                except ValueError:
                    print("Unknown value: {}".format(value))
            else:
                f_value = value
            converted_values.append(int(f_value * 100))

        self.client.write_registers(startingaddress, converted_values)

    def close(self):
        self.client.close()


class Sigfox_Interface(object):
    error_status = "SUCCESS"
    def __init__(self, logger, sigfox_details):
        super(Sigfox_Interface, self).__init__()
        host = socket.gethostbyname(socket.gethostname())
        port = 5020
        self.logger = log
        self.sigfox_details = sigfox_details

        # how often to contact the igfox API (in minutes)
        self.API_REQUEST_INTERVAL = 15 

        # Log interval (in minutes)
        self.GEKKO_LOG_INTERVAL = 5

        self.modbus_server = Modbus_server(host, port)
        self.modbus_server.daemon = True
        self.modbus_server.start()

        self.modbus_client = Modbus_client(host, port)
        self.modbus_client.daemon = True
        self.modbus_client.start()

        self.run()

    def clear_cmd_prompt(self):
        os.system('cls')

    def hex_conversion(self, hexstr, bits):
        value = int(hexstr,16)
        if value & (1 << (bits-1)):
            value -= 1 << bits
        return value


    def get_sigfox_data(self):
        r = "https://backend.sigfox.com/api/devices/" + self.sigfox_details['deviceId'] + "/messages"
        a = requests.get(r, auth=HTTPBasicAuth(self.sigfox_details['user'], self.sigfox_details['pass']))
        
        if a.status_code != 200:
            self.error_status = "FAILURE: {}".format(a.reason)
            return False
        parsed = a.json()
        data = parsed['data']
        
        for telegram in data:
            unix_time = telegram["time"]
            timestamp = datetime.utcfromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')
        
        observations = {}
        for p in parsed['data']:
            #p = parsed['data'][-1]
            t = datetime.fromtimestamp(p['time']).replace(second=0)
            if t < datetime.now().replace(second=0) - timedelta(days=1): # skip data from more than 24 hours ago
                continue
            d = p['data']
            current_value = self.hex_conversion(d[:4], 16)
            
            #struct.unpack('2B', 
            relative_values = d[4:]
            #current_value = int(current_value, 16)
            #st = struct.unpack('>h', relative_values.decode('hex'))
            samples = [relative_values[i:i+2] for i in range(0, len(relative_values), 2)]
            samples = [self.hex_conversion(h, 8) for h in samples]
            #samples = [int((relative_values[i:i+2]), 8) for i in range(0, len(relative_values), 2)]
            #print(current_value)
            #print(samples)
            
            sampled_values = []
            last_sampled_value = current_value
            for i, sample in enumerate(samples[:2]):
                last_sampled_value = last_sampled_value + sample
                sampled_values.append(last_sampled_value)

            results = [current_value] + sampled_values
            #print(results)
            for i, sample in enumerate(results):
                observation_time = t - timedelta(minutes=5*i)
                observations[observation_time] = sample
            #for obs in sorted(observations):
            #    print("{} : {}".format(obs, observations[obs]))

        current_time = parsed['data'][0]['time']
        current_time = datetime.fromtimestamp(current_time).replace(second=0)

        time_list = [
            current_time.second,
            current_time.minute,
            current_time.hour,
            current_time.day,
            current_time.month,
            current_time.year,
            0,
            0,
            0
        ]
        
        return (time_list, observations)

    def write_to_modbus(self, data):
        time_list = data[0]
        observations = data[1]
        builder = BinaryPayloadBuilder(endian=Endian.Big)
        builder.reset()
        for t in time_list:
            builder.add_16bit_int(t)
        for o in reversed(sorted(observations)):
            builder.add_16bit_int(observations[o])
        payload = builder.build()
        write_address = 20
        modbus_output = []
        for write_index in range(len(payload) / 123):
            write_address = write_index * 123
            try:
                modbus_output = payload[write_address:write_address + 123]
            except IndexError:
                modbus_output = payload[write_address:write_address + 123]
            try:
                result  = self.modbus_client.client.write_registers(write_address, modbus_output, skip_encode=True, unit=1)
            except Exception as e:
                print(e)
                return(0)
        return(len(payload))

    def run(self):
        regs = 0
        firstpass = True
        gekko_ts = None
        while True:
            self.clear_cmd_prompt()
            now = datetime.now()
            # only get data and write to modbus 
            # every 15 minutes at 30 seconds past (i.e. 00:00:30, 00:15:30, 00:30:30 etc.)
            # only check every 20 minutes for new data
            # if now.second >= 30 and now.minute%15==0 and (now - last_data_transfer).seconds >= 20:

            self.API_REQUEST_INTERVAL = 15
            if firstpass or (now.second >= 0 and now.minute%1==0 and (now - last_data_transfer).seconds >= self.API_REQUEST_INTERVAL * 60.0):
                #print("time for a datatranfer")
                data = self.get_sigfox_data()
                last_data_transfer = now
                if data != False:
                    regs = self.write_to_modbus(data)

                    gekko_time = str(data[0][2]) + ':' + str(data[0][1]) + ':' + '00'
                    gekko_date = str(data[0][5]) + '-' + str(data[0][4]) + '-' + str(data[0][3])
                    gekko_ts = gekko_time + " " + gekko_date
                    
                    self.error_status = "SUCCESS"

            elapsed_time = (now - last_data_transfer).seconds
            countdown = self.API_REQUEST_INTERVAL - (elapsed_time/60.0)
            minutes = int(countdown // 1)
            seconds = int((countdown - minutes)*60)
            
            # Print status
            print("Last Sigfox Request at {} had status:".format(last_data_transfer))
            print(self.error_status)
            print("Last GEKKO delivery: {}".format(gekko_ts))
            print("Wrote {} registers to modbus server".format(regs))
            print("")
            print("Next datatranfer in {} minutes, {} seconds".format(
                minutes, seconds)
            )

            firstpass = False
            time.sleep(2)

if __name__ == "__main__":
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)

    logformat = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logformat)
    log.addHandler(ch)

    fh = RotatingFileHandler('sigfox_to_modbus.log', maxBytes=(1048576*5), backupCount=3)
    fh.setFormatter(logformat)
    log.addHandler(fh)

    log.info("Running Sigfox collector")

    # Provide Sigfox API details
    SIGFOX_DETAILS = {
        'deviceId' : '',
        'user': '',
        'pass': '',
    }
    
    app = Sigfox_Interface(logger=log, sigfox_details=SIGFOX_DETAILS)
