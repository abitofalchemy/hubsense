# -*- coding: utf-8 -*-
import glob
import os
import sys
import json
import argparse
import pandas as pd
from influxdb import DataFrameClient,InfluxDBClient

debug = True

def write_habitslab_df_to_influx(
        pd_df, studyName, dataType, pId, wearable, deviceId, userId, fname):
    if pd_df is None: return
    # index of the dataframe
    pd_df['time'] = pd.to_datetime(pd_df['date'])
    pd_df = pd_df.set_index('time')
    tags = {"device": wearable,
            "deviceId": deviceId,
            "pId":      pId,
            # optional to add 'filename'
            "studyId":  studyName,
            "type":     dataType,
            "userId":   userId,
            "fileName": fname}
    pd_df.drop(['date'], inplace=True, axis=1)
    # connection to db 
    dbConnDF = DataFrameClient(host='localhost', port=8086, database='habitsDB')
    dbConnDF.write_points(pd_df,'habitsDB',tags=tags, batch_size=1000)
    return 




def frame_to_json_obj_fn(row,pId, studyName, dataType, userId, client):
    inflx_dict = [
            {"measurement": "necklace",
             "tags": {
                                        "deviceId": "NGL1",
                                        "pId": pId,
                                        "studyId": studyName,
                                        "dataCat": dataType,
                                        "userId": userId
                                        },
             "time": row['date'],
             "fields": {
                                        "rtime":     row['Time'],
                                        "proximity": row['proximity'],
                                        "ambient":   row['ambient'],
                                        "leanForward":row['leanForward'],
                                        "qW":  row['qW'],
                                        "qX":  row['qX'],
                                        "qY":  row['qY'],
                                        "qZ":  row['qZ'],
                                        "aX":  row['aX'],
                                        "aY":  row['aY'],
                                        "aZ":  row['aZ'],
                                        "power": row['power'],
                                        "cal": row['cal']
                                        },
                                        }]
    #print(json.dumps(inflx_dict, indent=4, sort_keys=True))
    #print("Write points: {0}".format(inflx_dict))
    client.switch_database('habitsDB')
    client.write_points(inflx_dict)
    



def main(studyName, dataType, participantId, wearable, devId, userId):
    """ main function
    arguments:
    - participantId study participant
    """

    rootDir = "/opt/fsmresfiles/" + studyName +"/"

    if userId is None:
        from pathlib import Path
        print("! detecting userId")
        userId = Path.home().parts[-1]

    inflx_dbconn= InfluxDBClient('localhost', 8086, database='habitsDB')
    files = glob.glob(rootDir+dataType +"/"+
                        participantId +"/" +
                        wearable + "/*.csv")
    for f in files:
        df = pd.read_csv(f, header=0)
        write_habitslab_df_to_influx(
                df, studyName, dataType, participantId, wearable, devId, userId,
                os.path.basename(f))
        print(os.path.basename(f))
        #df.apply(lambda row: frame_to_json_obj_fn(row,
        #    participantId,studyName, dataType, wearable, devId, userId, inflx_dbconn), axis=1)

    #print(json.dumps(nflx_point_dict, indent=4, sort_keys=True))
    return 

def parse_args():
    """Parse the args from main."""
    parser = argparse.ArgumentParser(description='Create Points for InfluxDB')
    parser.add_argument("-s","--study", type=str, required=True, help="Study Name")
    parser.add_argument("-t", "--datatype", type=str, required=False, 
            default="CLEAN", help="Data Type [CLEAN|RAW]")
    parser.add_argument('-p', '--participant', type=str, required=False,
                        default='TEST', help='Participant ID')
    parser.add_argument('-d', '--device', type=str, required=True,
    help='Device')
    parser.add_argument('--deviceid', type=str, required=True, help='Device Id')
    parser.add_argument("-u", "--userid", type=str, required=False,
            help="User Id (optional)")


    return parser


if __name__ == '__main__':
    args = parse_args()

    args = args.parse_args()
    main(
            studyName=args.study,
            dataType=args.datatype,
            participantId=args.participant,
            wearable=args.device,
            devId = args.deviceid,
            userId=args.userid)
