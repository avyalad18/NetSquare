import sys
from datetime import datetime
import pandas as pd


class FairBilling :

    def __init__(self, file_path):
        self.__file = file_path
        self.readData()
    
    def readData(self):
        df = pd.read_csv(self.__file, sep=' ', header=None)
        df.columns = ['time', 'user', 'session']
        df['time'] = pd.to_datetime(df['time'],errors='coerce', format='mixed', dayfirst=True)
        df.dropna(subset=['time'], inplace=True)
        df.dropna(subset=['session'], inplace=True)  
     
        sessionsDF = pd.DataFrame(columns=['user', 'starttime', 'endtime'])

        for i, row in df.iterrows():
            user = row['user']
            time = row['time']
            session = row['session']
            if session == 'Start':
                sessionsDF = sessionsDF._append({'user': user, 'starttime': time, 'endtime': pd.NaT}, ignore_index=True)
            elif session == 'End':
                # Find the matching 'Start' time for the current 'End' time
                if not sessionsDF.empty:
                    start_index = sessionsDF[(sessionsDF['user'] == user) & (sessionsDF['endtime'].isnull())]
                    start_index = sessionsDF.index[-1]
                    sessionsDF.at[start_index, 'endtime'] = time
                else :
                    print("found end but not have start")
        
      
        max_time = df['time'].max()
        min_time = df['time'].min()
        sessionsDF['endtime'] = sessionsDF['endtime'].fillna(max_time)     
        sessionsDF['starttime'] = sessionsDF['starttime'].fillna(min_time)

        sessionsDF['duration'] = (sessionsDF['endtime'] - sessionsDF['starttime']).dt.total_seconds()
       
        result = sessionsDF.groupby('user').agg(sessions=('starttime', 'size'), totalduration=('duration', 'sum')).reset_index()
        result['totalduration'] = result['totalduration'].astype(int)          

        result.to_csv("output.csv")

      
   

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python FairBilling.py samplelog.txt")
        sys.exit(1)

    file_path = sys.argv[1]
    FairBilling(file_path)