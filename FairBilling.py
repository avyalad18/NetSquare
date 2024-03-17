import sys
import pandas as pd
import time as t


class FairBilling :

    def __init__(self, file_path):
        self.__file = file_path
        self.readData()
    
    def readData(self):
        # reading text file using pandas based on seprator with no header 
        df = pd.read_csv(self.__file, sep=' ', header=None)
        df.columns = ['time', 'user', 'session']

        # converting data type of column time to datetime 
        df['time'] = pd.to_datetime(df['time'],errors='coerce', format='mixed', dayfirst=True)
        # dropping the null values from time and session column
        df.dropna(subset=['time','session'], inplace=True)
      
        # filtering data based of session flages i.e. "Start" , "End"
        checkValues = ['Start','End']
        df = df[df['session'].isin(checkValues)]  
        df = df.sort_values(by='time')
     
        # seprating df based in session flags
        startDF = df.loc[df['session']=='Start']
        endDF = df.loc[df['session']=='End']
        
        startDF['starttime'] = startDF['time']
        endDF['starttime'] = pd.NaT
        
        startDF['endtime'] = pd.NaT
        endDF['endtime'] = endDF['time']

      
        sessionsDF = pd.DataFrame(columns=['user', 'starttime', 'endtime'])
        # iterating base on unique users in data
        for user in df['user'].unique():          
            # checking if number for session starts is less than the end of session if yess then adding values accroidn to it
            if int(startDF['user'].loc[startDF['user']==user].count()) < int(endDF['user'].loc[endDF['user']==user].count()):

                    for i, row in endDF.loc[endDF['user']==user].iterrows():
                        try:
                            sessionsDF = sessionsDF._append({'user': row['user'], 'starttime': startDF[i]['starttime'] , 'endtime': row['endtime']}, ignore_index=True)
                        except:
                            sessionsDF = sessionsDF._append({'user': row['user'], 'starttime': df['time'].min() , 'endtime': row['endtime']}, ignore_index=True)                    
            
            # checking if number for session starts is greater than the end of session if yess then adding values accroidn to it
            elif int(startDF['user'].loc[startDF['user']==user].count()) > int(endDF['user'].loc[endDF['user']==user].count()) :
                    for i, row in startDF.loc[startDF['user']==user].iterrows():
                        try:
                            sessionsDF = sessionsDF._append({'user': row['user'], 'starttime': row['starttime'] , 'endtime': endDF[i]['endtime']}, ignore_index=True)
                        except:
                            sessionsDF = sessionsDF._append({'user': row['user'], 'starttime': row['starttime']  , 'endtime': df['time'].max()}, ignore_index=True)
            
            # if both flages are equal
            elif int(startDF['user'].loc[startDF['user']==user].count()) == int(endDF['user'].loc[endDF['user']==user].count()) :
                    for i, row in startDF.loc[startDF['user']==user].iterrows():
                        try:
                            sessionsDF = sessionsDF._append({'user': row['user'], 'starttime': row['starttime'] , 'endtime': endDF[i]['endtime']}, ignore_index=True)
                        except:
                            pass

        sessionsDF['duration'] = (sessionsDF['endtime'] - sessionsDF['starttime']).dt.total_seconds()       
        result = sessionsDF.groupby('user').agg(sessions=('starttime', 'size'), totalduration=('duration', 'sum')).reset_index()
        result['totalduration'] = result['totalduration'].astype(int)  
        print(result)
        result.to_csv('output.csv')
        


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python FairBilling.py samplelog.txt")
        sys.exit(1)

    file_path = sys.argv[1]
    FairBilling(file_path)