import pandas as pandas

print("Job complete")

import pandas as pd 
import numpy as np 
import argparse

def extract_data(source):
  return pd.read_csv(source)

def transform_data (data):
   new_data - data. copy()
   new_data[('month', 'year']] - new_data.Monthlear.str.split(' .expand-True)
   new_data(' sex*] = new_data('Sex upon Outcome ].replace('Unknown*, np.nan)
   nеw_data. drop(columns • ['MonthYear* "sex upon Outcome'], implace-True)
   return new_data

def load_data (data, target):
  data.to_csv(target)
if name = main
  parser • argparse.ArgumentParr)
  parser.add_argument('source helps 'source csv') parser. add_argument ('target', help 'target cav')
  args = parser-parse_args)
  print("Starting...")
df = extract_data (args.source)
  new_df - transform_data (df)
  load_data(new_df, args.target) 
print ( "Complete" ) 