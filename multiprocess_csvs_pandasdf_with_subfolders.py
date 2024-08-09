import glob
import os
from multiprocessing import Pool
import pandas as pd
import numpy as np

#Note, this only goes 1 layer deeper, for a folder which contains folder
#where operations are done on the files in each subfolder

mega_array = []

# wrap your csv importer in a function that can be mapped
def read_csv(filename):
    #converts a filename to a pandas dataframe'
    return pd.read_csv(f"{filename}")

def main():
  maindirectory = "./" #anything you want
  for subfolder in os.listdir('./{maindirectory}/'): 
    if subfolder.startswith(("tuple","of","your","choice")):
      files = os.listdir(f'./{maindirectory}/{subfolder}')
      file_list = [filename for filename in files if (filename.split('.')[1]=='csv')]
      #Make sure to get right full path
      file_list = [f'./{maindirectory}/{subfolder}/' + x for x in file_list if not str(x) == "nan"]
      # set up your pool
      with Pool(processes=4) as pool: # or whatever your hardware can support
          # have your pool map the file names to dataframes
          df_list = pool.map(read_csv, file_list)
          # reduce the list of dataframes to a single dataframe
          combined_df = pd.concat(df_list, ignore_index=True)
          mega_array.append(combined_df)
      pool.join()
      pool.close()
      pool.terminate()
      
if __name__ == '__main__':
    main()

print("We finished all the files.")
df_vs = pd.concat(mega_array, ignore_index=True)
print(df_vs.head(5))
