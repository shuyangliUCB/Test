# pandas tutorial online
# http://chrisalbon.com/python/pandas_join_merge_dataframe.html

import pandas as pd
from IPython.display import display
from IPython.display import Image
raw_data = {
        'subject_id': ['1', '2', '3', '4', '5'],
        'first_name': ['Alex', 'Amy', 'Allen', 'Alice', 'Ayoung'], 
        'last_name': ['Anderson', 'Ackerman', 'Ali', 'Aoni', 'Atiches']}
df_a = pd.DataFrame(raw_data, columns = ['subject_id', 'first_name', 'last_name'])
raw_data = {
        'subject_id': ['4', '5', '6', '7', '8'],
        'first_name': ['Billy', 'Brian', 'Bran', 'Bryce', 'Betty'], 
        'last_name': ['Bonder', 'Black', 'Balwner', 'Brice', 'Btisan']}
df_b = pd.DataFrame(raw_data, columns = ['subject_id', 'first_name', 'last_name'])
raw_data = {
        'subject_id': ['1', '2', '3', '4', '5', '7', '8', '9', '10', '11'],
        'test_id': [51, 15, 15, 61, 16, 14, 15, 1, 61, 16]}
df_n = pd.DataFrame(raw_data, columns = ['subject_id','test_id'])
#Join the two dataframes along rows
print pd.concat([df_a, df_b]) 
# Join the two dataframes along columns
print pd.concat([df_a, df_b], axis=1)
# Merge two dataframes along the subject_id value
print pd.merge(pd.concat([df_a, df_b]), df_n, on='subject_id')
# Merge two dataframes with both the left and right dataframes using the subject_id key
print pd.merge(pd.concat([df_a, df_b]), df_n, left_on='subject_id', right_on='subject_id')
# Merge with outer join
# "Full outer join produces the set of all records in Table A and Table B, with matching records from both sides where available. If there is no match, the missing side will contain null."
print pd.merge(df_a, df_b, on='subject_id', how='outer')
# Merge with inner join
# "Inner join produces only the set of records that match in both Table A and Table B." 
print pd.merge(df_a, df_b, on='subject_id', how='inner')
# Merge with left join
# "Left outer join produces a complete set of records from Table A, with the matching records (where available) in Table B. If there is no match, the right side will contain null."
print pd.merge(df_a, df_b, on='subject_id', how='left')
# Merge with right join
print pd.merge(df_a, df_b, on='subject_id', how='right')
# Merge while adding a suffix to duplicate column names
print pd.merge(df_a, df_b, on='subject_id', how='left', suffixes=('_left', '_right'))
# Merge based on indexes
print pd.merge(df_a, df_b, right_index=True, left_index=True)


# read csv file
df1 = pd.read_csv('file 1 path')
df2 = pd.read_csv('file 2 path')
df = pd.merge(df1,df2,on='merge key')
df['new colume'] = df.to_csv('name')

