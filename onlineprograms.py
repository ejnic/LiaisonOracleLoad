import pandas as pd
import sqlalchemy
import datetime
import connection as con


engine = sqlalchemy.create_engine(con.strengine)

df = pd.read_excel(homepath + 'OnlinePrograms.xlsx', skiprows=1, names = ['program','plan','online','hybrid','description','programcode'])
df.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)
programcodes = df.iloc[:,[5]]
print(programcodes.head(10))

programcodes.to_sql('onlineprograms', engine, 'ejnic', if_exists='replace', chunksize=1000,
                 dtype=
                 {'programcode':sqlalchemy.types.VARCHAR(programcodes.programcode.str.len().max())
                 })
