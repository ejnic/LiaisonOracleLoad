#!/usr/bin/env python



#import cx_oracle
import pandas as pd
import sqlalchemy
import datetime
import connection as con


engine = sqlalchemy.create_engine(con.strengine)

#load users from file from Liaison
df = pd.read_csv(con.homepath + 'indianausers.csv', skiprows=1, names = ['campus','first_name','last_name','email','phone_number','extension','primary','is_active','programname','webadmitname','roles','users_created_at','users_created_at2','last_login_at','login_count'])
df.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)
#trim to columns needed
users = df.iloc[:,[0,1,2,3,7,8,9,10,11,13,14]]

#parse email to networkID, fix column names
emailparse = users["email"].str.split("@", n = 2, expand = True) 
users = users.join(emailparse)
users = users.drop(1, axis=1)
users.rename(columns = {0:'networkid'}, inplace = True)
users['webadmitname'] = users['webadmitname'].str.lower()

users.to_sql('wausers', engine, 'ejnic', if_exists='replace', chunksize=1000, 
                 dtype=
                 {'campus':sqlalchemy.types.VARCHAR(df.campus.str.len().max()),
                 'first_name':sqlalchemy.types.VARCHAR(df.first_name.str.len().max()),
                 'last_name':sqlalchemy.types.VARCHAR(df.last_name.str.len().max()),
                 'email':sqlalchemy.types.VARCHAR(df.email.str.len().max()),
                 'programname':sqlalchemy.types.VARCHAR(df.programname.str.len().max()),
                 'webadmitname':sqlalchemy.types.VARCHAR(df.webadmitname.str.len().max()),
                 'roles':sqlalchemy.types.VARCHAR(df.roles.str.len().max()),
                 'networkid':sqlalchemy.types.VARCHAR(25),
                 'is_active':sqlalchemy.types.VARCHAR(2),
                 'users_created_at':sqlalchemy.types.VARCHAR(50),
                 'users_created_at2':sqlalchemy.types.VARCHAR(50),
                 'last_login_at':sqlalchemy.types.VARCHAR(50),
                 'login_count':sqlalchemy.types.VARCHAR(25)                
                 }) 

#load programs from file from Liaison 19-20
df = pd.read_csv(con.homepath + 'ExportPrograms.csv', skiprows=1, names = ['cycle','campus', 'programname', 'webadmitname', 'programcode', 'startyear',
       'startterm', 'school', 'delivery', 'degree', 'careercode', 'startdate',
       'deadline', 'deadlinedisplay', 'appfee', 'city','state', 'status', 'updateddate', 'programid'] )
df.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)
#trim columns and fix names
programs = df.iloc[:,[1,2,3,4,5,6,7,8,9,10,11,12,13,14,17,18,19]]
programs['webadmitname'] = programs['webadmitname'].str.lower()

programs.to_sql('waprograms', engine, 'ejnic', if_exists='replace', chunksize=1000,
                 dtype = 
                {'campus':sqlalchemy.types.VARCHAR(programs.campus.str.len().max()),
                'programname':sqlalchemy.types.VARCHAR(programs.programname.str.len().max()),
                'webadmitname':sqlalchemy.types.VARCHAR(programs.webadmitname.str.len().max()),
                'programcode':sqlalchemy.types.VARCHAR(programs.programcode.str.len().max()),
                #'StartYear':sqlalchemy.types.VARCHAR(128), 
                'startterm':sqlalchemy.types.VARCHAR(programs.startterm.str.len().max()),
                'school':sqlalchemy.types.VARCHAR(programs.school.str.len().max()),
                'delivery':sqlalchemy.types.VARCHAR(programs.delivery.str.len().max()),
                'degree':sqlalchemy.types.VARCHAR(programs.degree.str.len().max()),
                'careercode':sqlalchemy.types.VARCHAR(programs.careercode.str.len().max()),
                'startdate':sqlalchemy.types.VARCHAR(128),
                'deadline':sqlalchemy.types.VARCHAR(128), 
                #'appfee':sqlalchemy.types.VARCHAR(programshort.appfee.str.len().max()),
                'deadlinedisplay':sqlalchemy.types.VARCHAR(128),
                'status':sqlalchemy.types.VARCHAR(programs.status.str.len().max()),
                'updateddate':sqlalchemy.types.VARCHAR(128)
                #'ProgramID':sqlalchemy.types.VARCHAR(128)
                 })

#load programs from file from Liaison 19-20
df = pd.read_csv(con.homepath + 'ExportPrograms2021.csv', skiprows=1, names = ['cycle','campus', 'programname', 'webadmitname', 'programcode', 'startyear',
       'startterm', 'school', 'delivery', 'degree', 'careercode', 'startdate',
       'deadline', 'deadlinedisplay', 'appfee', 'city','state', 'status', 'updateddate', 'programid'] )
df.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)
#trim columns and fix names
programs = df.iloc[:,[1,2,3,4,5,6,7,8,9,10,11,12,13,14,17,18,19]]
programs['webadmitname'] = programs['webadmitname'].str.lower()

programs.to_sql('waprograms2021', engine, 'ejnic', if_exists='replace', chunksize=1000,
                 dtype =
                {'campus':sqlalchemy.types.VARCHAR(programs.campus.str.len().max()),
                'programname':sqlalchemy.types.VARCHAR(programs.programname.str.len().max()),
                'webadmitname':sqlalchemy.types.VARCHAR(programs.webadmitname.str.len().max()),
                'programcode':sqlalchemy.types.VARCHAR(programs.programcode.str.len().max()),
                #'StartYear':sqlalchemy.types.VARCHAR(128),
                'startterm':sqlalchemy.types.VARCHAR(programs.startterm.str.len().max()),
                'school':sqlalchemy.types.VARCHAR(programs.school.str.len().max()),
                'delivery':sqlalchemy.types.VARCHAR(programs.delivery.str.len().max()),
                'degree':sqlalchemy.types.VARCHAR(programs.degree.str.len().max()),
                'careercode':sqlalchemy.types.VARCHAR(programs.careercode.str.len().max()),
                'startdate':sqlalchemy.types.VARCHAR(128),
                'deadline':sqlalchemy.types.VARCHAR(128),
                #'appfee':sqlalchemy.types.VARCHAR(programshort.appfee.str.len().max()),
                'deadlinedisplay':sqlalchemy.types.VARCHAR(128),
                'status':sqlalchemy.types.VARCHAR(programs.status.str.len().max()),
                'updateddate':sqlalchemy.types.VARCHAR(128)
                #'ProgramID':sqlalchemy.types.VARCHAR(128)
                 })

strsql = "select distinct campus, networkid, webadmitname from wausers a where a.roles like 'Director of Adm%' and concat(a.campus,a.networkid) not in (select concat(b.campus,b.networkid) from wauserexceptions b)"
df = pd.read_sql_query(strsql, engine)
df.to_sql('wadiradmissions', engine, 'ejnic', if_exists='replace', chunksize=1000,
         dtype = 
                {'campus':sqlalchemy.types.VARCHAR(df.campus.str.len().max()),
                 'networkid':sqlalchemy.types.VARCHAR(df.networkid.str.len().max()),
                 'webadmitname':sqlalchemy.types.VARCHAR(programs.webadmitname.str.len().max()+5),
                })

strsql="select * from waprograms a where concat(a.campus, a.webadmitname) not in (select concat(b.campus, b.webadmitname) from wadiradmissions  b)"
df = pd.read_sql_query(strsql, engine)
df.to_sql('wadiradmissionsmissing', engine, 'ejnic', if_exists='replace', chunksize=1000,
          dtype = 
                {'campus':sqlalchemy.types.VARCHAR(programs.campus.str.len().max()),
                'programname':sqlalchemy.types.VARCHAR(programs.programname.str.len().max()),
                'webadmitname':sqlalchemy.types.VARCHAR(programs.webadmitname.str.len().max()),
                'programcode':sqlalchemy.types.VARCHAR(programs.programcode.str.len().max()), 
                'startterm':sqlalchemy.types.VARCHAR(programs.startterm.str.len().max()),
                'school':sqlalchemy.types.VARCHAR(programs.school.str.len().max()),
                'delivery':sqlalchemy.types.VARCHAR(programs.delivery.str.len().max()),
                'degree':sqlalchemy.types.VARCHAR(programs.degree.str.len().max()),
                'careercode':sqlalchemy.types.VARCHAR(programs.careercode.str.len().max()),
                'startdate':sqlalchemy.types.VARCHAR(128),
                'deadline':sqlalchemy.types.VARCHAR(128), 
                'deadlinedisplay':sqlalchemy.types.VARCHAR(128),
                'status':sqlalchemy.types.VARCHAR(programs.status.str.len().max()),
                'updateddate':sqlalchemy.types.VARCHAR(128)
                 })
strdate = '{date:%Y%m%d}'.format( date=datetime.datetime.now())
df.to_excel(con.homepath + 'NoDirAdmis_'+ strdate + '.xlsx')

strsql = """select distinct a.email from wausers a
where
a.campus not in ('IUPUI', 'IUPUC')
and
a.roles like '%Director of Admissions%'
and a.is_active = 1"""
df = pd.read_sql_query(strsql, engine)
df.to_excel(con.homepath + 'emaillist_'+ strdate + '.xlsx')
