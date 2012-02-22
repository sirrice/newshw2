#
# This file parses the FEC datasets and generates
# 1) properly formatted CSV files for postgresql
# 2) setup.sql - creates tables and copies CSV files into postgresql
#
#

import sys, csv
import datetime
from db import *
from collections import *

pacfields = ['ID',
'NAME',
'TYPE',
'DESIG',
'FF',
'Total Receipts',
'Transfers From Affiliates',
'Contributions Received from Individuals',
'Contributions received from Other Political Committees',
'Contributions from the Candidate',
'Candidate Loan',
'Total Loans Received            ',
'Total Disbursements             ',
'Transfers To Affiliates         ',
'Refunds to Individuals          ',
'Refunds to Other Political Committees   ',
'Candidate Loan Repayments', 
'Loan Repayments ',
'Cash Beginning of Year 1',
'Cash Close of Period    ',
'Debts Owed By           ',
'Nonfederal Transfers Received',
'Contributions Made to Other Committees',
'Independent Expenditures Made         ',
'Party Coordinated Expenditures Made   ',
'Nonfederal  Share of Expenditures     ',
'enddate',
'year']
pacfields = [field.replace(' ','').replace('from', '').replace('the', '').strip().lower() for field in pacfields]
pactypes = ([str] * 5) + ([int] * 21) + [datetime.datetime, int]

housefields = ['candid', 'candname', 'incumbent', 'party', 'desig', 'totalrecipts', 'auth_xfer_from',
        'totaldisbursments', 'auth_xfer_to', 'cash_start', 'cash_end', 'contrib_self',
        'loans_self', 'loans_other', 'repayments_self', 'repayments_other', 'totaldebt',
        'contrib_indiv', 'state', 'district', 'status_special', 'status_primary', 'status_runoff',
        'status_general', 'perc_general', 'contrib_otherparty', 'contrib_ownparty',
        'enddate', 'refunds_indiv', 'refunds_committees', 'year']
contribfields = ['cmte_id','cand_id','cand_nm','contbr_nm','contbr_city','contbr_st','contbr_zip','contbr_employer','contbr_occupation','contb_receipt_amt','contb_receipt_dt','receipt_desc','memo_cd','memo_text','form_tp','file_num', 'year']
expendfields = ['cmte_id','cand_id','recipient_nm','disb_amt','disb_dt','recipient_city','recipient_st','recipient_zip','disb_desc','memo_cd','memo_text','form_tp','fil', 'year']


def tosql(tablename, types, fields):
    print tablename, len(types), len(fields)
    #print types
    #print fields
    types = map(type2str, types)
    sql = ',\n'.join(['%s %s' % (f, t) for t, f in zip(types, fields)])
    sql = """drop table %s;\ncreate table %s (%s);""" % (tablename, tablename, sql)
    return sql

def get_type(val):
    try:
        d = datetime.datetime.strptime(val, '%d-%b-%y')
        return datetime.datetime
    except:
        pass
    try:
        d = datetime.datetime.strptime(val, '%Y-%m-%d')
        return datetime.datetime
    except:
        pass
    try:
        i = int(val)
        return int
    except:
        pass
    try:
        f = float(val)
        return float
    except:
        pass
    return str

def convert(t, val):
    try:
        if t == datetime.datetime:
            d = date.strftime('%Y-%m-%d')
            print d
            return d
    except:
        pass
    try:
        if t == int:
            return int(val)
    except:
        return 0
    return val
    
def detect_types(rowiter):
    rowiter.next()
    rowiter.next()
    linenum = 0
    types = [Counter() for j in xrange(max([len(rowiter.next()) for i in xrange(10)]))]
    for row in rowiter:
        for key, val in enumerate(row):
            types[key][get_type(val)] += 1
        linenum += 1
        if linenum > 50:
            break
    return [c.most_common(1)[0][0] for c in types]

def contrib_iter(fname, year):
    reader = csv.reader(open(fname, 'r'))
    for row in reader:
        row.append(year)
        yield row
        

def pac_iter(fname, year):
    f = file(fname, 'r')
    offsets = [9, 90, 1, 1, 1, 10] + ([10]*20) + [2, 2, 4]
    offsets = [sum(offsets[:idx+1]) for idx in range(len(offsets))]
    for line in f:
        idx = 0
        row = []
        for offset in offsets:
            row.append( line[idx:offset].strip().replace(',','').replace('"','').replace("'", '') )
            idx = offset
        datestr = '%s-%s-%s' % (row[-1], row[-3], row[-2])
        row = row[:-3]
        row.append(datestr)
        row.append(year)
        yield row
    f.close()



def house_iter(fname, year):
    f = file(fname, 'r')
    offsets = [9, 47, 48, 49, 52, 62, 72, 82, 92, 102, 112, 122, 132, 142, 152, 162, 172, 182, 184, 186,
                187, 188, 189, 190, 193,  203, 213, 221, 231, 241]
    for line in f:
        idx = 0
        row = []
        for offset in offsets:
            row.append( line[idx:offset].strip().replace(',','').replace('"','').replace("'", '') )
            idx = offset
        datestr = row[-3]
        datestr = '%s-%s-%s' % (datestr[-4:], datestr[:2], datestr[2:-4])
        row[-3] = datestr
        row.append(year)
        yield row
    f.close()

def type2str(t):
    if t == datetime.datetime:
        return 'date'
    if t == int:
        return 'float'
    return 'varchar(100)'



def validate_type(t, v):
    if t == datetime.datetime:
        if '0000-00-00' == v:
            return False
    if t == int:
        try:
            float(v)
        except:
            return False
    
    return True


def convert_type(v):
    if '0000-00-00' == v:
        return "'now'"
    return v


def write_iter(fname, rowiter, types):
    f = file(fname, 'w')
    for idx, row in enumerate(rowiter):
        row = map(str, row)
        row = map(lambda s: s.strip().replace(',', '').replace('"', '').replace("'", ''), row)
        row = map(convert_type, row)
        validation = [validate_type(t,v) for t, v in zip(types, row)]
        if reduce(lambda a,b: a and b, validation):
            print >>f, ','.join(row)
        else:
            print validation
            print ','.join(row)
#        if idx > 500:
#            break
    f.close()




detect_types(pac_iter('pac_2012.dat', 2012))
detect_types(pac_iter('pac_2010.dat', 2010))

housetypes = detect_types(house_iter('house_2012.dat', 2012))
housetypes = map(type2str, housetypes)
expendtypes = detect_types(contrib_iter('expend_2012.txt', 2012))
contribtypes = detect_types(contrib_iter('contrib_2012.txt', 2012))

ROOT = '/Users/sirrice/mitnotes/courses/news/hw2/FECWEB'
f = file('setup.sql', 'w')
print >>f,  tosql('pac', pactypes, pacfields)
print >>f,  tosql('house', housetypes, housefields)
print >>f,  tosql('expend', expendtypes, expendfields)
print >>f,  tosql('contrib', contribtypes, contribfields)
print >>f, "copy pac from '%s/pac_2012.csv' with csv;" % ROOT
print >>f, "copy pac from '%s/pac_2010.csv' with csv;" % ROOT
print >>f, "copy house from '%s/house_2012.csv' with csv;" % ROOT
print >>f, "copy expend from '%s/expend_2012.csv' with csv;" % ROOT
print >>f, "copy contrib from '%s/contrib_2012.csv' with csv;" % ROOT
print >>f, "copy pac from '%s/webk00.csv' with csv;" % ROOT
print >>f, "copy pac from '%s/webk02.csv' with csv;" % ROOT
print >>f, "copy pac from '%s/webk04.csv' with csv;" % ROOT
print >>f, "copy pac from '%s/webk06.csv' with csv;" % ROOT
print >>f, "copy pac from '%s/webk08.csv' with csv;" % ROOT

f.close()


write_iter('pac_2012.csv', pac_iter('pac_2012.dat', 2012), pactypes)
write_iter('pac_2010.csv', pac_iter('pac_2010.dat', 2010), pactypes)
write_iter('house_2012.csv', house_iter('house_2012.dat', 2012), housetypes)
write_iter('expend_2012.csv', contrib_iter('expend_2012.txt', 2012), expendtypes)
write_iter('contrib_2012.csv', contrib_iter('contrib_2012.txt', 2012), contribtypes)
write_iter('webk00.csv', pac_iter('webk00.dat', 2000), pactypes)
write_iter('webk02.csv', pac_iter('webk02.dat', 2002), pactypes)
write_iter('webk04.csv', pac_iter('webk04.dat', 2004), pactypes)
write_iter('webk06.csv', pac_iter('webk06.dat', 2006), pactypes)
write_iter('webk08.csv', pac_iter('webk08.dat', 2008), pactypes)
