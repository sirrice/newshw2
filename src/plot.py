import math
from db import *
from dateutil.parser import parse as dateparse
from collections import *
import matplotlib
matplotlib.use("Agg")
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np


def pac_summary(db, fname, where=''):

    #pp = PdfPages('figs/%s' % fname)
    fig = plt.figure(figsize=(10, 5))
    sub = fig.add_subplot(111)
    sub2 = sub.twinx()

    
    sql = """select year, sum(totalreceipts / 1000000000.0), stddev(totalreceipts / 1000000000.0), count(*)
    from pac where %s year > 2000 group by year order by year;""" % where
    data = query(db, sql)
    year, total, std, n = zip(*data)
    total = np.array(total)
    std = np.array(std) 
    sub.errorbar(year, total, yerr=std, color='r', label="total PAC $$$")
    sub.set_ylabel("Dollars (Billions)")
    sub2.plot(year, n,  color='r', ls="--", label="# PACs")

    # sql = "select year, sum(totalreceipts / 1000000000.0), stddev(totalreceipts / 1000000000.0), count(*)  from pac where type = 'P' and year > 2000 group by year order by year;"
    # data = query(db, sql)
    # year, total, std, n = zip(*data)
    # total = np.array(total)
    # std = np.array(std) 
    # sub.errorbar(year, total, yerr=std, color='b', label="total PAC $$$")
    # sub.set_ylabel("Dollars (Billions)")
    # sub2.plot(year, n,  color="b", ls='--', label="# PACs")




    sub2.set_ylabel("Number of PACs")
#    sub.set_ylim(0, 2)
    sub.set_xlabel('Year')
    sub.legend(loc='upper left')
    sub2.legend(loc='upper right')
    sub.set_title("All PAC Year by Year Summary")
    plt.savefig('figs/total_%s' % fname)

    years = year
    fig = plt.figure(figsize=(10, 5))
    sub = fig.add_subplot(111)
    for year in years:
        year = int(year)
        sql = """select 1000*((totalreceipts/1000)::int) as bucket, count(*)
        from pac where %s year = %d group by bucket""" % (where, year)
        data = query(db, sql)
        data.sort(key=lambda pair:pair[0])
        bucket, n = zip(*data)
        total = [b*c for b, c in data]
        n = total
        n = np.array([sum(n[:idx+1]) for idx in xrange(len(n))])
        n = n / 1000000.0
        bucket = np.array(bucket) / 1000000.0
        sub.plot(bucket, n, label=str(year))
        print year
    sub.set_title("Cumulative PAC Money")
    sub.set_ylabel('Total PAC funds (Millions)')
    sub.set_xlabel('PAC funds (Millions)')

    sub.legend(loc='upper left', ncol=6)
    plt.savefig('figs/cum_%s' % fname)#, format='pdf')
    #pp.close()

def plot_st_by_week(db, id2name):
    states = ['MA', 'IL', 'PA', 'MO', 'WI']
    template = "select date_trunc('week', disb_dt) as week, sum(disb_amt) as total from expend where cand_id = '%s' and recipient_st = '%s' and date_part('year', disb_dt) > 2008 group by week order by total asc;"

    pp = PdfPages('byweek_state.pdf')    
    for state in states:
        fig = plt.figure(figsize=(30, 10))
        sub = fig.add_subplot(111)
            
        for candid, name in id2name.items():
            sql = template % (candid, state)
            data = query(db, sql)
            data.sort(key=lambda pair: pair[0])
            if not data:
                print name
                continue
            weeks, amounts = zip(*data)
            amounts = [sum(amounts[:idx+1]) for idx in xrange(len(amounts))]
            amounts = np.array(amounts) / 1000000.0
            sub.plot(weeks, amounts, label=name)
            
            
        sub.set_title("Candidate expenditures by week in %s" % state)
        sub.set_ylabel("Expenditures (Millions)")
        sub.set_xlabel("date")
        sub.legend(loc='upper center', ncol=5)
        plt.savefig(pp, format='pdf')
    pp.close()
    


    

def plot_by_week(db, fname, id2name):
    #pp = PdfPages('byweek.pdf')    
    sql = """select byweek.week, byweek.cand_id, byweek.total, h.candname
    from (select date_trunc('week', disb_dt) as week, cand_id, sum(disb_amt) as total
          from expend group by week, cand_id order by cand_id, week) as byweek left join house as h
          on h.candid = byweek.cand_id;"""
    sql = "select * from byweek;"
    data = query(db, sql)

    id2weeks = defaultdict(list)
    
    for row in data:
        #row[0] = dateparse(row[0])
        if row[0].year < 2007: continue
            
        
        id2name[row[1]] = row[-1] or row[1]
        id2weeks[row[1]].append( (row[0], float(row[2])) )

    for weeks in id2weeks.values():
        weeks.sort(key=lambda pair:pair[0])


    fig = plt.figure(figsize=(30, 10))
    sub = fig.add_subplot(111)
    for candid, vals in id2weeks.items():
        weeks, amounts = zip(*vals)
        amounts = [sum(amounts[:idx+1]) for idx in xrange(len(amounts))]
        amounts = np.array(amounts) / 1000000.0
        sub.plot(weeks, amounts, label=id2name[candid])
    sub.set_title("candidate expenditures by week")
    sub.set_ylabel("Expenditures (Millions)")
    sub.set_xlabel("date")
    sub.legend(loc='upper center', ncol=5)
    plt.savefig('figs/%s' % fname)#, format='pdf')
    #pp.close()

db = connect('fec')
id2name = {}
pac_summary(db, 'pac_indep_summary.png', "type = 'I' and ")
pac_summary(db, 'pac_summary.png', "")
plot_by_week(db, 'byweek.png', id2name)
#plot_st_by_week(db, id2name)

db.close()



"""drop table byweek; create table byweek as select byweek.week, byweek.cand_id, byweek.total, h.candname  from (select date_trunc('week', disb_dt) as week, cand_id, sum(disb_amt) as total from expend where date_part('year', disb_dt) > 2008 and group by week, cand_id order by cand_id, week) as byweek left join house as h on h.candid = byweek.cand_id;"""
