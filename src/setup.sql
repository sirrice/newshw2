drop table pac;
create table pac (id varchar(100),
name varchar(100),
type varchar(100),
desig varchar(100),
ff varchar(100),
totalreceipts float,
transfersfromaffiliates float,
contributionsreceivedindividuals float,
contributionsreceivedorpoliticalcommittees float,
contributionscandidate float,
candidateloan float,
totalloansreceived float,
totaldisbursements float,
transferstoaffiliates float,
refundstoindividuals float,
refundstoorpoliticalcommittees float,
candidateloanrepayments float,
loanrepayments float,
cashbeginningofyear1 float,
cashcloseofperiod float,
debtsowedby float,
nonfederaltransfersreceived float,
contributionsmadetoorcommittees float,
independentexpendituresmade float,
partycoordinatedexpendituresmade float,
nonfederalshareofexpenditures float,
enddate date,
year float);
drop table house;
create table house (candid varchar(100),
candname varchar(100),
incumbent varchar(100),
party varchar(100),
desig varchar(100),
totalrecipts varchar(100),
auth_xfer_from varchar(100),
totaldisbursments varchar(100),
auth_xfer_to varchar(100),
cash_start varchar(100),
cash_end varchar(100),
contrib_self varchar(100),
loans_self varchar(100),
loans_other varchar(100),
repayments_self varchar(100),
repayments_other varchar(100),
totaldebt varchar(100),
contrib_indiv varchar(100),
state varchar(100),
district varchar(100),
status_special varchar(100),
status_primary varchar(100),
status_runoff varchar(100),
status_general varchar(100),
perc_general varchar(100),
contrib_otherparty varchar(100),
contrib_ownparty varchar(100),
enddate varchar(100),
refunds_indiv varchar(100),
refunds_committees varchar(100),
year varchar(100));
drop table expend;
create table expend (cmte_id varchar(100),
cand_id varchar(100),
recipient_nm varchar(100),
disb_amt float,
disb_dt date,
recipient_city varchar(100),
recipient_st varchar(100),
recipient_zip float,
disb_desc varchar(100),
memo_cd varchar(100),
memo_text varchar(100),
form_tp varchar(100),
fil float,
year float);
drop table contrib;
create table contrib (cmte_id varchar(100),
cand_id varchar(100),
cand_nm varchar(100),
contbr_nm varchar(100),
contbr_city varchar(100),
contbr_st varchar(100),
contbr_zip float,
contbr_employer varchar(100),
contbr_occupation varchar(100),
contb_receipt_amt float,
contb_receipt_dt date,
receipt_desc varchar(100),
memo_cd varchar(100),
memo_text varchar(100),
form_tp varchar(100),
file_num float,
year float);
copy pac from '/Users/sirrice/mitnotes/courses/news/hw2/FECWEB/pac_2012.csv' with csv;
copy pac from '/Users/sirrice/mitnotes/courses/news/hw2/FECWEB/pac_2010.csv' with csv;
copy house from '/Users/sirrice/mitnotes/courses/news/hw2/FECWEB/house_2012.csv' with csv;
copy expend from '/Users/sirrice/mitnotes/courses/news/hw2/FECWEB/expend_2012.csv' with csv;
copy contrib from '/Users/sirrice/mitnotes/courses/news/hw2/FECWEB/contrib_2012.csv' with csv;