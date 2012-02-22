create index expend_commid on expend(cmte_id);
create index expend_candid on expend(cand_id);
create index expend_state on expend(recipient_st);
create index expend_dt on expend(disb_dt);

create index contrib_commid on contrib(cmte_id);
create index contrib_candid on contrib(cand_id);
create index contrib_contrid on contrib(contbr_nm);
create index contrib_state on contrib(contbr_st);
create index contrib_emp on contrib(contbr_employer);
create index contrib_occ on contrib(contbr_occupation);
create index contrib_dt on contrib(contb_receipt_dt);
create index contrib_desc on contrib(receipt_desc);


create index pac_name on pac(name);
create index pac_id on pac(id);
create index pac_dt on pac(enddate);
create index pac_type on pac(type);