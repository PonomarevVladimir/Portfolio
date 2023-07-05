delete from cdm.dm_courier_ledger a 
where a.settlement_year =extract(year from now()- interval '1 day') and a.settlement_month = extract(month from now()- interval '1 day');

with c as (select id, courier_name from dds.dm_couriers), 
t as 	  (select distinct id, year, month from dds.dm_timestamps), 
d as 	  (select courier_id, t.year, t.month, count(1) as ord_count, sum(sum) as ord_total_sum, sum(tip) as cour_tips_sum, avg(rate) as rate 
	       from dds.dm_deliveries as dd 
	       left join t on dd.time_id = t.id 
	       group by courier_id, t.year, t.month), 
a as	   (select c.id as c_id, c.courier_name, d.year, d.month, d.ord_count, d.ord_total_sum, d.rate, d.ord_total_sum/4 as ord_proc_fee, 
		    case 
			    when d.rate<4 then GREATEST(100, d.ord_total_sum*0.05) 
			    when d.rate<4.5 and d.rate>=4 then GREATEST(150, d.ord_total_sum*0.07) 
			    when d.rate<4.9 and d.rate>=4.5 then GREATEST(175, d.ord_total_sum*0.08) 
			    when d.rate>=4.9 then GREATEST(200, d.ord_total_sum*0.1) 
			end as cour_ord_sum, 
			d.cour_tips_sum 
			from d 
			left join c on d.courier_id = c.id)  
insert into cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
select a.c_id, a.courier_name, a.year, a.month, a.ord_count, a.ord_total_sum, a.rate, a.ord_proc_fee, a.cour_ord_sum, a.cour_tips_sum, a.cour_ord_sum+a.cour_tips_sum*0.95 from a 
where year = extract(year from now()- interval '1 day') and
month = extract(month from now()- interval '1 day');

select * from cdm.dm_courier_ledger order by id desc limit 10;