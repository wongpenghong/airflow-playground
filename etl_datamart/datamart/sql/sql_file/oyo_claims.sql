select 
-- standard information datamart revised -- 
policies.number as policy_number 
,transaction_id as transaction_id
,NULL AS RefNo
,policies.booking_date as purchase_time
,policies.created_at as policy_activation_time
,bookings.start_date as policy_started_time
,bookings.end_date as policy_expired_time
,policies.partner_id as partner_id
,partners.name as partner_name
,'Partnership API' as Channel
,'Accomodation' as Category_LOB
,policies.insurance_partner_id as insurance_ID
,insurance_partners.name as insurance_name
,policies.booking_date as recon_time
,covered_users.id as user_id
,policies.id as policy_id
,null as invoice_id
,null as invoice_number
,policies.code as partner_transaction_id
,products_policies.product_price*bookings.room*bookings.days as GWP
,claims.id as claim_id
,claims.number as claim_number
,claims.created_at as claim_submission_time
,case when claim_histories.status = '3.INSURER APPROVED' then  claim_histories.status_update else null end as claim_approved
,claim_histories.status as claim_last_status
,claim_payment_amounts.amount as claim_amount
,benefits.name as claim_benefit
,claim_payment_amounts.created_at as claim_payment_time
,products.name as product_name
-- requested information from user -- 
,insurance_number as insurance_policy_number
,insurance_old_number as insurance_old_policy_number
,booker_name 
,booker_phone
,policies.code as booking_code
,bookings.amount as booking_price
,bookings.days as days
,bookings.room as room
,json_extract_path_text(bookings.details, 'rooms','count') as total_people
,json_extract_path_text(bookings.details, 'rooms','passengerCount','adult') as total_adult
,json_extract_path_text(bookings.details, 'totalBookingPrice') as total_booking_price
,transaction_histories.created_at as payment_time
,json_extract_path_text(bookings.details, 'category') as category
,json_extract_path_text(bookings.details, 'city') as city
,bookings.sales_channel as sales_channel
,bookings.sales_channel_name as sales_channel_name
,policies.status as policy_status
,benefits.threshold as benefit_amount
,claim_histories.status_update as status_time
from 
accommodation_service_production.policies
left join accommodation_service_production.bookings on bookings.id = booking_id
left join accommodation_service_production.products_policies on products_policies.policy_id = policies.id
left join accommodation_service_production.products on products.id = products_policies.product_id
LEFT JOIN accommodation_service_production.covered_users ON covered_users.policy_id = policies.id
 left join user_service_production.partners on policies.partner_id = partners.id
 left join user_service_production.insurance_partners on insurance_partners.id = policies.insurance_partner_id
left join accommodation_integration_production.transaction_histories on policies.transaction_id = transaction_histories.partner_transaction_id
left join datamart.oyo_countries on oyo_countries.code = bookings.country_id 
inner join accommodation_service_production.claims on policies.id = claims.policy_id
left join accommodation_service_production.benefits on benefits.id = claims.benefit_id 
left join 
	(select 
	claim_id,
	case 
				when status = 'REQUEST_DOCUMENT' and note like '%QOALA%' then 'QOALA PROCESS'
				when status like '%REJECT%' then '5.REJECTED'
				when status = 'REQUEST_DOCUMENT' and note like '%INSURER%' then 'INSURANCE PROCESS' 
				when status = 'INITIATED' then '1.INITIATED'
				when status = 'INSURANCE_APPROVED' then '3.INSURER APPROVED'
				when status = 'QOALA_APPROVED' then '2.QOALA VERIFIED'
				when status = 'INSURANCE_REJECTED' then '3.INSURER REJECTED'
				when status = 'PAID' then '4.PAID'
			else status end as status,
	updated_at as status_update
from 
(select 
		claim_id,status,updated_at, note, Row_number() over (partition by claim_id order by updated_at desc)rn
		from accommodation_service_production.claim_histories)claim_histories where rn = 1
)claim_histories on claims.id = claim_histories.claim_id
left join accommodation_service_production.claim_payment_amounts on claim_payment_amounts.claim_id = claims.id
where  
policies.transaction_id not like '%TEST%'
and 
policies.transaction_id not like '%TES%'
and 
policies.transaction_id not like '%OYO-RELEASE%'