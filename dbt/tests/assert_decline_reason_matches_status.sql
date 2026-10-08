-- Declined <=> has a decline_reason. Approved rows must not carry one.
select transaction_id, status, decline_reason
from {{ ref('stg_transactions') }}
where (status = 'declined' and decline_reason is null)
   or (status = 'approved' and decline_reason is not null)
