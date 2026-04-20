with comparison as (
    select
        d.respondent_key,
        d.day_1_protein_g,
        p.day_1_item_protein_g
    from {{ ref('int_nhanes__respondent_daily_nutrients') }} as d
    left join {{ ref('int_nhanes__respondent_protein_source_estimates') }} as p
        using (respondent_key)
    where d.day_1_protein_g is not null
      and p.day_1_item_protein_g is not null
)

select *
from comparison
where abs(day_1_protein_g - day_1_item_protein_g) > 0.1
