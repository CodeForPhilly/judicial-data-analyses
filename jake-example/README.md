## Some caveats
* `SentenceData` has unique values in the field `SentenceConditions` but all other columns are repeated. It is recommended to remove these columns
* The granularity of the `CaseData` tables is at the Docket Number level:

![](code/quick-analysis/img/docket-number-distribution.png)


## Supplemental Data

### Statute Hierarchy

The `statute_hierarchy.parquet` file provides a hierarchical structure for the `OffenseData` files. 
This file has two sources:

* [`statute_website.csv`][csv-scrape] comes from [this code][code-scrape] and has code that scraped the [Pennsylvania General Assembly Consolidated Statutes][site-scrape] website
* [`statute_manual.csv`][csv-manual] has most of the unconsolidated statutes, some statute titles with very low counts have not been added and comes from [casetext.com][site-manual]


[code-scrape]: code/data-prep/statutes/scrape-html.R
[csv-manual]: code/data-prep/statutes/statute_manual.csv
[csv-scrape]: code/data-prep/statutes/statute_webscrape.csv
[site-manual]: https://www.legis.state.pa.us/cfdocs/legis/LI/Public/cons_index.cfm
[site-scrape]: https://www.legis.state.pa.us/cfdocs/legis/LI/Public/cons_index.cfm


| title number|title text          |chapter number |chapter_text |section number |section_text                          |subsection number |subsection text                           |
|------------:|:-------------------|:--------------|:------------|:--------------|:-------------------------------------|:-----------------|:-----------------------------------------|
|           18|Crimes and offenses |63             |Minors       |6310           |Inducement of minors to buy liquor... |1                 |Selling or furnishing liquor or malt o... |
|           18|Crimes and offenses |63             |Minors       |6310           |Inducement of minors to buy liquor... |2                 |Manufacture or sale of false identific    |
|           18|Crimes and offenses |63             |Minors       |6310           |Inducement of minors to buy liquor... |3                 |Carrying a false identification card      |


## Examples

### Analysis
**Comparison of judges for one specific type of offense**

![](code/quick-analysis/img/demographics-by-judge.png)

**How the section hierarchy can be grouped into title / chapter**

![](code/quick-analysis/img/treemap-all-offenses.png)

**How the section hierarchy can be grouped into chapter / section**

![](code/quick-analysis/img/treemap-title-18-offenses.png)


### Code

An example of combining sentencing data with demographics and offense descriptions

```sql
select
    cd.docket_number,
    cd.otn,
    cd.originating_docket_number,
    (sd.sentence_date::date - cd.defendant_dob::date) / 365.25 as age_at_sentencing,
    cd.defendant_gender,
    cd.defendant_race,
    cd.defendant_zip_code,
    cd.county,
    sd.disposing_judge,
    sd.sentence_date,
    od.title || '_' || od.section || coalesce('_' || od.sub_section, '') as title_section,
    od.title,
    sh.title_text,
    sh.chapter_number,
    sh.chapter_text,
    od.section,
    sh.section_text,
    od.sub_section,
    od.description,
    od.lead_offense_indicator,
    od.offense_grade,
    sd.offense_disposition,
    sd.program_type,
    sd.sentence_type,
    (sd.min_years * 12)::int + sd.min_months + (sd.min_days / 30)::int as total_min_months,
    (sd.max_years * 12)::int + sd.max_months + (sd.max_days / 30)::int as total_max_months
  from
    -- each case, with demographics
    cpcms_case_data                 cd
    -- add deduped sentencing data
    inner join cpcms_sentence_data  sd on sd.docket_number = cd.docket_number
    -- add offense
    inner join cpcms_offense_data   od 
       on od.docket_number = sd.docket_number 
       and od.offense_sequence_number = sd.offense_sequence_number
       and od.title not in ('129-64', 'CO', 'CP', 'FC', 'LO', 'Migrated', 'Migration')
    -- add section hierarchy groupers
    inner join statute_hierarchy    sh 
       on sh.title_number::int = od.title::int
```

An example of finding indicators and aggregating data to the case level

```sql
with 
money_owed as (
    select 
        docket_number,
        sum(assessed_amount + adjusted_amount) as total_money_owed,
        sum(balance) as current_balance
    from cpcms_financial_data
    where assessment_category in ('Restitution', 'Fines') -- excludes 'Costs/Fees'
    group by docket_number
),

bail as (
    select
        docket_number,
        max(bail_set_amount) as bail_amount
    from cpcms_bail_action_data
    where bail_action = 'Set'
    group by docket_number
),

docket_indicators as (
  select 
      cd.docket_number,
      min(sd.sentence_date) as first_sentence_date,
      max(if(ad.representation_type = 'Public Defender', 1, 0)) as public_defender_ind,
      max(if(sd.case_disposition = 'Guilty Plea - Negotiated', 1, 0)) as guilty_plea_ind,
      max(if(sd.sentence_type = 'Probation', 1, 0)) as probation_eligible_ind,
      max(if(cc.confinement_reason = 'Unable to Post Bail', 1, 0)) as unable_to_post_ind,
      max(if(dd.docket_number is not null, 1, 0)) as diversionary_ind
  from
      cpcms_case_data                      cd
      inner join cpcms_sentence_data       sd on sd.docket_number = cd.docket_number
      inner join cpcms_attorney_data       ad on ad.docket_number = cd.docket_number
      left join cpcms_bail_action_data     ba on ba.docket_number = cd.docket_number
      left join mdjs_case_confinement_data cc on cc.docket_number = cd.originating_docket_number
      left join cpcms_diversionary_data    dd on dd.docket_number = cd.docket_number
  group by 
      cd.docket_number
)

select 
    cd.docket_number,
    cd.otn,
    cd.originating_docket_number,
    coalesce(cd.arrest_date, cd.initiation_date) as arrest_date,
    cd.filed_date,
    di.first_sentence_date,
    (coalesce(cd.arrest_date, cd.initiation_date)::date - cd.defendant_dob::date) / 365.25 as age_at_arrest,
    cd.defendant_gender,
    cd.defendant_race,
    cd.defendant_zip_code,
    cd.county,
    mo.total_money_owed,
    mo.current_balance,
    di.public_defender_ind,
    di.guilty_plea_ind,
    di.probation_eligible_ind,
    coalesce(ba.bail_amount, 0) as bail_amount,
    di.unable_to_post_ind,
    di.diversionary_ind
from 
    cpcms_case_data              cd
    inner join docket_indicators di on di.docket_number = cd.docket_number
    left join bail               ba on ba.docket_number = cd.docket_number
    left join money_owed         mo on mo.docket_number = cd.docket_number
where
    cd.arrest_date is not null 
```

