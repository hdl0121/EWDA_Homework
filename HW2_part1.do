****************************************************************
****************************************************************
**********In this part, the code is to input the data,**********
*********merge the data, calculate the median multipliers*******
********and save the data for further empirical analysis********
****************************************************************
****************************************************************
set more off
global root_path "D:\GSM\Courses\2018_spring\Empirical Research Workflow and Data Analyses\HW2\data_code_0323"
log using "$root_path\output\log_part_1.log",replace
****************************************************************
****************************************************************
**********1. input, merge and select the valid dataset**********
****************************************************************
****************************************************************
cd "$root_path\raw_data"

*import the firm level compustat data***************************
use "1_firm_data_compustat.dta",clear
rename fyear year
drop indfmt consol popsrc datafmt curcd costat
*drop the firms with total sales<20 million
drop if sale <20
*drop the firms in financial industries
drop if substr(sic,1,1)=="6"
*merge with ccm for permno
joinby gvkey datadate using "ccm.dta"
tempfile firm_compustat
save "`firm_compustat'"
*import the firm level crsp data********************************
use "2_firm_data_crsp.dta",clear
*keep the year data
gen year=year(date)
gen month=month(date)
keep if month==12
drop month date
*keep the positive price
replace prc=abs(prc)
*calculate the end-year market value in millions
gen mv=prc*shrout/1000
drop prc shrout
tempfile firm_crsp
save `firm_crsp'
*merge the crsp and compustat***********************************
use `firm_compustat',clear
joinby permno year using `firm_crsp'
*drop the missing value
egen miss=rowmiss(_all)
keep if miss==0
drop miss
gen capital=mv+dltt
save "$root_path\intermediate\firm_data.dta",replace


************import the segment level data***********************
use "3_segment_data.dta",clear
*drop segments with no sic code
drop if sics1==""
*drop the firms containing financial segments
gen fin_seg=0
replace fin_seg=1 if substr(sics1,1,1)=="6"
bysort gvkey datadate: egen fin_firm=total(fin_seg)
keep if fin_firm==0
drop fin_seg fin_firm
*drop the duplicates record
drop stype sid srcdate
duplicates drop
bysort gvkey datadate : gen sid=_n
*calculate the total sales of segments 
bysort gvkey datadate : egen total_sales=total(sales)
*add the prefix "seg_" to each variable
rename * seg_*
rename (seg_gvkey seg_datadate) (gvkey datadate)
tempfile seg_compustat
save `seg_compustat'
************merge the firm-level and segment-level data*********
use "$root_path\intermediate\firm_data.dta",clear
joinby gvkey datadate using "`seg_compustat'"
*drop the segments without sales, assets capital expenditure
egen miss=rowmiss(seg_sales seg_ias seg_capxs)
keep if miss==0
drop miss
*keep data whose sale deviation is in the 1% range
drop if abs((seg_total_sales-sale)/sale)>0.01
*calculate the number of segments (different industry) in a firm
bysort gvkey datadate seg_sics1 : gen seg_id=_n 
bysort gvkey datadate : egen seg_count=count(seg_id) if seg_id==1
bysort gvkey datadate : egen seg_num=mean(seg_count)
drop seg_count seg_id
*in the 2-digit level, define the related & unrelated segments
gen dig_2_sics=substr(seg_sics1,1,2)
bysort gvkey datadate dig_2_sics : gen un_seg_id=_n
bysort gvkey datadate : egen un_seg_count=count(un_seg_id) if un_seg_id==1
bysort gvkey datadate : egen un_seg_num=mean(un_seg_count)
drop un_seg_count un_seg_id dig_2_sics
*define an indicator whether the firm is diversify or not
gen diversify=0
replace diversify=1 if seg_num>=2
*calculate the number of related-segments
gen relate_seg=seg_num-un_seg_num
save "$root_path\intermediate\firm_seg_data.dta",replace


****************************************************************
****************************************************************
************2. imputed values calculation***********************
****************************************************************
****************************************************************
cd "$root_path\intermediate"
use "firm_seg_data.dta",clear
*the multiplier is based on single-segment firms
keep if diversify==0
****************************************************************
**************calculate the median multiplier*******************
****************************************************************
*calculate the individual multiplier
*the following are for imputed firm value
gen sale_multip=capital/sale
gen asset_multip=capital/at
gen ebit_multip=capital/ebit
gen ebitd_multip=capital/ebitd
*the following are for profitability measure
gen ebit_sale=ebit/sale
gen ebit_asset=ebit/at
*the following are for leverage and tax measure
gen lev=dltt/at
gen tax_ebit=txt/ebit
*drop the negative ebit segment for tax_ebit
replace tax_ebit=. if ebit<0
*generate 2-, 3-digit sic
gen dig_2_sic=substr(sic,1,2)
gen dig_3_sic=substr(sic,1,3)
rename sic dig_4_sic
*calculate the 2-,3- and 4-digit sic industry median multiplier
*and the number of firms in each industry
forvalues i=2/4{
	foreach variable in sale_multip asset_multip ebit_multip ///
	ebitd_multip ebit_sale ebit_asset lev tax_ebit txt{
		bysort dig_`i'_sic year: egen `variable'_`i'=median(`variable')
	}
	bysort dig_`i'_sic year : gen dig_`i'_num=_N
}

*calculate the narrowest sic
gen narrow_sic=dig_4_sic
replace narrow_sic=dig_3_sic if dig_4_num<5
replace narrow_sic=dig_2_sic if dig_3_num<5
*drop if the narrowest sic has less than 5 firms
drop if dig_2_num<5
*calculate the median multiplier for narrowest sic
foreach variable in sale_multip asset_multip ebit_multip ebitd_multip ///
ebit_sale ebit_asset lev tax_ebit txt{
	gen `variable'_med=`variable'_4
	replace `variable'_med=`variable'_3 if dig_4_num<5
	replace `variable'_med=`variable'_2 if dig_3_num<5
}
duplicates drop year narrow_sic,force
keep year narrow_sic sale_multip_med asset_multip_med ebit_multip_med ///
	 ebitd_multip_med ebit_sale_med ebit_asset_med lev_med tax_ebit_med txt_med
*generate some variables for later merge
gen sic=narrow_sic
gen dig_3_sic=narrow_sic
gen dig_2_sic=narrow_sic
save "$root_path\intermediate\multiplier",replace





****************************************************************
**************merge the median multiplier***********************
****************************************************************
cd "$root_path\intermediate"
use "firm_seg_data.dta",clear
*generate 2-,3-digit sic
gen dig_2_sic=substr(sic,1,2)
gen dig_3_sic=substr(sic,1,3)
*merge from the most accurate sic code because merge can update
*the missing values
*merge the 4-digit sic
merge m:1 sic year using "multiplier", gen(dig_4_merge) ///
keepusing(sale_multip_med asset_multip_med ebit_multip_med /// 
	ebitd_multip_med ebit_sale_med ebit_asset_med lev_med ///
	tax_ebit_med txt_med)
*drop the data from using only 
drop if dig_4_merge==2
*merge the 3-digit sic and update the unmerged missing
*in 4-digit merge
merge m:1 dig_3_sic year using "multiplier", gen(dig_3_merge) update ///
keepusing(sale_multip_med asset_multip_med ebit_multip_med /// 
	ebitd_multip_med ebit_sale_med ebit_asset_med lev_med ///
	tax_ebit_med txt_med)
*drop the data from using only 
drop if dig_3_merge==2
*merge the 2-digit sic and update the unmerged missing
*in 3-digit merge and 4-digit merge
merge m:1 dig_2_sic year using "multiplier", gen(dig_2_merge) update ///
keepusing(sale_multip_med asset_multip_med ebit_multip_med /// 
	ebitd_multip_med ebit_sale_med ebit_asset_med lev_med ///
	tax_ebit_med txt_med)
*drop the data from using only 
drop if dig_2_merge==2
*drop the unmatched items
keep if ((dig_4_merge==3) |(dig_3_merge==4) |(dig_2_merge==4))
drop dig_4_merge dig_3_merge dig_2_merge
save "$root_path\intermediate\firm_segment_multiplier.dta",replace


****************************************************************
**************calculate the imputed and excess value************
****************************************************************
cd "$root_path\intermediate"
use "firm_segment_multiplier.dta",clear
*calculate the imputed value
gen seg_ebit=seg_oiadps
gen sale_imput=seg_sales*sale_multip_med
gen asset_imput=seg_ias*asset_multip_med
gen ebit_imput=seg_ebit*ebit_multip_med
gen ebitd_imput=seg_oibdps*ebitd_multip_med
gen debt_imput=seg_ias*lev_med

*industry-adjusted profitability
gen indus_ebit_sale=seg_ebit/seg_sales-ebit_sale_med
gen indus_ebit_asset=seg_ebit/seg_ias-ebit_asset_med

gen tax_imput=seg_ebit*tax_ebit_med
*replace negative imputed tax for 0
replace tax_imput=0 if tax_imput<0 
gen indus_tax=tax_imput-txt_med

*calculate the total imputed value
bysort gvkey year : egen sale_imput_total=total(sale_imput)
bysort gvkey year : egen asset_imput_total=total(asset_imput)
bysort gvkey year : egen ebit_imput_total=total(ebit_imput)
bysort gvkey year : egen ebitd_imput_total=total(ebitd_imput)

*deal with firms having abnormal asset values
bysort gvkey year : egen seg_asset_total=total(seg_ias)
gen asset_deviation=(seg_asset_total-at)/at
replace asset_imput_total=. if abs(asset_deviation)>0.25
replace asset_imput_total=asset_imput_total*(1+asset_deviation) ///
		if abs(asset_deviation)<=0.25

*replace the negative CF 
replace ebit_imput_total=ebitd_imput_total if ebit_imput_total<0
replace ebit_imput_total=sale_imput_total if ebit_imput_total<0
*deal with firms having abnormal ebit
bysort gvkey year : egen seg_ebit_total=total(seg_ebit)
gen ebit_deviation=(seg_ebit_total-ebit)/ebit
replace ebit_imput_total=. if abs(ebit_deviation)>0.25
replace ebit_imput_total=ebit_imput_total*(1+ebit_deviation) ///
		if abs(ebit_deviation)<=0.25

*replace imputed debt for asset deviation
replace debt_imput=debt_imput*(1+asset_deviation)
gen seg_lev=debt_imput/seg_ias
gen indus_lev=seg_lev-lev_med


*calculate the excess_value
gen excess_sale=log(capital/sale_imput_total)
gen excess_asset=log(capital/asset_imput_total)
gen excess_ebit=log(capital/ebit_imput_total)
*drop the extreme values
foreach var in excess_sale excess_asset excess_ebit{
	replace `var'=. if abs(`var')>1.386
}
save "imputed.dta",replace



****************************************************************
**************calculate the condition median multiplier*********
****************************************************************
use "firm_seg_data.dta",clear
*the multiplier is based on single-segment firms
keep if diversify==0
gen sale_multip=capital/sale
gen asset_multip=capital/at
gen ebit_positive=1
replace ebit_positive=0 if ebit<0
*generate 2-, 3-digit sic
gen dig_2_sic=substr(sic,1,2)
gen dig_3_sic=substr(sic,1,3)
rename sic dig_4_sic
*calculate the 2-,3- and 4-digit sic industry median multiplier
*and the number of firms in each industry
forvalues i=2/4{
	*the condition excess value is based on whether ebit is positive 
	foreach variable in sale_multip asset_multip{
		bysort dig_`i'_sic year ebit_positive: ///
		 egen `variable'_`i'=median(`variable')
	}
	bysort dig_`i'_sic year ebit_positive: gen dig_`i'_num=_N
}
*calculate the narrowest sic
gen narrow_sic=dig_4_sic
replace narrow_sic=dig_3_sic if dig_4_num<5
replace narrow_sic=dig_2_sic if dig_3_num<5
*calculate the median multiplier for narrowest sic
foreach variable in asset_multip sale_multip{
	gen `variable'_cdt_med=`variable'_4
	replace `variable'_cdt_med=`variable'_3 if dig_4_num<5
	replace `variable'_cdt_med=`variable'_2 if dig_3_num<5
}
duplicates drop year narrow_sic ebit_positive,force
keep year narrow_sic ebit_positive asset_multip_cdt_med sale_multip_cdt_med
*generate some variables for later merge
gen sic=narrow_sic
gen dig_3_sic=narrow_sic
gen dig_2_sic=narrow_sic
save "$root_path\intermediate\condition_multiplier",replace

****************************************************************
********merge and calculate the condition excess value**********
****************************************************************
cd "$root_path\intermediate"
use "firm_seg_data.dta",clear
*generate 2-,3-digit sic
gen dig_2_sic=substr(sic,1,2)
gen dig_3_sic=substr(sic,1,3)
gen seg_ebit=seg_oibdps-seg_dps
gen ebit_positive=1
replace ebit_positive=0 if seg_ebit<0
*merge from the most accurate sic code because merge can update
*the missing values
*merge the 4-digit sic
merge m:1 sic year ebit_positive using "condition_multiplier", gen(dig_4_merge) ///
keepusing(asset_multip_cdt_med sale_multip_cdt_med ebit_positive)
*drop the data from using only 
drop if dig_4_merge==2
*merge the 3-digit sic and update the unmerged missing
*in 4-digit merge
merge m:1 dig_3_sic year ebit_positive using "condition_multiplier", gen(dig_3_merge) update ///
keepusing(asset_multip_cdt_med sale_multip_cdt_med ebit_positive)
*drop the data from using only 
drop if dig_3_merge==2
*merge the 2-digit sic and update the unmerged missing
*in 3-digit merge and 4-digit merge
merge m:1 dig_2_sic year ebit_positive using "condition_multiplier", gen(dig_2_merge) update ///
keepusing(asset_multip_cdt_med sale_multip_cdt_med ebit_positive)
*drop the data from using only 
drop if dig_2_merge==2
*drop the unmatched items
keep if ((dig_4_merge==3) |(dig_3_merge==4) |(dig_2_merge==4))
drop dig_4_merge dig_3_merge dig_2_merge
save "$root_path\intermediate\firm_segment_condition_multiplier.dta",replace


*calculate the imputed value
use "firm_segment_condition_multiplier.dta",clear
gen sale_cdt_imput=seg_sales*sale_multip_cdt_med
gen asset_cdt_imput=seg_ias*asset_multip_cdt_med
*calculate the total imputed value
bysort gvkey year : egen sale_cdt_imput_total=total(sale_cdt_imput)
bysort gvkey year : egen asset_cdt_imput_total=total(asset_cdt_imput)
gen sale_cdt_excess=log(capital/sale_cdt_imput_total)
gen asset_cdt_excess=log(capital/asset_cdt_imput_total)
save "condition_imputed.dta",replace
log close
