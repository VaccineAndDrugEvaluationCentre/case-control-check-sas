/*Call the macro and change ouput_file_loc parameter to point to correct library*/
%let ouput_file_loc = C:\data;

/*Create population dataset*/
data population_dtset(keep=Name Team nAtBat YrMajor League Division Position salary);
	set sashelp.baseball;
	where nAtBat >= 200;
run;

/*First call to the macro*/
%case_control_check(macro_num=1,
	case_definition=%str(not missing(name)),
	case_change_reason=Initial run,
	case_primary_key=name,
	filename_prefix=baseball,
	file_location=&ouput_file_loc.);

/*Create case dataset from population dataset*/
data case_dtset;
	set population_dtset;
	where nAtBat >= 500 
	  and YrMajor >= 5;
	is_case = 1; /*Created new variable is_case and assigned value 1 to it*/
run;

/*Second call to the macro*/
%case_control_check(macro_num=2,
	case_definition=%str(is_case=1),
	case_change_reason=Case selection,
	case_primary_key=name,
	filename_prefix=baseball,
	file_location=&ouput_file_loc.);

/*Create controls dataset who are in the same league, division and plays at same position but times at bat (natbat) is less than 500*/ 
proc sql;
	create table control_dtset as
	select a.*, 
		   0 as is_case
	from population_dtset a
		, case_dtset b

		where a.league  = b.league
		  and a.division = b.division
		  and a.Position= b.Position
		  and a.nAtBat < 500
		  and a.name not= b.name
	;
quit;

/*Create cohort*/
data cohort_dtset_1;
	set case_dtset
		control_dtset;
run;

/*Third call to the macro*/
%case_control_check(macro_num=3,
	case_definition=%str(is_case=1),
	case_primary_key=name,
	control_definition=%str(is_case = 0),
	control_change_reason=Conrols selection,
	control_primary_key=name,
	filename_prefix=baseball,
	file_location=&ouput_file_loc.);

/*Remove records with missing salary*/
data cohort_dtset;
	set cohort_dtset_1;
	where not missing(salary);
run;

/*Fourth call to the macro*/
%case_control_check(macro_num=4,
	case_definition=%str(is_case=1),
	case_change_reason=Remove missing salary records,
	case_primary_key=name,
	control_definition=%str(is_case = 0),
	control_change_reason=Remove missing salary records,
	control_primary_key=name,
	filename_prefix=baseball,
	file_location=&ouput_file_loc.);

