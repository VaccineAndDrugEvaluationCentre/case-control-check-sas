/*~
# Summary
Record number of records/cases/controls change after each data/proc step. Check for duplicates in cases and controls.

# Parameters
	macro_num= Start with 1 and provide number in the increment of 1 for each macro call.
	case_definition= Variables which defines a case i.e is_case = 1.
	case_change_reason= If you know that number of cases will change from last step, please put a reason for future reference. Use %str.
	case_primary_key= Variables which define a unique case i.e scrphin index_date. It will be used in conjunction with case_definition parameter.
	control_definition= Variables which defines a control i.e is_case = 0. Use %str. Optional.
	control_change_reason= If you know that number of controls will change from last step, please put a reason for future reference. Optional.
	control_primary_key= Variables which define a unique control i.e scrphin index_date. It will be used in conjunction with control_definition parameter. Optional.
	filename_prefix=Name of output file and should be same in each call of the macro within same project. Datetime timestamp will be suffixed automatically. No special characters, spaces, hyphen. Underscore "_" is preferred.
	file_location=Location of the output file and should be same in each call of the macro within same project.

# Notes
This macro will create a text file with following columns:
	Dataset = Name of the dataset
	No. of cases = Number of cases based on case_definition parameter
	Cases changed = Change in number of cases from previous step
	Cases change reason = Reason of change if provided in case_change_reason parameter
	Cases dups = Duplicates in cases using case_primary_key and case_definition parametres
	No. of controls =  Number of controls based on control_definition parameter
	Controls changed = Change in number of controls from previous step
	Controls change reason = Reason of change if provided in control_change_reason parameter
	Controls dups = Duplicates in controls using control_primary_key and control_definition parametres

# Example 
	%case_control_check(macro_num=1,case_definition=%str(is_case = 1),case_change_reason=first run,case_primary_key=scrphin index_date,control_definition=,control_change_reason=,control_primary_key=,filename_prefix=chk,file_location=P:\VDEC\temp);
	%case_control_check(macro_num=2,case_definition=%str(is_case = 1),case_change_reason=,case_primary_key=scrphin index_date,control_definition=%str(is_case = 0),control_change_reason=first run,control_primary_key=scrphin index_date,filename_prefix=chk,file_location=P:\VDEC\temp);
	
# Version history
* v1.0.0; Gurpreet Pabla, July 2017; Initial version

# Limitations
	This macro works only in the windows environment using Base SAS or SAS Enterprise Guide as it use "pipe" statement.
	
Copyright (c) 2016 Vaccine and Drug Evaluation Centre, Winnipeg. All rights reserved.

~*/

%macro case_control_check(macro_num= ,
						  case_definition= ,
						  case_change_reason= , 
						  case_primary_key= ,
						  control_definition= ,
						  control_change_reason= ,
						  control_primary_key= ,
						  filename_prefix= ,
						  file_location=
);

	%let dsname = &syslast.; /*Identify last dataset name*/
	
	/*Define macro variables*/
	%let _n_cases_&filename_prefix._0 = 0;
	%let _n_cntrol_&filename_prefix._0 = 0;
	%let prev_num = %eval(&macro_num-1);
	%let prev_fl_flag = 0;
	%let curr_macro_num_flag = 0;

	%if not(%symexist(_n_cases_&filename_prefix._&prev_num)) or %symexist(_n_cases_&filename_prefix._&macro_num) %then %do;
		%if not(&macro_num.=1) %then %do;
			%if not(%symexist(_n_cases_&filename_prefix._&prev_num)) %then %do;
				%let prev_fl_flag = 1;
			%end;
			%else %do;
				%let curr_macro_num_flag = 1;
			%end;
			
			/*Find the name of latest previous file*/
			filename temp_prev_fl pipe "dir &file_location.\&filename_prefix.*.txt /od /t:w /b";
			data _null_;
				infile temp_prev_fl ;
				input;
				call symputx('latest',_infile_);
			run;
			
			%if not(%symexist(latest)) %then %do;
				%put ERROR: Parameter macro_num should be 1 as no file with prefix "&filename_prefix." exists at the location provided.;
			%end;
			
			/*Read in latest previous file*/
			data _temp_prev_fl;
			infile "&file_location.\&latest." missover pad firstobs=2 obs=&macro_num;
			input 	@1	dataset_name $49.
					@51	cases	12.
					@64 changed_cases 13.
					@78 Reason_cases $30.
					@109 duplicates_in_cases 10.
					@120 controls 15. 
					@136 changed_controls 16.
					@153 Reason_controls $30.
					@184 duplicates_in_controls 11.
			;
			if dataset_name = "" then delete;
			call symputx("_n_cases_&filename_prefix._&prev_num",cases);
			call symputx("_n_cntrol_&filename_prefix._&prev_num",controls);
			run;
		%end;
	%end;

	%global _n_cases_&filename_prefix._&macro_num _n_cntrol_&filename_prefix._&macro_num;
	%let _n_cases_&filename_prefix._&macro_num = 0;
	%let _num_cases_changed = 0;

	%let _n_cntrol_&filename_prefix._&macro_num = 0;
	%let _num_controls_changed = 0;

	/*cases*/
	proc sql noprint;
	select count(*) into :_n_cases_&filename_prefix._&macro_num from &dsname where &case_definition.;
	quit;

	%let _num_cases_changed = %eval(&&_n_cases_&filename_prefix._&macro_num - &&_n_cases_&filename_prefix._&prev_num);

	proc sort data=&dsname out=_temp_dups_chk_cases dupout=_temp_dups_cases nodupkey; by &case_primary_key.; where &case_definition.;
	run;

	proc sql noprint;
	select count(*) into :_num_dup_cases_&macro_num from _temp_dups_cases;
	quit;

	/*controls*/
	%if %sysevalf(%superq(control_definition)=,boolean) = 0 %then %do;
		proc sql noprint;
		select count(*) into :_n_cntrol_&filename_prefix._&macro_num from &dsname where &control_definition.;
		quit;

		%let _num_controls_changed = %eval(&&_n_cntrol_&filename_prefix._&macro_num - &&_n_cntrol_&filename_prefix._&prev_num.);

		proc sort data=&dsname out=_temp_dups_chk_controls dupout=_temp_dups_controls nodupkey; by &control_primary_key.; where &control_definition.;
		run;

		proc sql noprint;
		select count(*) into :_num_dup_controls_&macro_num from _temp_dups_controls;
		quit;
	%end;
	%else %do;
		%let _n_cntrol_&filename_prefix._&macro_num = 0;
		%let _num_controls_changed = 0;
		%let _num_dup_controls_&macro_num = 0;
		%let control_change_reason = ;
	%end;

	%if &macro_num = 1 %then %do;
		%let _num_controls_changed = 0;
		%let _num_cases_changed = 0;
	%end;

	%if &macro_num = 1 or &prev_fl_flag = 1 or &curr_macro_num_flag = 1 %then %do;
		%if &curr_macro_num_flag=0 %then %do;
			filename temp_out "&file_location.\&filename_prefix._%sysfunc(today(),date7.)%sysfunc(compress(%sysfunc(time(),time8.),:)).txt";
		%end;
		data _null_;
		file temp_out;
		put @1	"Dataset" 
			@50 "|"
			@51	"No. of cases"
			@63 "|"
			@64 "Cases changed"
			@77 "|"
			@78 "Cases change reason"
			@108 "|"
			@109 "Cases dups"
			@119 "|"
			@120 "No. of controls"
			@135 "|"
			@136 "Controls changed"
			@152 "|"
			@153 "Controls change reason"
			@183 "|"
			@184 "Controls dups"
			@197 "|"
			;
		run;

		proc sql noprint;
		create table tracker_ds
			(dataset_name char(49)
			,cases num format=12.
			,changed_cases num format=13.
			,Reason_cases char(30)
			,duplicates_in_cases num format=10.
			,controls num format=15.
			,changed_controls num format=16.
			,Reason_controls char(30)
			,duplicates_in_controls num format=11.
		);
		quit;

		%if &prev_fl_flag = 1 or &curr_macro_num_flag = 1 %then %do;
			data tracker_ds;
			set _temp_prev_fl;
			run;
			
			data _null_;
			set _temp_prev_fl;
			file temp_out mod;
			put @1	dataset_name 
				@50 "|"
				@51	cases
				@63 "|"
				@64 changed_cases
				@77 "|"
				@78 Reason_cases
				@108 "|"
				@109 duplicates_in_cases
				@119 "|"
				@120 controls
				@135 "|"
				@136 changed_controls
				@152 "|"
				@153 Reason_controls
				@183 "|"
				@184 duplicates_in_controls
				@197 "|"
			;
			run;


		%end;
	%end;

	proc sql noprint;
	insert into tracker_ds
	values("&dsname.",&&_n_cases_&filename_prefix._&macro_num,&_num_cases_changed,"&case_change_reason",&&_num_dup_cases_&macro_num.,&&_n_cntrol_&filename_prefix._&macro_num,&_num_controls_changed.,"&control_change_reason",&&_num_dup_controls_&macro_num.);
	quit;

	data _null_;
		file temp_out mod;
		cases = &&_n_cases_&filename_prefix._&macro_num;
		changed_cases = &_num_cases_changed;
		duplicates_in_cases = &&_num_dup_cases_&macro_num.;
		controls = &&_n_cntrol_&filename_prefix._&macro_num.;
		changed_controls = &_num_controls_changed.;
		duplicates_in_controls = &&_num_dup_controls_&macro_num.;
		put @1	"&dsname." 
			@50 "|"
			@51	cases
			@63 "|"
			@64 changed_cases
			@77 "|"
			@78 "&case_change_reason"
			@108 "|"
			@109 duplicates_in_cases
			@119 "|"
			@120 controls
			@135 "|"
			@136 changed_controls
			@152 "|"
			@153 "&control_change_reason"
			@183 "|"
			@184 duplicates_in_controls
			@197 "|"
			;
	run;

	proc datasets nolist memtype=data;
	  delete _temp_dups_: _temp_prev_fl ;
	quit;
%mend;
