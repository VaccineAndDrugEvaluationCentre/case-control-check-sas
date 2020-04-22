# Introduction
This macro (case_control_check) tracks the number of records/cases/controls in a dataset and how those change over the course of a project.    
What are the biggest issues programmer faces when writing scripts?     
Are there any dropped or added records, or are there any duplicates, especially after linking multiple datasets. The SAS log is available, but does not provide all the information. A programmer has to write additional code after each data or proc step. This will be a cumbersome task for bigger projects.    
The purpose of this macro is to report on these issues with minimal amount of extra work. This macro was developed for epidemiological (case-control) studies but has been applied elsewhere.    
