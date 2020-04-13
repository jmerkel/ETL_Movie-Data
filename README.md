# ETL - Movie Data
### UC Berkeley - jmerkel_Module_8

This ETL pipelines takes 3 different data sources (2 CSV files & 1 JSON file), cleans each submission, transforms the data into more useful or proper formats, and then loads into two tables on a SQL server. In order for this pipeline to continue to function, various assumptions are being made. The following are some of the major expectations of each data source.

1. Data is expected to be in the same column format between the three input files
2. SQL loading is designed to be appended. Tables should be cleared between each ETL job.
3. The same sorts of data errors are expected to be encountered.
4. The location of each file source is assumed to be in the same directory with the same names
5. The data gaps are expected to be similar between future imports (i.e. Kaggle information is more complete than wiki).
6. Similar datetimes formats will be used.
