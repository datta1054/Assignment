The Scenario 
The objective of this project is to design and implement a data pipeline that extracts data from Kaggle, transforms it into dimension and fact tables, and loads it into a SQLite database. Additionally, a report will need to be generated based on the dimension and fact tables created. Lastly, draft a solution for deploying this data pipeline and report to the cloud. 
Tasks 
1. Data Extraction: 
○ Use Python for data processing. 
i. Pick any framework. 
○ Extract data from: 
i.https://www.kaggle.com/datasets/faresashraf1001/supermarket-sales/data

ii. Use the Kaggle API Python package to pull the data: 
1. https://github.com/Kaggle/kaggle-api

2. Schema Design: 
○ Identify two dimensions from the dataset. 
○ Define the schema for the two dimension tables and one fact table. 3. Transform and Load Data: 
○ Transform the extracted data into two dimension tables and one fact table. ○ Load the transformed data into a SQLite database. 

4. Reporting: 
○ Generate an analytical report by aggregating the data from the dimension and fact tables you created. Solution must include: 
i. Expected output is a SQL Query and the resulting dataset
                ii. Should include joins to your dimensions as well as windowing 
functions 
5. Draft a Solution for Cloud Deployment: 
○ Design a cloud solution to fully automate the data pipeline and report generation process. 
○ Identify the necessary cloud components. 
i. Assume you will not be using SQLite in the cloud. 
○ Create an architecture diagram to represent the entire workflow and be prepared to discuss. 
Deliverables 

● Jupyter notebook or python scripts that encapsulate the tasks above
● Code should be loaded into a git repository and shared with provided users 
● Python code for data extraction, transformation, and loading. 
● SQL script(s) for creating the tables. 
● SQL script for report generation. 
● Cloud architecture diagram representing a solution for deploying the data pipeline and report generation process to the cloud. 
