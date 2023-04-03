# Assessment Index Modelling

This Code Assessment simulates the scenario of building an index model based on a given guideline. 
The rules provided below describe a simple stock index and for this assessment the objective is to 
build a model that can calculate such index values.

The idea is not only that you can show us what you've got, but also that you can get a first impression 
of what it means to build an index model.

The required input prices can be found at `data_sources\stock_prices.csv`. 

You can also find the correct (rounded) index level at `data_sources\index_level_results_rounded.csv`. 
Please note these values cannot be used as an input but solely act as a reference for you to verify 
your model. For the evaluation of your model we will use the high precision index values your model 
calculates.


# Index Notes

- The index is a stock index made up of imaginary stocks. 
- There are no further corporate actions to consider here 
- The index doesn't resemble any real existing index.
- The model should be able to calculate the index levels based on the rules below.
- All provided prices are total return. 
- All companies got the same amount of shares outstanding.


# Index Rules

- The index is a total return index.
- The index universe consists of all stocks from "Stock_A" to including "Stock_J".
- Every first business day of a month the index selects from the universe the top three stocks based on their market capitalization, 
  based on the close of business values as of the last business day of the immediately preceding month.
- The selected stock with the highest market capitalization gets assigned a 50% weight, while the second and third each 
  get assigned 25%.
- The selection becomes effective close of business on the first business date of each month.
- The index starts with a level of 100.
- The index start date is January 1st 2020.
- The index business days are Monday to Friday.
- There are no additional holidays.

# Setup
We've already provided a `__main__.py` for this project and there should be no need for you to modify it. 
As you will see the main file expects an index model which can be found at `index_model/index.py`. 
This is the model we expect you to build. It should then ultimately be able to export the 
calculated values to a csv file. 

If you want to use additional packages feel free to do so. The actual business logic must be part of 
your repo, but we actively encourage you to use packages that you're already familiar 
with. Please make sure to update the requirements.txt file accordingly.

# Submission
We are happy to see your project. If not already agreed otherwise, please send us your project 
as a link to your GitHub repo via mail to jobs@solactive.com. 

Please make sure that you clone the project instead of forking it.
