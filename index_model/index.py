import datetime as dt
import pandas as pd
import numpy as np
import os

class IndexModel:
    '''
    This Class reads the time series of Stocks and Calculates the index value depending
    on some rules.
    '''
    def __init__(self) -> None:
        self._read_inpfile('data_sources', 'stock_prices.csv')


    def _read_inpfile(self, source='data_sources',filename = 'stock_prices.csv')->None:
        '''
        This function read the data file "filename" from "source folder" in the current working directory
        :param source:
        :param filename:
        :return:
        '''
        try:
            self.stocks_ts = pd.read_csv(os.path.join(os.getcwd(),source, filename))
        except FileNotFoundError:
            print("Data file doesn't exist, Check the path and file.")

        assert isinstance(self.stocks_ts,
                          pd.DataFrame), "The input file is not a dataframe with a Date column and time series of stocks"

        self.stocks_ts['Date'] = pd.to_datetime(self.stocks_ts['Date'], format='%d/%m/%Y')
        self.stocks_ts.set_index('Date', inplace=True)


    def _process_vals(self, row:list) -> float:
        '''
        This function takes the list of top 3 stocks and calcualtes the index value based on
        50% weight to first stock and 25% weight to each of second and third stock.
        :param row: List of top three stocks
        :return:
        '''
        return row.values[0] * 0.50 + row.values[1] * 0.25 + row.values[2] * 0.25


    def calc_index_level(self, start_date: dt.date, end_date: dt.date) -> None:
        '''
        This function calculates the index values in the backtesting period.
        :param start_date: Starting date for backtesting
        :param end_date: Ending date for backtesting
        :return: None
        '''
        backtest_start = pd.Timestamp(start_date)
        backtest_end = pd.Timestamp(end_date)

        temp = self.stocks_ts.asfreq('BM').T.apply(lambda s: pd.Series(s.nlargest(3).index)).T #Top 3 stocks on month end
        temp['combined'] = temp.values.tolist()
        temp = temp['combined']

        self.merged_df = self.stocks_ts.merge(temp, how='outer', left_index=True, right_index=True)
        self.merged_df['combined'] = self.merged_df['combined'].shift(1).fillna(method='ffill')
        self.merged_df = self.merged_df[(self.merged_df.index<=backtest_end) & (self.merged_df.index>=backtest_start)]

        self.merged_df['Index_value'] = self.merged_df.apply(lambda x: self._process_vals(self.merged_df.loc[x.name, x.combined]), axis=1)
        self.merged_df.loc[self.merged_df.index[0], 'Index_value'] = 100.00


    def export_values(self, file_name: str) -> None:
        '''
        This function saves the calculated index values in the csv file "file_name"
        :param file_name: File where calculated index values for the backtesting period are stored.
        :return: None
        '''
        dest_path = os.path.join(os.getcwd(), file_name)
        self.merged_df['Index_value'].to_csv(dest_path)
