import os
import math
import numpy
import Quandl
import pandas
import matplotlib.pyplot as plt


class Downloader:
    def __init__(self, proxy, username, password, server, quandl_auth):
        """
        Initialization method for the Quandl Data Sets downloader
        :param proxy: True / False
        :param username: your username
        :param password: your password
        :param server: the proxy server address
        :param quandl_auth: Quandl authentication token
        """
        self.token = quandl_auth
        self.memoized_data = {}
        self.proxy = proxy
        self.username = username
        self.password = password
        self.server = server

    def get_data_set(self, data_set, start, end, drop=None, collapse="daily", transform="None"):
        """
        Method for downloading one data set from Quandl
        :param data_set: the data set code
        :param start: the start date
        :param end: the end date
        :param drop: which columns to drop
        :param collapse: frequency of the data
        :param transform: any data transformations from quandl
        :return: the data set as a pandas data frame
        """
        if drop is None:
            drop = []
        if self.proxy:
            # If we are running behind the proxy set it up
            os.environ['HTTP_PROXY'] = "http://" + self.username + ":" + self.password + "@" + self.server
        # Check if the dataframe has been downloaded already in this session
        hash_val = hash(data_set + str(start) + str(end) + str(transform))
        if self.memoized_data.__contains__(hash_val):
            return self.memoized_data[hash_val]
        else:
            try:
                print("\tDownloading", data_set)
                # Otherwise download the data frame from scratch
                if transform is not "None":
                    downloaded_data_frame = Quandl.get(data_set, authtoken=self.token, trim_start=start,
                                                       trim_end=end, collapse=collapse, transformation=transform)
                else:
                    downloaded_data_frame = Quandl.get(data_set, authtoken=self.token, trim_start=start,
                                                       trim_end=end, collapse=collapse)
                # Remove any unnecessary columns and rename the columns
                # print downloaded_data_frame.columns
                updated_column_labels = []
                for column_label in downloaded_data_frame.columns:
                    if column_label in drop:
                        downloaded_data_frame = downloaded_data_frame.drop([column_label], axis=1)
                    else:
                        updated_column_labels.append(data_set + "_" + column_label)
                downloaded_data_frame.columns = updated_column_labels
                self.memoized_data[hash_val] = downloaded_data_frame
                return downloaded_data_frame
            except Quandl.DatasetNotFound:
                print("Exception - DataSetNotFound", data_set)
            except Quandl.CodeFormatError:
                print("Exception - CallFormatError", data_set)
            except Quandl.DateNotRecognized:
                print("Exception - DateNotRecognized", data_set)
            except Quandl.ErrorDownloading:
                print("Exception - ErrorDownloading", data_set)
            except Quandl.ParsingError:
                print("Exception - ParsingError", data_set)
            except:
                print("Some other error occurred")

    def get_data_sets(self, data_sets, start, end, drop=None, collapse="daily", transform="None"):
        """
        This is a method for downloading multiple Quandl data-sets and joining them
        :param data_sets: the list of data set codes
        :param start: the start date
        :param end: the end date
        :param drop: which columns to drop
        :param collapse: frequency of the data
        :param transform: any data transformations from quandl
        :return: the data set as a pandas data frame
        :return:
        """
        all_data_sets = None
        for data_set in data_sets:
            downloaded_data_frame = self.get_data_set(data_set, start, end, drop, collapse, transform)
            if all_data_sets is None:
                all_data_sets = downloaded_data_frame
            else:
                if downloaded_data_frame is not None:
                    if not downloaded_data_frame.empty:
                        all_data_sets = all_data_sets.join(downloaded_data_frame, how="outer")
        return all_data_sets


if __name__ == '__main__':
    start_date = "1990-01-01"
    end_date = "2020-01-01"
    my_downloader = Downloader(False, "", "", "", "N9HccV672zuuiU5MUvcq")
    indicators = list(pandas.read_csv("IMF-Indicators.csv")["Indicator"])
    countries = list(pandas.read_csv("ISO-Codes-Africa.csv")["Code"])
    names = list(pandas.read_csv("ISO-Codes-Africa.csv")["Country"])
    j = 0
    for c in countries:
        print("Downloading data for", names[j])
        j += 1
        try:
            c_args = []
            for i in indicators:
                c_args.append("ODA/" + c + "_" + i)
            c_data = my_downloader.get_data_sets(c_args,
                                                 start=start_date,
                                                 end=end_date,
                                                 transform="None",
                                                 drop=[],
                                                 collapse="annual")

            if c_data is not None:
                c_data.to_csv("Africa/" + c + ".csv")
            else:
                print("No data available")
        except:
            print("Exception caught")
            continue