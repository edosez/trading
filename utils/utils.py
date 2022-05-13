import logging
import sys
import os
import json
import errno
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
from logging import Logger
from logging.handlers import TimedRotatingFileHandler


def mkdir_p(path_file):
    """
    Function in order to create all directories, if do no exist, given a path
    source: http://stackoverflow.com/a/600612/190597 (tzot)

    :param path_file: path to use to create all the necessary directory-ies
    :type path_file: str
    :return: empty
    """
    try:
        # Try to create the directories present into the path
        # Will handle if they already exist
        os.makedirs(os.path.dirname(path_file), exist_ok=True)  # Python>3.2
    except TypeError:
        try:
            os.makedirs(os.path.dirname(path_file))
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(os.path.dirname(path_file)):
                pass
            else:
                raise


def model_folder_nb(path_folder):
    """
    Function to return the number to use for new model directory

    It will find out what is the current max model folder number and add 1 to it

    :param path_folder: string for the folder path to evaluate
    :type path_folder: str
    :return: integer depicting the new model number
    """
    try:
        try:
            # Convert the folder list into an integer array
            list_dir = os.listdir(path=path_folder)
            dir_array = np.array(list_dir, dtype=int)
            return np.max(dir_array) + 1
        except ValueError:
            if not list_dir:
                return 1
            print(f'Your {os.getcwd()}/model/ directory does not contain only integer directories | Please adjust it!')
            raise
    except TypeError:
        try:
            os.listdir(path=path_folder)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST:
                pass
            else:
                raise


def initialise_run():
    """
    Function to create all the necessary directory for your run

    This will create model, config, log directories if not present
    Model folder will be a timestamp.

    :return: empty
    """

    # Create the timestamp to use for the model directory
    time_stamp_folder = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    # Create new model, config and log directory
    mkdir_p(path_file=f'logs/{time_stamp_folder}')
    print(f'Logs {time_stamp_folder} directory created')
    print('Everything is initialised correctly!')


class MyLogger(Logger):
    def __init__(
        self,
        log_file=None,
        log_format="%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s",
        *args,
        **kwargs
    ):
        """
        Class to handle your python logging. This class is inheriting from Logger one.
        You can import it and then directly use it to log.info("to log") etc...

        This class was taken from here: https://gist.github.com/nguyenkims/e92df0f8bd49973f0c94bddf36ed7fd0

        :param log_file: string to give a name to your log file. By default it will write on your current dir
        you can also add a path for specific location.
        :type log_file: str
        :param log_format: string to declare the your log format
        (https://www.toptal.com/python/in-depth-python-logging for more details)
        :type log_format: str
        :param args: positional arguments.
        :param kwargs: keyword arguments. For example you will need to add keyword argument "name=<any name>"
        when initialising the class so that the Logger constructor can be correctly called
        """
        self.formatter = logging.Formatter(log_format)
        self.log_file = log_file

        Logger.__init__(self, *args, **kwargs)

        self.addHandler(self.get_console_handler())
        if log_file:
            # Make sure that all the directory present in the file name are created
            mkdir_p(self.log_file)
            self.addHandler(self.get_file_handler())

        # with this pattern, it's rarely necessary to propagate the| error up to parent
        self.propagate = False

    def get_console_handler(self):
        """
        Function to set up the logging process on the console

        :return: console_handler object
        """
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(self.formatter)
        return console_handler

    def get_file_handler(self):
        """
        Function to set up the logging to write on a flat .log file
        Using the TimedRotatingFileHandler class in order to create a new log_file every day.

        :return: file_handler object
        """
        file_handler = TimedRotatingFileHandler(self.log_file, when="midnight")
        file_handler.setFormatter(self.formatter)
        return file_handler


def save_json(json_dict, json_name, folder_name='config'):
    """
    Function to save a dictionary as a json file in a specified folder

    :param json_dict: dictionary to be saved
    :type json_dict dict
    :param json_name: name for the config file to be saved as json
    :type json_name: str
    :param folder_name: name of the folder on which to save the json file
    :type folder_name: str
    :return: string path where file was saved
    """

    with open(f'{folder_name}/{json_name}.json', 'w', encoding='utf-8') as fp:
        json.dump(json_dict, fp, ensure_ascii=False, indent=4)

    return f"{json_name} saved in {os.getcwd()}/{folder_name}/"


def load_config(json_name):
    """
    Function to load a config json file and return a python dictionary

    :param json_name: name for the config file to be loaded
    :type json_name: str
    :return: config as a dict
    """

    with open(f'config/{json_name}.json', 'r', encoding = 'utf-8') as fp:
        config = json.load(fp)

    return config


def save_to_csv(df, path_csv):
    """
    Save cleaned data to a csv for further analysis.

    :param df: Pandas dataframe to save
    :param path_csv: path for where to save the file
    :return: str saying it was successfully saved
    """
    df.to_csv(path_csv)
    return f"csv successfully saved on {path_csv}"

def save_to_xlsx(df, path_xlsx):
    """
    Save cleaned data to a xlsx for further analysis.

    :param df: Pandas dataframe to save
    :param path_xlsx: path for where to save the file
    :return: str saying it was successfully saved
    """
    df.to_excel(path_xlsx)
    return f"spreadsheet successfully saved on {path_xlsx}"



def save_to_pickle(df, path_pickle):
    """
    Save cleaned data as a pickle for further analysis.

    :param df: Pandas dataframe to save
    :param path_pickle: path for where to save the file
    :return: str saying it was successfully saved
    """
    with open(path_pickle, 'wb') as f:
        pickle.dump(df, f)

    return f"pickle successfully saved on {path_pickle}"

# round utilities
def round_nearest_base(x, base=25):
    return base * round(x/base)

# Check python venv
def is_venv():
    return (hasattr(sys, 'real_prefix') or
            (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix))

# From numpy nan to SQL NULL (equivalent to python None)
def nan_to_none(df):
    '''
    Setting numpy NaN values to None values in a Dataframe
    '''
    df_imputed = df.astype('object').where(df.notna(), None)
    return df_imputed
