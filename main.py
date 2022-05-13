from data_ingestion.db_utils import DBUtils
from data_ingestion.directa_data_pull import DirectaDataPull
from sqlalchemy import String, Integer, DateTime
# import importlib, sys
# importlib.reload(sys.modules['data_ingestion.db_utils'])



if __name__ == '__main__':
    pull_obj = DirectaDataPull(save_log=True, expiration_date_of_interest='SET22', symbol = 'FXM22')
    pull_obj.navigating_directa(options_prices=True, options_open_positions=True, options_calendar=True, options_greeks=True)
    df_options, pk_options = pull_obj.cleaning_options_data('options_table.csv')
    df_open_positions, pk_open_positions = pull_obj.cleaning_tabellone_data(purchase_date='2022-03-31')
    df_calendar, pk_calendar = pull_obj.cleaning_calendar_data()
    df_greeks, pk_greeks = pull_obj.cleaning_greeks_data()
    pull_obj.editing_strategy_calculator(df_options, grid_shift_input=50)
    # initiating DBUtils object for each DataFrame
    db_class = DBUtils('directa', df_options)
    db_class_open = DBUtils('directa', df_open_positions)
    db_class_calendar = DBUtils('directa', df_calendar)
    db_class_greeks = DBUtils('directa', df_greeks)
    # Only run when table doesn't exist and it is the first run
    db_class.LoadTable('daily_options', pk = pk_options, data_types={'strike': Integer(), 'insert_date': DateTime(), 'expiration_date': String(10), 'option_type': String(10)})
    db_class_open.LoadTable('open_position_options', pk = pk_open_positions, data_types={'strike': Integer(), 'purchase_date': DateTime(), 'expiration_date': DateTime(), 'option_type': String(10), 'underlying_asset': String(10)})
    db_class_calendar.LoadTable('calendar_options', pk = pk_calendar, data_types={'strike': Integer(), 'expiration_date': DateTime(), 'option_type': String(10)})
    db_class_greeks.LoadTable('greeks_options', pk = pk_greeks, data_types={'strike': Integer(), 'option_type': String(10), 'expiration_date': DateTime(), 'insert_date': DateTime()})
    # Upsert
    db_class.UpdateInsertTable('daily_options')
    db_class_open.UpdateInsertTable('open_position_options')
    db_class_calendar.UpdateInsertTable('calendar_options')
    db_class_greeks.UpdateInsertTable('greeks_options')

    # Alternative period
    # pull_obj = DirectaDataPull(save_log=True, expiration_date_of_interest='MAR22')
    # pull_obj.navigating_directa()
    # df = pull_obj.cleaning_options_data('options_table.csv', purchase_date='2022-01-19')
    # df_open_positions = pull_obj.cleaning_tabellone_data(purchase_date='2022-01-19')
    # df_calendar = pull_obj.cleaning_calendar_data()
    # pull_obj.editing_strategy_calculator(df)
    # db_class = DBUtils('directa', df)
    # db_class_open = DBUtils('directa', df_open_positions)
    # # initiating DBUtils object for each DataFrame
    # db_class = DBUtils('directa', df)
    # db_class_open = DBUtils('directa', df_open_positions)
    # db_class_calendar = DBUtils('directa', df_calendar)
    # # Only run when table doesn't exist and it is the first run
    # db_class.LoadTable('daily_options', pk = 'pk', data_types={'pk': String(50)})
    # db_class_open.LoadTable('open_position_options', pk = 'pk', data_types={'pk': String(50)})
    # db_class_calendar.LoadTable('calendar_options', pk = 'pk', data_types={'pk': String(50)})
    # # Upsert
    # db_class.UpdateInsertTable('daily_options')
    # db_class_open.UpdateInsertTable('open_position_options')
    # db_class_calendar.UpdateInsertTable('calendar_options')


