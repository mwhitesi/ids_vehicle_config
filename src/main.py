import os
import numpy as np
import pandas as pd


data_dir = None


def main():
    global data_dir
    cwd = os.path.dirname(os.path.realpath(__file__))
    data_dir = os.path.abspath(
        os.path.join(cwd, os.pardir, os.pardir, 'data/'))

    excel_file = os.path.join(data_dir, 'raw',
                              'EMSData_Asset_Data_20190305.xlsx')
    units_file = os.path.join(data_dir, 'raw',
                              'Unit_2019-03-08_15-19-07.xlsx')
    fleet_df, units_df = load(excel_file, units_file)

    # Check all units have entry in the fleet data frame
    print(units_df['Name'].values[0])
    print(np.where(np.in1d(units_df['Name'].values, [11050])))
    #print(fleet_df['AHS Vehicle ID'])

    l_df = fleet_df.join(units_df, how='inner', lsuffix='_fleet', rsuffix='_ids')
    r_df = units_df.join(fleet_df, how='inner', lsuffix='_ids', rsuffix='_fleet')

    print(units_df.shape)
    print(fleet_df.shape)
    print(l_df.shape)
    print(r_df.shape)


def load(excel_file, units_file):

    fleet_df = pd.read_excel(excel_file)
    units_df = pd.read_excel(units_file)

    fleet_df.set_index('AHS Vehicle ID')
    units_df.set_index('Name')

    return(fleet_df, units_df)




if __name__ == "__main__":
    main()
