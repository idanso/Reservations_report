import argparse
import mysql.connector
from mysql.connector import Error
import sys
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import csv
import pandas as pd


#####################################
######### Globals variables #########
#####################################

#############################
# portal DB connection data #
#############################
# portal_ip = None
# portal_db_name = None
# portal_usr = None
# portal_pass = None
############################

############################
######## time data #########
############################

months_diff = 6
# unix time of current time
now_time = int(time.time())

# unix time of time in the past (current 6 months ago)
past_time = int((datetime.today() + relativedelta(months=-months_diff)).timestamp())
############################

############################
######## file data #########
############################

path = "C:\\inetpub\\FTP_Folder\\"
file_name = "Reservations_Summary-last_" + str(months_diff) + "_months"

############################
## portal DB Query's data ##
############################
header_data = ['User Name', 'Manager', 'Region', 'Location', 'Reservations Total Count',
               'Alteon and Analytics', 'Last Reservation', 'Alteon Ansible Automation', 'Last Reservation', 'Alteon Cloud Controller', 'Last Reservation', 'Virtual DefensePro', 'Last Reservation', 'SSL Inspection', 'Last Reservation',
               'Appwall', 'Last Reservation', 'Defense Flow', 'Last Reservation', 'KWAF - ExtAuth', 'Last Reservation', 'KWAF - Inline Mode', 'Last Reservation', 'Alteon GEL Automation', 'Last Reservation']

regions_lst = ["'APAC'", "'EMEA & CALA'", "'North America'", "'EMEA & CALA', 'APAC', 'North America'"]
############################


# getarg to add later
# def GetArgs():
#    """
#    Supports the command-line arguments listed below.
#    """
#    parser = argparse.ArgumentParser(
#        description='Process args for retrieving all the Virtual Machines')
#    parser.add_argument('-s', '--host', required=True, action='store',
#                        help='Remote host to connect to')
#    parser.add_argument('-o', '--port', type=int, default=443, action='store',
#                        help='Port to connect on')
#    parser.add_argument('-u', '--user', required=True, action='store',
#                        help='User name to use when connecting to host')
#    parser.add_argument('-v', '--vm', required=True, action='store',
#                        help='header vm to monitor')
#    parser.add_argument('-p', '--password', required=False, action='store',
#                        help='Password to use when connecting to host')
#    args = parser.parse_args()
#    return args

def summary_report_query_builder(region_code=3):
    lab_lst = [('Alteon and Analytics', 'AAAnalytics_col'), ('Alteon Ansible Automation', 'AAAutomation_col'), (('Alteon Cloud Controller', 'Alteon Cloud Controller - Demo', 'Alteon Cloud Controller - Training'), 'ACController_col'), ('Virtual DefensePro', 'VDP_col'), ('SSL Inspection', 'SSLI_col'), ('Appwall', 'APPW_col'), ('Defense Flow', 'DF_col'), (('KWAF - ExtAuth', 'KWAF - External Authorization Mode'), 'KWAFEA_col'), ('KWAF - Inline Mode', 'KWAFIN_col'), (('Global Elastic License (GEL)', 'Alteon GEL Automation'), 'GEL_col')]

    query =  "SELECT\n" + \
             "Employees.Full_Name, Employees.Manager," + ("Employees.Region," if region_code == 3 else "") + " Employees.Location, IF(total_by_time.Count IS NULL, 0, total_by_time.Count) AS 'Reservations Total Count'"

    for lab in lab_lst:
        query += ",\nIF(" + lab[1] + ".Count IS NULL, 0, " + lab[1] + ".Count) AS '" + (str(lab[0]) if isinstance(lab[0], str) else lab[0][0]) + "', IF(" + lab[1] + "2.Last_Res IS NULL, '', DATE_FORMAT(MAX(" + lab[1] + "2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'"

    query += "\nFROM\nEmployees"
    query += "\nLEFT JOIN\n" + \
        "(SELECT Full_Name, COUNT(VMName) AS Count FROM Reservations, Employees WHERE Start BETWEEN FROM_UNIXTIME(" + str(
            past_time) + ") AND FROM_UNIXTIME(" + str(
            now_time) + ") AND Reservations.Email = Employees.Email GROUP BY Full_Name) AS total_by_time ON Employees.Full_Name = total_by_time.Full_Name"

    for lab in lab_lst:
        query += "\nLEFT JOIN" + \
        "\n(SELECT Full_Name, COUNT(VMName) AS Count FROM Reservations, Employees WHERE Lab in " + (str(lab[0]) if isinstance(lab[0], tuple) else str("('" + str(lab[0]) + "')")) + " AND Start BETWEEN FROM_UNIXTIME(" + str(
            past_time) + ") AND FROM_UNIXTIME(" + str(
            now_time) + ") AND Reservations.Email = Employees.Email GROUP BY Full_Name) AS " + lab[1] + " ON Employees.Full_Name = " + lab[1] + ".Full_Name" + \
        "\nLEFT JOIN" + \
        "\n(SELECT Full_Name, Max(Start) AS Last_Res FROM Reservations, Employees WHERE Lab in " + (str(lab[0]) if isinstance(lab[0], tuple) else str("('" + str(lab[0]) + "')")) + " AND Reservations.Email = Employees.Email GROUP BY Full_Name) AS " + lab[1] + "2 ON Employees.Full_Name = " + lab[1] + "2.Full_Name"

    query += "\nWHERE Employees.Region IN (" + regions_lst[region_code] + ")\n" + \
        "Group by Full_Name, Manager, Region, Location, total_by_time.Count"
    for lab in lab_lst:
        query += ", " + lab[1] + ".count, " + lab[1] + "2.Last_Res"

    return query

def save_csv_file(data, header, path, fn):
    # open the file in the write mode
    with open(path + fn + ".csv", 'w', newline='') as f:
        writer = csv.writer(f)
        # write the header
        writer.writerow(header)

        # write multiple rows
        writer.writerows(data)


# established connection to required DB and return cursor
def get_db_connection(host, db, user, password):
    try:
        connection = mysql.connector.connect(host=host,
                                             database=db,
                                             user=user,
                                             password=password)

        if connection.is_connected():
            return connection
        else:
            return 0

    except Error as e:
        print("Error while connecting to MySQL", e)


# established connection to portal db, query count of active labs and return total labs count
def get_portal_db_data(mysql_connection, query):
    try:
        mysql_cursor = mysql_connection.cursor()
        if mysql_cursor:
            mysql_cursor.execute(query)
            record = mysql_cursor.fetchall()
            return list(record)
        return 0

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if mysql_connection.is_connected():
            mysql_cursor.close()
            mysql_connection.close()
            print("MySQL connection is closed")


def merge_queries_data(df_nj, df_de):
    # import data
    df_nj = pd.read_csv("/usr/data/Reservations_Summary-last_6_months-NJ.csv", encoding="ISO-8859-1")
    df_de = pd.read_csv('/usr/data/Reservations_Summary-last_6_months-DE.csv', encoding="ISO-8859-1")

    # fix data types of last reservations to datetime
    last_reservations_col_lst = ['Last Reservation', 'Last Reservation.1', 'Last Reservation.2',
                                 'Last Reservation.3', 'Last Reservation.4', 'Last Reservation.5', 'Last Reservation.6',
                                 'Last Reservation.7',
                                 'Last Reservation.8', 'Last Reservation.9']

    for col in last_reservations_col_lst:
        df_nj[col] = pd.to_datetime(df_nj[col])
        df_de[col] = pd.to_datetime(df_de[col])

    # set size of dataframe output to max
    pd.set_option("display.max_rows", None, "display.max_columns", None)

    # merge the 2 data frames
    df_merge = pd.concat([df_nj, df_de])

    # create 2 data frames for each data type (int, date) and group them by 'User Name'
    df_merge_count = df_merge.groupby('User Name')[
        ['Manager', 'Region', 'Location', 'Reservations Total Count', 'Alteon and Analytics',
         'Alteon Ansible Automation', 'Alteon Cloud Controller', 'Virtual DefensePro', 'SSL Inspection', 'Appwall',
         'Defense Flow', 'KWAF - ExtAuth', 'KWAF - Inline Mode', 'Alteon GEL Automation']].sum()
    df_merge_last_reservations = df_merge.groupby('User Name')[
        ['Manager', 'Region', 'Location', 'Last Reservation', 'Last Reservation.1', 'Last Reservation.2',
         'Last Reservation.3', 'Last Reservation.4', 'Last Reservation.5', 'Last Reservation.6', 'Last Reservation.7',
         'Last Reservation.8', 'Last Reservation.9']].max()

    # merge the 2 data frames and arrange the columns as the original data frame
    df_merge_full = df_merge_last_reservations.merge(df_merge_count, how='left', on='User Name')
    df_merge_full = df_merge_full[
        ['Manager', 'Region', 'Location', 'Reservations Total Count', 'Alteon and Analytics', 'Last Reservation',
         'Alteon Ansible Automation', 'Last Reservation.1', 'Alteon Cloud Controller', 'Last Reservation.2',
         'Virtual DefensePro', 'Last Reservation.3', 'SSL Inspection', 'Last Reservation.4', 'Appwall',
         'Last Reservation.5', 'Defense Flow', 'Last Reservation.6', 'KWAF - ExtAuth', 'Last Reservation.7',
         'KWAF - Inline Mode', 'Last Reservation.8', 'Alteon GEL Automation', 'Last Reservation.9']]

    return df_merge_full


if __name__ == '__main__':
    # get arguments
    args = sys.argv[1:]
    portal_ip_nj = args[0]
    portal_ip_de = args[1]
    portal_db_name = args[2]
    portal_usr = args[3]
    portal_pass = args[4]
    # site = args[4]
    #
    # # portal DB connection data
    # db_connection = get_db_connection(portal_ip, portal_db_name, portal_usr, portal_pass)
    # if db_connection:
    #     query = summary_report_query_builder()
    #     query_data = get_portal_db_data(mysql_connection=db_connection, query=query)
    #     if query_data:
    #         # print(query_data)
    #         save_csv_file(query_data, header_data, path, file_name + "-" + site)
    #         print("script finished successfully")
    #     else:
    #         print("Error in MySQL query")
    # else:
    #     print("Error in MySQL Connection")

    query_data_nj = None
    query_data_de = None

    # NJ portal DB connection data
    db_connection = get_db_connection(query_data_nj, portal_db_name, portal_usr, portal_pass)
    if db_connection:
        query = summary_report_query_builder()
        query_data_nj = get_portal_db_data(mysql_connection=db_connection, query=query)

        if query_data_nj:
            # convert query result to pandas DF
            df_nj = pd.DataFrame(query_data_nj, columns=header_data)

    # DE portal DB connection data
    db_connection = get_db_connection(query_data_de, portal_db_name, portal_usr, portal_pass)
    if db_connection:
        query = summary_report_query_builder()
        query_data_de = get_portal_db_data(mysql_connection=db_connection, query=query)

        if query_data_nj:
            # convert query result to pandas DF
            df_de = pd.DataFrame(query_data_de, columns=header_data)

    # merge data frames
    merged_df = merge_queries_data(df_nj=df_nj, df_de=df_de)
    print(merged_df)

