import argparse
import mysql.connector
from mysql.connector import Error
import sys
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import csv


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


if __name__ == '__main__':
    #get arguments
    args = sys.argv[1:]
    portal_ip = args[0]
    portal_db_name = args[1]
    portal_usr = args[2]
    portal_pass = args[3]
    site = args[4]

    # portal DB connection data
    db_connection = get_db_connection(portal_ip, portal_db_name, portal_usr, portal_pass)
    if db_connection:
        query = summary_report_query_builder()
        query_data = get_portal_db_data(mysql_connection=db_connection, query=query)
        if query_data:
            # print(query_data)
            save_csv_file(query_data, header_data, path, file_name + "-" + site)
            print("script finished successfully")
        else:
            print("Error in MySQL query")
    else:
        print("Error in MySQL Connection")