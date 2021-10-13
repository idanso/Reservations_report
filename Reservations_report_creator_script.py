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
header_data = ['User Name', 'Manager', 'Region', 'Location', 'Department', 'ASE', 'Reservations Total Count',
               'Alteon and Analytics', 'Last Reservation', 'Alteon Ansible Automation', 'Last Reservation', 'Alteon Cloud Controller', 'Last Reservation', 'Virtual DefensePro', 'Last Reservation', 'SSL Inspection', 'Last Reservation',
               'Appwall', 'Last Reservation', 'Defense Flow', 'Last Reservation', 'KWAF - ExtAuth', 'Last Reservation', 'KWAF - Inline Mode', 'Last Reservation', 'Alteon GEL Automation', 'Last Reservation']
# summary_report_query = str(
#     "SELECT" +
#     " Employees.Full_Name AS 'User Name', Employees.Manager, Employees.Region, Employees.Location, COUNT(Reservations.VMName) AS 'Reservations Total Count'," +
#     " IF(AAAnalytics_col.Count IS NULL, 0, AAAnalytics_col.Count) AS 'Alteon and Analytics', IF(AAAnalytics_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(AAAnalytics_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
#     " IF(AAAutomation_col.Count IS NULL, 0, AAAutomation_col.Count) AS 'Alteon Ansible Automation', IF(AAAutomation_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(AAAutomation_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
#     " IF(ACController_col.Count IS NULL, 0, ACController_col.Count) AS 'Alteon Cloud Controller', IF(ACController_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(ACController_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
#     " IF(VDP_col.Count IS NULL, 0, VDP_col.Count) AS 'Virtual DefensePro', IF(VDP_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(VDP_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
#     " IF(SSLI_col.Count IS NULL, 0, SSLI_col.Count) AS 'SSL Inspection', IF(SSLI_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(SSLI_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
#     " IF(APPW_col.Count IS NULL, 0, APPW_col.Count) AS 'Appwall', IF(APPW_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(APPW_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
#     " IF(DF_col.Count IS NULL, 0, DF_col.Count) AS 'Defense Flow', IF(DF_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(DF_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
#     " IF(KWAFEA_col.Count IS NULL, 0, KWAFEA_col.Count) AS 'KWAF - ExtAuth', IF(KWAFEA_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(KWAFEA_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
#     " IF(KWAFIM_col.Count IS NULL, 0, KWAFIM_col.Count) AS 'KWAF - Inline Mode', IF(KWAFIM_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(KWAFIM_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
#     " IF(GEL_col.Count IS NULL, 0, GEL_col.Count) AS 'Global Elastic License (GEL)', IF(GEL_col.Last_Res IS NULL, '', DATE_FORMAT(MAX(GEL_col.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'" +
#     " FROM" +
#     " Reservations" +
#     " LEFT JOIN Employees ON Reservations.Email = Employees.Email" +
#     " LEFT JOIN" +
#     " (SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Alteon and Analytics' AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS AAAnalytics_col ON Reservations.Email = AAAnalytics_col.Email" +
#     " LEFT JOIN" +
#     " (SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Alteon Ansible Automation' AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS AAAutomation_col ON Reservations.Email = AAAutomation_col.Email" +
#     " LEFT JOIN" +
#     " (SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab IN ('Alteon Cloud Controller', 'Alteon Cloud Controller - Demo', 'Alteon Cloud Controller - Training') AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS ACController_col ON Reservations.Email = ACController_col.Email" +
#     " LEFT JOIN" +
#     " (SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Virtual DefensePro' AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS VDP_col ON Reservations.Email = VDP_col.Email" +
#     " LEFT JOIN" +
#     " (SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'SSL Inspection' AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS SSLI_col ON Reservations.Email = SSLI_col.Email" +
#     " LEFT JOIN" +
#     " (SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Appwall' AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS APPW_col ON Reservations.Email = APPW_col.Email" +
#     " LEFT JOIN" +
#     " (SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Defense Flow' AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS DF_col ON Reservations.Email = DF_col.Email" +
#     " LEFT JOIN" +
#     " (SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab IN ('KWAF - ExtAuth', 'KWAF - External Authorization Mode') AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS KWAFEA_col ON Reservations.Email = KWAFEA_col.Email" +
#     " LEFT JOIN" +
#     " (SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'KWAF - Inline Mode' AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS KWAFIM_col ON Reservations.Email = KWAFIM_col.Email" +
#     " LEFT JOIN" +
#     " (SELECT Email, COUNT(VMName) AS Count, Max(Start) AS Last_Res FROM Reservations WHERE Lab IN ('Global Elastic License (GEL)', 'Alteon GEL Automation') AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS GEL_col ON Reservations.Email = GEL_col.Email" +
#     " WHERE" +
#     " Reservations.Start BETWEEN FROM_UNIXTIME(1617718394) AND FROM_UNIXTIME(1633529594)" +
#     " Group by Full_Name, Manager, Region, Location, Department, Reservations.Email, ASE;")

summary_report_query = str(
    "SELECT" +
    "Employees.Full_Name, Employees.Manager, Employees.Region, Employees.Location, COUNT(Reservations.VMName) AS 'Reservations Total Count'," +
    "IF(AAAnalytics_col.Count IS NULL, 0, AAAnalytics_col.Count) AS 'Alteon and Analytics', IF(AAAnalytics_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(AAAnalytics_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
    "IF(AAAutomation_col.Count IS NULL, 0, AAAutomation_col.Count) AS 'Alteon Ansible Automation', IF(AAAutomation_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(AAAutomation_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
    "IF(ACController_col.Count IS NULL, 0, ACController_col.Count) AS 'Alteon Cloud Controller', IF(ACController_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(ACController_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
    "IF(VDP_col.Count IS NULL, 0, VDP_col.Count) AS 'Virtual DefensePro', IF(VDP_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(VDP_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
    "IF(SSLI_col.Count IS NULL, 0, SSLI_col.Count) AS 'SSL Inspection', IF(SSLI_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(SSLI_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
    "IF(APPW_col.Count IS NULL, 0, APPW_col.Count) AS 'Appwall', IF(APPW_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(APPW_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
    "IF(DF_col.Count IS NULL, 0, DF_col.Count) AS 'Defense Flow', IF(DF_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(DF_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
    "IF(KWAFEA_col.Count IS NULL, 0, KWAFEA_col.Count) AS 'KWAF - ExtAuth', IF(KWAFEA_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(KWAFEA_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
    "vIF(KWAFIM_col.Count IS NULL, 0, KWAFIM_col.Count) AS 'KWAF - Inline Mode', IF(KWAFIM_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(KWAFIM_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'," +
    "IF(GEL_col.Count IS NULL, 0, GEL_col.Count) AS 'Global Elastic License (GEL)', IF(GEL_col2.Last_Res IS NULL, '', DATE_FORMAT(MAX(GEL_col2.Last_Res), '%Y/%m/%d %H:%i:%S')) AS 'Last Reservation'" +
    "FROM" +
    "Employees" +
    "LEFT JOIN" +
    "Reservations ON Reservations.Email = Employees.Email" +
    "LEFT JOIN" +
    "(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab = 'Alteon and Analytics' AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS AAAnalytics_col ON Reservations.Email = AAAnalytics_col.Email" +
    "LEFT JOIN" +
    "(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Alteon and Analytics' GROUP BY Email) AS AAAnalytics_col2 ON Reservations.Email = AAAnalytics_col2.Email" +
    "LEFT JOIN" +
    "(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab = 'Alteon Ansible Automation' AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS AAAutomation_col ON Reservations.Email = AAAutomation_col.Email" +
    "LEFT JOIN" +
    "(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Alteon Ansible Automation' GROUP BY Email) AS AAAutomation_col2 ON Reservations.Email = AAAutomation_col2.Email" +
    "LEFT JOIN" +
    "(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab IN ('Alteon Cloud Controller', 'Alteon Cloud Controller - Demo', 'Alteon Cloud Controller - Training') AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS ACController_col ON Reservations.Email = ACController_col.Email" +
    "LEFT JOIN" +
    "(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab IN ('Alteon Cloud Controller', 'Alteon Cloud Controller - Demo', 'Alteon Cloud Controller - Training') GROUP BY Email) AS ACController_col2 ON Reservations.Email = ACController_col2.Email" +
    "LEFT JOIN" +
    "(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab = 'Virtual DefensePro' AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS VDP_col ON Reservations.Email = VDP_col.Email" +
    "LEFT JOIN" +
    "(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Virtual DefensePro' GROUP BY Email) AS VDP_col2 ON Reservations.Email = VDP_col2.Email" +
    "LEFT JOIN" +
    "(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab = 'SSL Inspection' AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS SSLI_col ON Reservations.Email = SSLI_col.Email" +
    "LEFT JOIN" +
    "(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'SSL Inspection' GROUP BY Email) AS SSLI_col2 ON Reservations.Email = SSLI_col2.Email" +
    "LEFT JOIN" +
    "(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab = 'Appwall' AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS APPW_col ON Reservations.Email = APPW_col.Email" +
    "LEFT JOIN" +
    "(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Appwall' GROUP BY Email) AS APPW_col2 ON Reservations.Email = APPW_col2.Email" +
    "LEFT JOIN" +
    "(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab = 'Defense Flow' AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS DF_col ON Reservations.Email = DF_col.Email" +
    "LEFT JOIN" +
    "(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'Defense Flow' GROUP BY Email) AS DF_col2 ON Reservations.Email = DF_col2.Email" +
    "LEFT JOIN" +
    "(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab IN ('KWAF - ExtAuth', 'KWAF - External Authorization Mode') AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS KWAFEA_col ON Reservations.Email = KWAFEA_col.Email" +
    "LEFT JOIN" +
    "(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab IN ('KWAF - ExtAuth', 'KWAF - External Authorization Mode') GROUP BY Email) AS KWAFEA_col2 ON Reservations.Email = KWAFEA_col2.Email" +
    "LEFT JOIN" +
    "(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab = 'KWAF - Inline Mode' AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS KWAFIM_col ON Reservations.Email = KWAFIM_col.Email" +
    "LEFT JOIN" +
    "(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab = 'KWAF - Inline Mode' GROUP BY Email) AS KWAFIM_col2 ON Reservations.Email = KWAFIM_col2.Email" +
    "LEFT JOIN" +
    "(SELECT Email, COUNT(VMName) AS Count FROM Reservations WHERE Lab IN ('Global Elastic License (GEL)', 'Alteon GEL Automation') AND Start BETWEEN FROM_UNIXTIME(" + str(past_time) + ") AND FROM_UNIXTIME(" + str(now_time) + ") GROUP BY Email) AS GEL_col ON Reservations.Email = GEL_col.Email" +
    "LEFT JOIN" +
    "(SELECT Email, Max(Start) AS Last_Res FROM Reservations WHERE Lab IN ('Global Elastic License (GEL)', 'Alteon GEL Automation') GROUP BY Email) AS GEL_col2 ON Reservations.Email = GEL_col2.Email" +
    "Group by Full_Name, Manager, Region, Location, Reservations.Email;")
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


def save_csv_file(data, header, path, file_name):
    # open the file in the write mode
    with open(path + file_name + ".csv", 'w', newline='') as f:
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
        mysql_cursor.execute(query)
        record = mysql_cursor.fetchall()
        return list(record)

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if mysql_connection.is_connected():
            mysql_cursor.close()
            mysql_connection.close()
            print("MySQL connection is closed")


if __name__ == '__main__':
    # get arguments
    args = sys.argv[1:]
    portal_ip = args[0]
    portal_db_name = args[1]
    portal_usr = args[2]
    portal_pass = args[3]
    site = args[4]

    # portal DB connection data
    db_connection = get_db_connection(portal_ip, portal_db_name, portal_usr, portal_pass)
    query_data = get_portal_db_data(mysql_connection=db_connection, query=summary_report_query)
    # print(query_data)
    save_csv_file(query_data, header_data, path, file_name + "-" + site)
    print("script finished successfully")
    # print(get_portal_db_data(cursor))
