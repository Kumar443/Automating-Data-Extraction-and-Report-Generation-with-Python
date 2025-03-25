import cx_Oracle
import pandas as pd

def fetch_report_config(report_name, connection):
    try:
        query = """
        SELECT RPT_SQL_TXT, FILE_TYPE, MULTY_REPORT, EFX_FLAG, SHEET_NAME, START_ROW, START_COL
        FROM report_config_table
        WHERE REPORT_NAME = :report_name
        """
        df = pd.read_sql(query, connection, params={"report_name": report_name})
        if df.empty:
            raise ValueError(f"There is no Mapping available, please check whether the config table is updated for process name: {report_name}")
        return df.iloc[0]
    except Exception as e:
        print(f"Error fetching report config: {e}")
        return None

# Example usage:
config = fetch_report_config("example_report_name", conn)


import subprocess
import os

def generate_report(config, connection):
    try:
        sql_query = config['RPT_SQL_TXT']
        file_type = config['FILE_TYPE']
        is_multy_report = config['MULTY_REPORT'] == 'Y'
        efx_flag = config['EFX_FLAG'] == 'Y'
        rpt_src_file = "template.xlsx"
        rpt_path_file = f"{config['REPORT_NAME']}.{file_type.lower()}"
        rpt_archive_path = "/path/to/archive"

        # Archive existing file
        if os.path.exists(rpt_path_file):
            subprocess.check_call(["mv", rpt_path_file, rpt_archive_path])

        if file_type == 'CSV':
            CSV_File_Gen(sql_query, connection, rpt_path_file, rpt_archive_path)
        elif file_type == 'EXCEL':
            if is_multy_report:
                generate_multiple_reports(sql_query, connection, rpt_src_file, rpt_path_file, config)
            else:
                Final_report_create(sql_query, connection, "1", rpt_src_file, rpt_path_file, config['SHEET_NAME'], config['START_ROW'], config['START_COL'], rpt_archive_path)

        # EFX control
        if efx_flag:
            EFX_Final_Report(config['REPORT_NAME'])

    except Exception as e:
        print(f"Report generation error: {e}")

# Example usage:
generate_report(config, conn)


def generate_multiple_reports(sql_query, connection, rpt_src_file, rpt_path_file, config):
    # Assume `Process_query` logic is to fetch distinct codes for multi-reporting
    Process_query = "SELECT DISTINCT some_field FROM some_table WHERE some_condition = 'Y'"
    cur = connection.cursor()
    cur.execute(Process_query)
    result_codes = cur.fetchall()

    for code in result_codes:
        modified_query = f"{sql_query} WHERE some_condition_field = {code[0]}"
        Final_report_create(modified_query, connection, "1", rpt_src_file, rpt_path_file, f"Sheet_{code[0]}", config['START_ROW'], config['START_COL'], "/path/to/archive")

# Example usage inside generate_report when is_multy_report flag is True
# generate_multiple_reports(sql_query, connection, rpt_src_file, rpt_path_file, config)


