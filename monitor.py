import argparse
import datetime
import sys
import sqlite3
import yaml

def load_conf_file_monitors(yaml_file):
  try:
     with open(yaml_file,'r') as file:
        return yaml.safe_load(file)['monitors']
  except FileNotFoundError:
        print(f"Configuration file Not exists in the folder: {yaml_file}")
        sys.exit(1)
     
def execute_monitor(cursor, monitor, date):
    sql = monitor['sql']
    place_holder = sql.count('?')
    if place_holder == 4:
       cursor.execute(sql,(date,date,date,date))
    elif place_holder == 2:
       cursor.execute(sql, (date,date))
    elif place_holder == 1:
       cursor.execute(sql,(date,))
    return cursor.fetchall()
    

def send_alert(channel, data, notify):
    if channel == 'jira':
        create_jira_ticket(data, notify)
    elif channel == 'email':
        send_email_alert(data, notify)
    elif channel == 'slack':
        send_slack_alert(data, notify)

def create_jira_ticket(data,recipient):
    print(f"Creating JIRA ticket for {recipient}:{data}")

def send_email_alert(data, recipient):
    print(f"Sending email alert to {recipient}:{data}")

def send_slack_alert(data, channel):
    print(f"Sending stack alert to {channel}:{data}")


def run_monitors(date):
    monitors = load_conf_file_monitors('monitors.yaml')
    try:
      conn = sqlite3.connect('sample.db')
      cursor = conn.cursor()
    except Exception as ex:
      print('Error Connection to SQLLite Database')
      sys.exit(1)
    for monitor in monitors:
      if (monitor['frequency'] == 'daily' or
           (monitor['frequency']=='monthly' and date[-2:]=='01')):
            results = execute_monitor(cursor, monitor, date)
            if results:
                send_alert(monitor['alert_channel'], results, monitor['notify'])
            else:
               print('no transactions for alert channel: ',monitor['alert_channel'])
      else:
         print(f"skipping monitor '{monitor['title']}' based on frequency")
    conn.close()
    
    

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description="Run Fraud monitors")
    argparser.add_argument("--input_run_date",help="input date to run monitors. enter only in (YYYY-MM-DD) format")
    args = argparser.parse_args()
    try:
       run_date=args.input_run_date
       if args.input_run_date:
         validation_check_date=datetime.datetime.strptime(args.input_run_date,"%Y-%m-%d")
       else:
        run_date = datetime.datetime.now()
        run_date= run_date.strftime("%Y-%m-%d")
    except Exception as ex:
       print('Enter input_run_date only in this format(YYYY-MM-DD): ', ex)
       sys.exit(1)
    print('Input Parameter parsed Successfully')
    print('run_date:',run_date)
    run_monitors(run_date)