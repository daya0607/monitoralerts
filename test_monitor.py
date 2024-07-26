import unittest
from unittest.mock import patch, MagicMock
import sqlite3
import yaml
from monitor import load_conf_file_monitors, execute_monitor, send_alert, run_monitors
import io
import argparse
import sys
import datetime

class TestFraudMonitoringSystem(unittest.TestCase):

    yaml_content = """
monitors:
 - title: "Daily alert - Transactions above $300"
   sql: >
    SELECT * FROM transactions 
    WHERE CAST(transaction_amount AS FLOAT)<-300
    and date(transaction_date)= date(?,'-1 day') ;
   frequency : "daily"
   alert_channel : "jira"
   notify : "fraud_team@gmail.com"

 - title: "Monthly Email Alert - Spending Deviation"
   sql: >
    WITH monthly_totals AS (
        SELECT account_id, strftime('%Y-%m', transaction_date) as month,
          SUM(CAST(transaction_amount AS FLOAT)) as monthly_spend
        FROM transactions
        group by account_id, month
      ),
    account_average AS (
        SELECT account_id, AVG(monthly_spend) as average_spend
        FROM monthly_totals
        WHERE month<strftime('%Y-%m', date(?,'-1 month'))
        GROUP BY account_id
      )
    SELECT t.account_id, SUM(CAST(t.transaction_amount AS FLOAT)) as total_spend, a.average_spend
      FROM transactions t
      JOIN account_average a ON t.account_id=a.account_id
      WHERE strftime('%Y-%m', t.transaction_date)=strftime('%Y-%m', date(?,'-1 month'))
      GROUP BY t.account_id
      HAVING total_spend <= 2*a.average_spend ;
   frequency: "monthly"
   alert_channel: "email"
   notify: "fraud_team@gmail.com"

 - title : "Slack notifications - Accounts spent more than 500$"
   sql: >
    WITH today_spending AS (
    SELECT 
        customer_id,
        account_id,
        SUM(transaction_amount) as total_spend
    FROM 
        transactions
    WHERE 
        transaction_date >= date(?, 'start of month')
        AND transaction_date <= ?
    GROUP BY 
        customer_id, account_id
    ),
    yesterday_spending AS (
    SELECT 
        customer_id,
        account_id, 
        SUM(transaction_amount) as total_spend
    FROM 
        transactions
    WHERE 
        transaction_date >= date(?, 'start of month')
        AND transaction_date <= date(?, '-1 day')
    GROUP BY 
        customer_id, account_id
    )
    SELECT DISTINCT
    t.customer_id,
    t.account_id, 
    t.total_spend
    FROM 
    today_spending t
    LEFT JOIN 
    yesterday_spending y ON t.account_id = y.account_id AND t.customer_id = y.customer_id
    WHERE 
    t.total_spend < -500
    AND (y.total_spend IS NULL OR y.total_spend >= -500)
    ORDER BY 
    t.total_spend DESC;
   frequency: "daily"
   alert_channel: "slack"
   notify: "#fraud-team"
"""

    #validating if loading yaml file parse right data
    @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data=yaml_content)
    def test_load_conf_file_monitors(self, mock_open):
        monitors = load_conf_file_monitors('monitors.yaml')
        self.assertEqual(len(monitors), 3)
        self.assertEqual(monitors[0]['title'], "Daily alert - Transactions above $300")
        self.assertEqual(monitors[1]['title'],"Monthly Email Alert - Spending Deviation")
        self.assertEqual(monitors[2]['title'],"Slack notifications - Accounts spent more than 500$")
        self.assertEqual(monitors[0]['frequency'], "daily")
        self.assertEqual(monitors[1]['frequency'], "monthly")
        self.assertEqual(monitors[2]['frequency'], "daily")
        self.assertEqual(monitors[0]['alert_channel'], "jira")
        self.assertEqual(monitors[1]['alert_channel'], "email")
        self.assertEqual(monitors[2]['alert_channel'], "slack")
        
    #testing if the yaml loading function throws a error when file is not found
    def test_load_conf_file_monitors_file_not_found(self):
        with self.assertRaises(SystemExit):
            load_conf_file_monitors('doesnotexist.yaml')

    #testing a sql query to return expected result creating a mock db connection
    @patch('sqlite3.connect')
    def test_execute_monitor(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('customer1', -481)]

        monitor = {
            'sql': "SELECT * FROM transactions WHERE CAST(transaction_amount AS FLOAT)<-300 and date(transaction_date)= date(?,'-1 day')",
            'frequency': 'daily'
        }
        results = execute_monitor(mock_cursor, monitor, '2024-02-01')

        mock_cursor.execute.assert_called_once_with(monitor['sql'], ('2024-02-01',))
        self.assertEqual(results, [('customer1', -481)])

    #testing if the create alert function is sending the right data
    @patch('monitor.create_jira_ticket')
    @patch('monitor.send_email_alert')
    @patch('monitor.send_slack_alert')
    def test_send_alert(self, mock_slack, mock_email, mock_jira):
        data = [('customer1', -481)]
        send_alert('jira', data, 'fraud_team@gmail.com')
        mock_jira.assert_called_once_with(data, 'fraud_team@gmail.com')

        send_alert('email', data, 'fraud_team@gmail.com')
        mock_email.assert_called_once_with(data, 'fraud_team@gmail.com')

        send_alert('slack', data, '#fraud-team')
        mock_slack.assert_called_once_with(data, '#fraud-team')

    #testing run monitor function. It returns 3 values for all first dates of month. 2 otherwise. 
    @patch('monitor.load_conf_file_monitors')
    @patch('sqlite3.connect')
    @patch('monitor.execute_monitor')
    @patch('monitor.send_alert')
    def test_run_monitors(self, mock_send_alert, mock_execute, mock_connect, mock_load):
        mock_load.return_value = [
            {'title': 'Daily alert - Transactions above $300', 'frequency': 'daily', 'alert_channel': 'jira', 'notify': 'fraud_team@gmail.com'},
            {'title': 'Monthly Email Alert - Spending Deviation', 'frequency': 'monthly', 'alert_channel': 'email', 'notify': 'fraud_team@gmail.com'},
            {'title': 'Slack notifications - Accounts spent more than 500$', 'frequency': 'daily', 'alert_channel': 'slack', 'notify': '#fraud-team'}
        ]
        mock_execute.return_value = [('customer1', -481)]

        with patch('builtins.print') as mock_print:
            run_monitors('2023-12-15')
            mock_print.assert_any_call("skipping monitor 'Monthly Email Alert - Spending Deviation' based on frequency")

        self.assertEqual(mock_execute.call_count, 2)
        self.assertEqual(mock_send_alert.call_count, 2)  

        mock_execute.reset_mock()
        mock_send_alert.reset_mock()


        run_monitors('2023-12-01')

        self.assertEqual(mock_execute.call_count, 3)
        self.assertEqual(mock_send_alert.call_count, 3) 

        expected_calls = [
            unittest.mock.call('jira', [('customer1', -481)], 'fraud_team@gmail.com'),
            unittest.mock.call('email', [('customer1', -481)], 'fraud_team@gmail.com'),
            unittest.mock.call('slack', [('customer1', -481)], '#fraud-team')
        ]
        mock_send_alert.assert_has_calls(expected_calls, any_order=True)
    #testing database error
    @patch('sqlite3.connect')
    def test_database_connection_error(self, mock_connect):
        mock_connect.side_effect = sqlite3.Error("Unable to connect")
        with self.assertRaises(SystemExit):
            run_monitors('2023-12-01')

    #testing empty returns
    @patch('monitor.load_conf_file_monitors')
    @patch('sqlite3.connect')
    @patch('monitor.execute_monitor')
    def test_no_transactions(self, mock_execute, mock_connect, mock_load):
        mock_load.return_value = [
            {'title': 'Daily alert - Transactions above $300', 'frequency': 'daily', 'alert_channel': 'jira', 'notify': 'fraud_team@gmail.com'}
        ]
        mock_execute.return_value = []

        with patch('builtins.print') as mock_print:
            run_monitors('2023-12-01')
            mock_print.assert_any_call('no transactions for alert channel: ', 'jira')

    #testing test monitor skip behaviour. for dates not starting with 1
    @patch('monitor.load_conf_file_monitors')
    @patch('sqlite3.connect')
    @patch('monitor.execute_monitor')
    def test_monitor_skipped(self, mock_execute, mock_connect, mock_load):
        mock_load.return_value = [
            {'title': 'Monthly Email Alert - Spending Deviation', 'frequency': 'monthly', 'alert_channel': 'email', 'notify': 'fraud_team@gmail.com'}
        ]

        with patch('builtins.print') as mock_print:
            run_monitors('2023-12-02')
            mock_print.assert_any_call("skipping monitor 'Monthly Email Alert - Spending Deviation' based on frequency")
    
    #testing if run monitors calls the date provided in the CLI
    @patch('monitor.run_monitors')
    @patch('monitor.argparse.ArgumentParser.parse_args')
    def test_valid_input_date(self, mock_parse_args, mock_run_monitors):
        with patch('sys.argv', ['main', '--input_run_date', '2023-12-25']):
            mock_parse_args.return_value = argparse.Namespace(input_run_date='2023-12-25')
            import monitor
            monitor.run_monitors('2023-12-25')
            mock_run_monitors.assert_called_once_with('2023-12-25')

    #test the case when no date is provided
    @patch('monitor.run_monitors')
    @patch('monitor.argparse.ArgumentParser.parse_args')
    def test_no_input_date_provided(self, mock_parse_args, mock_run_monitors):
        with patch('monitor.datetime') as mock_datetime:
            mock_date = datetime.datetime(2024, 7, 26)
            mock_datetime.now.return_value = mock_date
            def mock_strftime(format):
                return mock_date.strftime(format)
            mock_datetime.now.strftime = mock_strftime
            mock_parse_args.return_value = argparse.Namespace(input_run_date=None)
            import monitor
            monitor.run_monitors(mock_date.strftime("%Y-%m-%d"))
            mock_run_monitors.assert_called_once_with(mock_date.strftime("%Y-%m-%d"))
    #manual test case
    @patch('monitor.argparse.ArgumentParser.parse_args')
    def test_output_on_specific_date(self, mock_parse_args):
        mock_parse_args.return_value = argparse.Namespace(input_run_date='2023-12-25')
        expected_output = (
            "no transactions for alert channel:  jira\n"
            "skipping monitor 'Monthly Email Alert - Spending Deviation' based on frequency\n"
            "Sending stack alert to #fraud-team:[('cef2f12b-9c36-49f1-962a-468689abaf0e', '7fbee373-0043-49c0-ba19-612713ed2363', -525.82), "
            "('5d7aa9e2-c686-4a7e-927f-a207b0a12016', '0e017dcd-114d-4aa7-853a-b70972176840', -611.95), "
            "('d953dc43-1e42-4787-941c-fabcc8dcc905', '747ff4e7-5bca-4327-bb7c-4d5c20256284', -633.21)]\n"
        )
        with patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            import monitor
            with patch('monitor.datetime') as mock_datetime:
                mock_date = datetime.datetime(2023, 12, 25)
                mock_datetime.now.return_value = mock_date
                def mock_strftime(format):
                    return mock_date.strftime(format)
                monitor.run_monitors(mock_date.strftime("%Y-%m-%d"))
                
            self.assertEqual(mock_stdout.getvalue(), expected_output)
if __name__ == '__main__':
    unittest.main()