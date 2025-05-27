import os
from clickhouse_driver import Client
from datetime import datetime
from slack_sdk.webhook import WebhookClient
from dotenv import load_dotenv

load_dotenv()

print(os.getenv('CLICKHOUSE_HOST'))
# ClickHouse connection using environment variables from GitHub Secrets
clickhouse_client = Client(
    host=os.getenv('CLICKHOUSE_HOST'),
    user=os.getenv('CLICKHOUSE_USER'),
    password=os.getenv('CLICKHOUSE_PASSWORD'),
    database='casino_dbt_source',
    secure=True
)

# Slack webhook client
slack = WebhookClient(os.getenv('SLACK_WEBHOOK_URL'))

def fetch_betting_data():
    query = """
    SELECT 
        e.PlayerId AS user_id,
        p.Email AS nickname,
        c.Name AS game,
        e.CreatedAt AS time,
        e.BetAmount AS bet_amount,
        e.WinAmount AS win_amount
    FROM 
        casino_dbt_source.external_casino_rounds_info e
    JOIN 
        casino_dbt_source.casino_games_for_reports c 
        ON c.Id = e.CasinoGameId 
    JOIN 
        casino_dbt_source.players p 
        ON p.ID = e.PlayerId
    WHERE 
        e.CurrencyId = 33
        AND e.Status = 2
        AND e.CreatedAt >= now() - INTERVAL 1 HOUR
        AND (
            e.WinAmount >= 50000
            OR (e.BetAmount >= 20000 AND e.WinAmount = 0)
        )
    ORDER BY 
        e.CreatedAt DESC
    """
    try:
        result = clickhouse_client.execute(query)
        return result
    except Exception as e:
        print(f"ClickHouse query failed: {str(e)}")
        raise

def format_slack_message(data):
    if not data:
        return {
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Hourly Betting Activity Alert - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\nNo significant betting activity in the last hour."
                    }
                }
            ]
        }

    headers = ["User ID", "Nickname", "Game", "Time", "Bet Amount", "Win Amount"]
    table_rows = []
    for row in data:
        user_id, nickname, game, time, bet_amount, win_amount = row
        time_str = time.strftime("%Y-%m-%d %H:%M:%S")
        table_rows.append(f"|{user_id}|{nickname}|{game}|{time_str}|${bet_amount:,.2f}|${win_amount:,.2f}|")

    table = "\n".join([f"|{'|'.join(headers)}|"] + ["|-" * len(headers) + "|"] + table_rows)
    
    return {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Hourly Betting Activity Alert - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n```{table}```"
                }
            }
        ]
    }

def send_slack_alert():
    try:
        data = fetch_betting_data()
        slack_message = format_slack_message(data)
        response = slack.send(
            text="Hourly Betting Activity Alert",
            blocks=slack_message["blocks"]
        )
        if response.status_code == 200:
            print("Alert sent successfully")
        else:
            print(f"Failed to send alert: {response.status_code}, {response.body}")
            raise Exception(f"Slack API error: {response.body}")
    except Exception as e:
        print(f"Error in send_slack_alert: {str(e)}")
        raise

if __name__ == "__main__":
    send_slack_alert()