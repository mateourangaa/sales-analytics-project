# this simulates sales activity (I don't have access to a real sales team haha)

import pandas as pd
import random
from datetime import datetime, timedelta
import os

def generateData():

    # csv file in folder
    file_path = "live_deal_data.csv"

    # how many new deals that will generate each time this file is ran
    new_deal_batch = 5

        # used to set up the data frame to evetually export the data in a CSV file
    try:
        deals_df = pd.read_csv(file_path)
    except FileNotFoundError:
        deals_df = pd.DataFrame(columns=["deal_id","account_id","deal_stage","stage_entry_date","deal_value","close_date"])


    # this will generate the random sales data
    for i in range(new_deal_batch):

        # deal id
        most_recent_deal_id = deals_df.deal_id.max() + 1 
        if pd.isna(most_recent_deal_id):
            deal_id = 1000
        else:
            deal_id = int(most_recent_deal_id) + 1

        # account id
        account_id = random.randint(100, 300)

        # stage
        stage = random.choice(["Prospect", "Demo", "Proposal", "Closed"])

        # stage date
        stage_date = datetime.today() - timedelta(days=random.randint(0,90))

        # deal value
        deal_value = random.randint(17000, 100000)

        # generates deal closure date under 'close_date' 
        # (ONLY IF THE DEAL HAS CLOSED)
        if stage == "Closed":
            days_to_close = random.randint(5, 30)
            close_date = (stage_date + timedelta(days=days_to_close)).date()
        else:
            close_date = None
        
        deals_df = pd.concat([deals_df, pd.DataFrame([{
            "deal_id": deal_id,
            "account_id": account_id,
            "deal_stage": stage,
            "stage_entry_date": stage_date.date(),
            "deal_value": deal_value,
            "close_date": close_date
        }])], ignore_index=True)



    # appends the newly generated data to the CSV file
    deals_df.to_csv(file_path, index=False)



