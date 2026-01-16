# this pushes the simulated sales data from the other file to the cloud-hosted database

import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
load_dotenv()


conn = psycopg2.connect(
    host=os.getenv("SUPABASE_HOST"),
    database=os.getenv("SUPABASE_DB"),
    user=os.getenv("SUPABASE_USER"),
    password=os.getenv("SUPABASE_PASSWORD"),
    port=os.getenv("SUPABASE_PORT")
)

cursor = conn.cursor()


df = pd.read_csv("live_deal_data.csv")

df["stage_entry_date"] = df["stage_entry_date"].where(pd.notna(df["stage_entry_date"]), None)
df["close_date"] = df["close_date"].where(pd.notna(df["close_date"]), None)

for _, row in df.iterrows():
    cursor.execute(
        """
        INSERT INTO deals (deal_id, account_id, deal_stage, stage_entry_date, deal_value, close_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (deal_id) DO NOTHING;
        """,
        (
            row["deal_id"],
            row["account_id"],
            row["deal_stage"],
            row["stage_entry_date"],
            row["deal_value"],
            row["close_date"]
        )
    )

conn.commit()
cursor.close()
conn.close()


