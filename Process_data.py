
import pandas as pd
import numpy as np

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime,timedelta

import os
from pathlib import Path
# Note: sender email and app password is required

# Sender and receiver
sender_email = "gm540445@gmail.com"
password = "pgwg tiyq jojq qjsf"

def run(folder_path):

    #Folder Path
    src = folder_path+"/form_data.xlsx"
    dst = folder_path+"/RAW_DATA/BeforeArrival/BA_RAW.xlsx"
    
    #Updatation Date
    today = datetime.today().date()

    #Read Excel: form data
    df = pd.read_excel(src)

    #Adding last updated date row
    df["Last_Updated_Date"] = today

    # Providing to Ship IDs to new data from form
    try:

        old = pd.read_excel(dst)

        old["prefix"] = old["Ship_ID"].str[0]
        old["postfix"] = old["Ship_ID"].str[1:].astype(int)

        new = pd.concat([old, df], ignore_index=True)
        new_mask = new["Ship_ID"].isna()
        max_map = old.groupby("prefix")["postfix"].max().to_dict()

        new.loc[new_mask, "prefix"] = new.loc[new_mask, "Direction"].str[0]
        for p in new.loc[new_mask, "prefix"].unique():
            mask = new_mask & (new["prefix"] == p)
            count = mask.sum()

            if count == 0:
                continue

            start = max_map.get(p, 0)   
            seq = pd.Series(
                range(start + 1, start + 1 + count),
                index=new.loc[mask].index,
                dtype="int64"
            )

            new.loc[mask, "postfix"] = seq
        print("done1")
        new["postfix"] = new["postfix"].astype(int)
        print("done2")
        new["Ship_ID"] = new["prefix"] + new["postfix"].astype(str)
        print("done3")
        new = new.drop(columns=["prefix", "postfix"])
        print("done4")

    except FileNotFoundError:
        new = df.copy()     
        new["prefix"] = new["Direction"].str[0]
        new["postfix"] = (new.groupby("prefix").cumcount() + 1).astype(int)
        new["Ship_ID"] = new["prefix"] + new["postfix"].astype(str)
        new = new.drop(columns=["prefix", "postfix"])



    # Saving it to Raw File(before)
    print(new)
    with pd.ExcelWriter(dst, engine="openpyxl") as writer:
        new.to_excel(writer, index=False)
    
    #Clearing up the form file
    with pd.ExcelWriter(src, engine="openpyxl") as writer:
        pd.DataFrame().to_excel(writer, index=False)

    print("run")
    #Read Excel: RAW DATA of before arrival
    raw_data = pd.read_excel(folder_path+"/RAW_DATA/BeforeArrival/BA_RAW.xlsx")

    #Removeing Duplicates Ship_ID
    raw_data['prefix'] = (raw_data['Ship_ID'].str)[0]
    raw_data['postfix'] = ((raw_data['Ship_ID'].str)[1:]).astype(int)

    max_postfix = raw_data.groupby('prefix')['postfix'].transform('max')
    dup_mask = raw_data.duplicated(subset=['Ship_ID'], keep='first')
    dup_index = raw_data[dup_mask].groupby('prefix').cumcount() + 1

    raw_data.loc[dup_mask, 'postfix'] = max_postfix[dup_mask] + dup_index
    raw_data["Ship_ID"] = raw_data['prefix']+raw_data['postfix'].astype(str)

    raw_data = raw_data.drop(columns=['prefix','postfix'])

    #Finding duplicate Phone Number
    duplicate_data = raw_data.duplicated(subset=["Phone No."], keep=False)
    #Finding nan Value
    null_mask = raw_data.isnull().any(axis=1)

    #duplicated phone number and rows having nan values
    combined_mask = null_mask | duplicate_data
    #Accepted entires of vessels
    needed_mask = ~combined_mask

    #Filter data to access new added data and not to disturb old data
    raw_data["Last_Updated_Date"] = pd.to_datetime(raw_data["Last_Updated_Date"], errors="coerce").dt.date
    max = raw_data["Last_Updated_Date"].sort_values(ascending=False).unique()

    if(len(max)>=2):
        second_max = max[1]
    else:
        second_max = max[0] - timedelta(days=1)

    raw_data['Arrival Date'] = pd.to_datetime(raw_data['Arrival Date'], errors='coerce').dt.date

    
    email_data  = raw_data[raw_data["Arrival Date"] > second_max]
    
    #Finding duplicate Phone Number
    duplicate_data = email_data.duplicated(subset=["Phone No."], keep=False)
    #Finding nan Value
    null_mask = email_data.isnull().any(axis=1)
    combined_mask = null_mask | duplicate_data

    #Sending Email to Incorrect Data
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(sender_email, password)
    for idx, row in email_data[combined_mask].iterrows():
        null_columns = ", ".join(row[row.isnull()].index.tolist())
        is_duplicate = duplicate_data.loc[idx]
        receiver_email = row["Email"]
        receiver_email = "abcg@gmail.com" # ============For Testing

        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = receiver_email
        msg["Subject"] = "Suez Canal: Missing Information"

        body = ""

        if(is_duplicate and null_columns!=""):
            body = f"""
                    <!doctype html>
                    <html>
                    <head>
                        <meta charset="utf-8" />
                        <title>Action required: Missing info & duplicate phone</title>
                    </head>
                    <body style="margin:0; padding:0; background-color:#f4f6f8; font-family: Arial, Helvetica, sans-serif;">

                        <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f6f8; padding:20px 0;">
                            <tr>
                                <td align="center">
                                    <table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff; border-radius:10px; overflow:hidden; box-shadow:0 8px 30px rgba(0,0,0,0.05);">

                                        <!-- Header -->
                                        <tr>
                                            <td style="background:linear-gradient(90deg, #7b1c1c, #b33939); padding:20px; color:#ffffff;">
                                                <h2 style="margin:0; font-size:22px;">Important: Missing information & duplicate phone number</h2>
                                            </td>
                                        </tr>


                                        <!-- Content -->
                                        <tr>
                                            <td style="padding:30px; color:#333333; font-size:15px; line-height:1.6;">
                                                <p style="margin-top:0;">Dear User,</p>

                                                <p>We found two issues with your record (Phone: <em>{row['Phone No.']}</em>):</p>

                                                <ol style="padding-left:20px;">
                                                    <li>
                                                        <strong>Missing fields:</strong>
                                                        <ul>
                                                            {null_columns}
                                                        </ul>
                                                    </li>
                                                    <li>
                                                        <strong>Duplicate phone number:</strong> This phone number appears in another record in our system.
                                                    </li>
                                                </ol>

                                                <p>Please update the missing information and provide a unique phone number.</p>

                                                <p style="margin-bottom:0;">Thank you,<br><strong>Data Team</strong></p>
                                            </td>
                                        </tr>

                                    </table>
                                </td>
                            </tr>
                        </table>

                    </body>
                    </html>
                    """
        elif(is_duplicate):
            body = f"""
                    <!doctype html>
                    <html>
                    <head>
                        <meta charset="utf-8" />
                        <title>Duplicate phone number</title>
                    </head>
                    <body style="margin:0; padding:0; background-color:#f4f6f8; font-family: Arial, Helvetica, sans-serif;">

                        <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f6f8; padding:20px 0;">
                            <tr>
                                <td align="center">
                                    <table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff; border-radius:10px; overflow:hidden; box-shadow:0 8px 30px rgba(0,0,0,0.05);">

                                        <!-- Header -->
                                        <tr>
                                            <td style="background:linear-gradient(90deg, #b45309, #dd6b20); padding:20px; color:#ffffff;">
                                                <h2 style="margin:0; font-size:22px;">Notice: Duplicate phone number found</h2>
                                            </td>
                                        </tr>

                                    

                                        <!-- Content -->
                                        <tr>
                                            <td style="padding:30px; color:#333333; font-size:15px; line-height:1.6;">
                                                <p style="margin-top:0;">Dear User,</p>

                                                <p>Our system detected that the phone number <strong>{row['Phone No.']}</strong> is already associated with another record in our database.</p>

                                                <p>To avoid confusion, please provide an alternate phone number.</p>

                                                <p style="margin-bottom:0;">Regards,<br><strong>Data Team</strong></p>
                                            </td>
                                        </tr>

                                    </table>
                                </td>
                            </tr>
                        </table>

                    </body>
                    </html>
                    """
        else: 
            body = f"""
                    <!doctype html>
                    <html>
                    <head>
                        <meta charset="utf-8" />
                        <title>Missing information</title>
                    </head>
                    <body style="margin:0; padding:0; background-color:#f4f6f8; font-family: Arial, Helvetica, sans-serif;">

                        <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f6f8; padding:20px 0;">
                            <tr>
                                <td align="center">
                                    <table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff; border-radius:10px; overflow:hidden; box-shadow:0 8px 30px rgba(0,0,0,0.05);">

                                        <!-- Header -->
                                        <tr>
                                            <td style="background:linear-gradient(90deg, #2b6cb0, #3182ce); padding:20px; color:#ffffff;">
                                                <h2 style="margin:0; font-size:22px;">Action required: Missing information in your record</h2>
                                            </td>
                                        </tr>

    

                                        <!-- Content -->
                                        <tr>
                                            <td style="padding:30px; color:#333333; font-size:15px; line-height:1.6;">
                                                <p style="margin-top:0;">Dear Client,</p>

                                                <p>We noticed that the following information is <strong>missing</strong> from your record (Phone: <em>{row['Phone No.']}</em>):</p>

                                                <ul>
                                                    {null_columns}
                                                </ul>

                                                <p>Please update the missing fields at your earliest convenience so we can proceed.</p>

                                                <p style="margin-bottom:0;">Thanks,<br><strong>Data Team</strong></p>
                                            </td>
                                        </tr>

                                    </table>
                                </td>
                            </tr>
                        </table>

                    </body>
                    </html>
                    """

        if(body!=""):

            msg.attach(MIMEText(body, "html"))

            
            #server.send_message(msg)
            
    print("ALL Email sent successfully!")

    server.quit()

    #Finding duplicate Phone Number
    duplicate_data = raw_data.duplicated(subset=["Phone No."], keep=False)

    #Finding nan Value
    null_mask = raw_data.isnull().any(axis=1)
    combined_mask = null_mask | duplicate_data
    
    #Saving Data to File
    file_path = folder_path+"\\CLEAN_DATA\\BeforeArrival\\BA_CLEAN.xlsx"
    null_file_path = folder_path+"\\CLEAN_DATA\\BeforeArrival\\NNBA_CLEAN.xlsx"
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        raw_data[needed_mask].to_excel(writer, index=False)
    with pd.ExcelWriter(null_file_path, engine="openpyxl") as writer:
        raw_data[combined_mask].to_excel(writer, index=False)
    print("File created")

    #Processing the data
    #Read Excel: Taking cleaned raw data
    df = pd.read_excel(folder_path+"\\CLEAN_DATA\\BeforeArrival\\BA_CLEAN.xlsx")

    #Filter data to access only new data
    df["Last_Updated_Date"] = pd.to_datetime(df["Last_Updated_Date"], errors="coerce").dt.date
    max = df["Last_Updated_Date"].sort_values(ascending=False).unique()
    
    if(len(max)>=2):
        second_max = max[1]
    else:
        second_max = max[0] - timedelta(days=1)

    # Convert Arrival Date to date only
    df['Arrival Date'] = pd.to_datetime(df['Arrival Date'], errors='coerce').dt.date

    df  = df[df["Arrival Date"] > second_max]
    print("\nColumns in Excel:", df.columns, "\n")


    # TRANSIT TIME CALCULATIONS
    DIST_BEFORE = 50   # km
    DIST_DUAL   = 72   # km
    DIST_AFTER  = 45   # km

    df["Transit_time_before"] = DIST_BEFORE / (df['Speed (knots)'] * 1.852)
    df["Transit_time_after"] = DIST_AFTER / (df['Speed (knots)'] * 1.852)
    df["Transit_time_between"] = DIST_DUAL / (df['Speed (knots)'] * 1.852)

    df["total"] = (
        df["Transit_time_before"] +
        df["Transit_time_between"] +
        df["Transit_time_after"]
    )
    
    # CONVOY GROUP 
    
    FAST_SPEED = 13
    HIGH_LENGTH = 250
    HIGH_BEAM = 40
    HIGH_DRAFT = 14
    HIGH_SCGT = 80000

    def assign_convoy(row):

        N3_Cargo = ["Crude Oil", "LNG", "LPG", "Chemical", "Product Tanker"]
        
        d = str(row["Direction"]).strip().upper()
        cargo = row["Cargo Type"]
        length= row['Length (m)']
        beam = row['Beam(Max Width) (m)']
        draft = row['Draft(depth) (m)']
        scgt = row['SCGT(Volume) (tons)']

        if d == "" or d == "NAN":
            return "Unknown"  

        direction = d[0]       

        speed = row["Speed (knots)"]

        if cargo in N3_Cargo:
            return direction +"3"
        # ---- N1 / S1 (FAST) ----
        elif (speed > FAST_SPEED) and (length<HIGH_LENGTH) and (beam< HIGH_BEAM) and (draft<HIGH_DRAFT) and (scgt<HIGH_SCGT):
            return direction + "1"

        # ---- N3 / S3 (SLOW) ----
        else:
            return direction + "2"

    # Apply convoy group
    df["Convoy Group"] = df.apply(assign_convoy, axis=1)


   
    # SPLIT INTO DATE-WISE FILES
    dates = set(df["Arrival Date"].dropna())
    print("\nUnique Dates Found:", dates)
    
    folder = folder_path+"\\PROCESSED_DATA\\BeforeArrival"
    os.makedirs(folder, exist_ok=True)

    for date in dates:
        save_path = f"{folder}/{date}.xlsx"
        with pd.ExcelWriter(save_path, engine="openpyxl") as writer:
            df[df["Arrival Date"] == date].to_excel(writer, index=False)
        
        #Making Raw data for After Arrival
        raw_folder = folder_path+"\\RAW_DATA\\AfterArrival"
        dst = f"{raw_folder}/{date}.xlsx"
        with pd.ExcelWriter(dst, engine="openpyxl") as writer:
            df[df["Arrival Date"] ==date].to_excel(writer, index=False)
        

    print("Date-wise files saved into 'text' folder.")


    # def hhmm_to_hours(hhmm):
    #     h, m = map(int, hhmm.split(":"))
    #     return h + m / 60

    # df["total_hr"] = df["Total_transit_time"].apply(hhmm_to_hours)

#Creating body for email to send schdule 
def build_schedule_email(s_df, report_date):
    table_html = s_df.to_html(
        index=False,
        border=0,
        justify="center",
        classes="schedule-table"
    )

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <meta charset="UTF-8">
    <style>
        body {{
            margin: 0;
            padding: 0;
            background-color: #f4f6f8;
            font-family: "Segoe UI", Roboto, Arial, sans-serif;
            color: #333;
        }}
        .container {{
            max-width: 900px;
            margin: 30px auto;
            background: #ffffff;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 4px 15px rgba(0,0,0,0.08);
        }}
        .header {{
            background: linear-gradient(135deg, #003366, #005fa3);
            color: white;
            padding: 24px 30px;
        }}
        .header h1 {{
            margin: 0;
            font-size: 22px;
            font-weight: 600;
        }}
        .header p {{
            margin: 6px 0 0;
            font-size: 14px;
            opacity: 0.9;
        }}
        .content {{
            padding: 30px;
            font-size: 14px;
            line-height: 1.6;
        }}
        .content p {{
            margin: 0 0 16px;
        }}
        .highlight {{
            background: #f0f6ff;
            padding: 14px 18px;
            border-left: 4px solid #005fa3;
            margin: 20px 0;
            border-radius: 4px;
        }}
        table.schedule-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            font-size: 13px;
        }}
        table.schedule-table th {{
            background: #005fa3;
            color: white;
            padding: 10px;
            text-align: center;
            font-weight: 600;
        }}
        table.schedule-table td {{
            padding: 9px;
            text-align: center;
            border-bottom: 1px solid #e5e7eb;
        }}
        table.schedule-table tr:nth-child(even) {{
            background-color: #f9fbfd;
        }}
        .footer {{
            background: #f4f6f8;
            padding: 18px 30px;
            font-size: 12px;
            color: #666;
            text-align: center;
        }}
    </style>
    </head>

    <body>
        <div class="container">
            <div class="header">
                <h1>Suez Canal Transit Schedule</h1>
                <p>Operational planning & vessel coordination report</p>
            </div>

            <div class="content">
                <p>Dear Team,</p>

                <p>
                    Please find below the prepared transit schedule based on the latest
                    vessel data and operational constraints for <strong>{report_date}</strong>.
                </p>

                <div class="highlight">
                    <strong>Note:</strong>  
                    The schedule is calculated considering vessel direction, slowest vessel
                    constraints, and sectional transit times to ensure safe and efficient
                    passage planning.
                </div>

                {table_html}


                <p>
                    Best regards,<br>
                    <strong>Operations & Planning Team</strong>
                </p>
            </div>

            <div class="footer">
                This is an automated operational email.  
                Please do not reply unless clarification is required.
            </div>
        </div>
    </body>
    </html>
    """
    return html

# Convert hours into hours and minutes
def proper_hhmm(hours):
    total_minutes = round(hours * 60)
    h, m = divmod(total_minutes, 60)
    return f"{h:02d}:{m:02d}"

#Create schdule 
def schdule(df,north_start=None,south_start = None):
    Start_routine = 5

    #blueprint for excel file
    s_df = pd.DataFrame(columns=["Direction", "Scheduled Time", "StartTime", "Before_section", "Wait Time", "Dual Crossed Time", "DestTime"])
    
    #Finding slowest for each direction
    slowest_per_direction = (
        df.loc[df.groupby("Direction")["Speed (knots)"].idxmin()]
    )

    #Finding in which direction is for arriving 
    slowest_per_direction["Direct"] = slowest_per_direction["Direction"].str[0]
    directions_present = set(slowest_per_direction["Direct"])
    report_date = ""

    #Finding Schdule and storing in s_df
    if {'N', 'S'}.issubset(directions_present):
        max_row = slowest_per_direction.loc[slowest_per_direction["Transit_time_before"].idxmax()]
        min_row = slowest_per_direction.loc[slowest_per_direction["Transit_time_before"].idxmin()]
        max_time = max_row["Transit_time_before"]
        max_direction = max_row["Direction"]
        report_date = str(max_row["Arrival Date"])

        basedOn = max_direction
        if(north_start!= None and basedOn == "Northbound"):
            Start_routine = north_start
        if(south_start!= None and basedOn == "Southbound"):
            Start_routine = south_start

        startTime = Start_routine+max_time- min_row['Transit_time_before']
        if(north_start!= None and  min_row['Direction'] == "Northbound"):
            startTime = north_start
        if(south_start!= None and  min_row['Direction'] == "Southbound"):
            startTime = south_start
        Reached_time = max(Start_routine + max_time,startTime+min_row["Transit_time_before"])
        schTime_BasedOn = proper_hhmm(Start_routine - 1)
        startTime_BasedOn = proper_hhmm(Start_routine)
        BeforeTime_BasedOn = proper_hhmm(Start_routine+max_time) 
        WaitTime_BasedOn = proper_hhmm(Reached_time-(Start_routine+max_time))
        DualTime_BasedON = proper_hhmm(Reached_time+min_row["Transit_time_between"])
        destTime_BasedOn = proper_hhmm(Reached_time +min_row["Transit_time_between"]+ max_row["Transit_time_after"])
        row = {
            "Direction": basedOn,
            "Scheduled Time": schTime_BasedOn,
            "StartTime": startTime_BasedOn,
            "Before_section": BeforeTime_BasedOn,
            "Wait Time": WaitTime_BasedOn,
            "Dual Crossed Time": DualTime_BasedON,
            "DestTime": destTime_BasedOn
        }

        s_df.loc[len(s_df)] = row

        basedOn = min_row['Direction']
        startTime = Start_routine+max_time- min_row['Transit_time_before']
        if(north_start!= None and basedOn == "Northbound"):
            startTime = north_start
        if(south_start!= None and basedOn == "Southbound"):
            startTime = south_start
        schTime_BasedOn = proper_hhmm(startTime - 1)
        startTime_BasedOn = proper_hhmm(startTime)
        BeforeTime_BasedOn = proper_hhmm(startTime+ min_row['Transit_time_before']) 
        WaitTime_BasedOn = proper_hhmm(Reached_time -(startTime+ min_row['Transit_time_before']) )
        DualTime_BasedON = proper_hhmm(Reached_time+ min_row['Transit_time_between'])
        destTime_BasedOn = proper_hhmm(Reached_time+ min_row['Transit_time_between']+min_row["Transit_time_after"])
        row = {
            "Direction": basedOn,
            "Scheduled Time": schTime_BasedOn,
            "StartTime": startTime_BasedOn,
            "Before_section": BeforeTime_BasedOn,
            "Wait Time": WaitTime_BasedOn,
            "Dual Crossed Time": DualTime_BasedON,
            "DestTime": destTime_BasedOn
        }

        s_df.loc[len(s_df)] = row
    elif ('N' in directions_present) or ('S' in directions_present):
        max_row = slowest_per_direction.loc[slowest_per_direction["Transit_time_before"].idxmax()]
        report_date = str(max_row["Arrival Date"])
        max_time = max_row["Transit_time_before"]
        max_direction = max_row["Direction"]

        basedOn = max_direction
        if(north_start!= None and basedOn == "Northbound"):
            Start_routine = north_start
        if(south_start!= None and basedOn == "Southbound"):
            Start_routine = south_start
        schTime_BasedOn = proper_hhmm(Start_routine - 1)
        startTime_BasedOn = proper_hhmm(Start_routine)
        BeforeTime_BasedOn = proper_hhmm(Start_routine+max_time) 
        WaitTime_BasedOn = proper_hhmm(0)
        DualTime_BasedON = proper_hhmm(Start_routine+max_time+max_row["Transit_time_between"])
        destTime_BasedOn = proper_hhmm(Start_routine + max_row["total"])
        row = {
            "Direction": basedOn,
            "Scheduled Time": schTime_BasedOn,
            "StartTime": startTime_BasedOn,
            "Before_section": BeforeTime_BasedOn,
            "Wait Time": WaitTime_BasedOn,
            "Dual Crossed Time": DualTime_BasedON,
            "DestTime": destTime_BasedOn
        }

        s_df.loc[len(s_df)] = row


    else:
        print("No valid direction found")

  
    #Sending schdule on email 
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(sender_email, password)
    for idx, row in df.iterrows():
        receiver_email = row["Email"]
        receiver_email = "abcg@gmail.com" # ============For Testing

        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = receiver_email
        msg["Subject"] = "Suez Canal: Transit Schedule"

        body = build_schedule_email(s_df, report_date)

        msg.attach(MIMEText(body, "html"))

                
        #server.send_message(msg)
    server.quit()

    return s_df


def run_schdule(folder_path):
    src = folder_path + "/PROCESSED_DATA/BeforeArrival"
    dst = folder_path + "/SCH_DATA/Before_Arrival"
    
    src_path = Path(src)
    dst_path =  Path(dst)

    #  DST FILES
    dst_files = [f.name for f in dst_path.iterdir() if f.is_file()]

    dst_dates = []
    for f in dst_files:
        try:
            dst_dates.append(datetime.strptime(f[:-5], "%Y-%m-%d"))
        except ValueError:
            print("Skipping invalid DST filename:", f)

    if dst_dates==[]:
        max_dst_date = None
    else:
        max_dst_date = max(dst_dates)

    # SRC FILES 
    src_files = [f.name for f in src_path.iterdir() if f.is_file()]
    filtered_src_files = []
    if(dst_dates!=[]):
        if max_dst_date:
            for f in src_files:
                try:
                    file_date = datetime.strptime(f[:-5], "%Y-%m-%d")
                    if file_date > max_dst_date:
                        filtered_src_files.append(f)
                except ValueError:
                    print("Skipping invalid SRC filename:", f)
    else:
        filtered_src_files = src_files
    #Finding Schdule for each day in filtered source files
    for file_name in filtered_src_files:
        file_path = src_path / file_name

        if file_path.exists():
            df = pd.read_excel(file_path)
            print(file_path)
            sch_df = schdule(df)
            end_path = dst_path / file_name
            with pd.ExcelWriter(end_path, engine="openpyxl") as writer:
                sch_df.to_excel(writer, index=False)

        else:
            print("Missing file:", file_name)

import pandas as pd
import datetime as dt

def time_to_hours(val):
    if pd.isna(val):
        return None

    if isinstance(val, pd.Timestamp):
        return val.hour + val.minute / 60

    if isinstance(val, dt.time):
        return val.hour + val.minute / 60

    if isinstance(val, str):
        h, m = map(int, val.split(":"))
        return h + m / 60

    if isinstance(val, (int, float)):
        total_minutes = val * 24 * 60
        return int(total_minutes // 60) + (total_minutes % 60) / 60

    raise ValueError(f"Unsupported time format: {val}")

def run_schduleA(folder_path):
    src = folder_path + "/PROCESSED_DATA/AfterArrival/OnTime"
    dst = folder_path + "/SCH_DATA/After_Arrival"
    
    src_path = Path(src)
    dst_path =  Path(dst)

    #  DST FILES
    dst_files = [f.name for f in dst_path.iterdir() if f.is_file()]

    dst_dates = []

    for f in dst_files:
        try:
            dst_dates.append(datetime.strptime(f[:-5], "%Y-%m-%d"))
        except ValueError:
            print("Skipping invalid DST filename:", f)

    if dst_dates==[]:
        max_dst_date = None
    else:
        max_dst_date = max(dst_dates)



    # SRC FILES 
    src_files = [f.name for f in src_path.iterdir() if f.is_file()]
    filtered_src_files = []
    if(dst_dates!=[]):
        if max_dst_date:
            for f in src_files:
                try:
                    file_date = datetime.strptime(f[:-5], "%Y-%m-%d")
                    if file_date > max_dst_date:
                        filtered_src_files.append(f)
                except ValueError:
                    print("Skipping invalid SRC filename:", f)
    else:
        filtered_src_files = src_files

    for file_name in filtered_src_files:
        file_path = src_path / file_name

        if file_path.exists():

            df = pd.read_excel(file_path)
            sch_time = (
                df.loc[df.groupby("Direction")["Scheduled Time"].idxmin()]
            )   
            north_series = sch_time.loc[sch_time["Direction"] == "Northbound", "Scheduled Time"]
            south_series = sch_time.loc[sch_time["Direction"] == "Southbound", "Scheduled Time"]

            north_start = time_to_hours(north_series.iloc[0])+1 if not north_series.empty else None
            south_start = time_to_hours(south_series.iloc[0])+1 if not south_series.empty else None
            
            print(file_path)
            sch_df = schdule(df,north_start,south_start)
            end_path = dst_path / file_name
            with pd.ExcelWriter(end_path, engine="openpyxl") as writer:
                sch_df.to_excel(writer, index=False)

        else:
            print("Missing file:", file_name)

def calculate_fine(minutes_late):
    if minutes_late <= 0:
        return 0
    elif minutes_late <= 10:
        return 20000          
    elif minutes_late <= 30:
        return 60000       
    elif minutes_late <= 60:
        return 100000
    else:
        return 20000
    
def run_A(folder_path):
    src = folder_path+"/RAW_DATA/AfterArrival"
    dst = folder_path+"/CLEAN_DATA/AfterArrival"
    pro_ontime_dst = folder_path+"/PROCESSED_DATA/AfterArrival/OnTime"
    pro_late_dst = folder_path+"/PROCESSED_DATA/AfterArrival/Late"

    src_path = Path(src)
    dst_path =  Path(dst)
    pro_ontime_dst_path = Path(pro_ontime_dst)
    pro_late_dst_path = Path(pro_late_dst)

    # DST FILES 
    dst_files = [f.name for f in dst_path.iterdir() if f.is_file()]

    dst_dates = []
    for f in dst_files:
        try:
            dst_dates.append(datetime.strptime(f[:-5], "%Y-%m-%d"))
        except ValueError:
            print("Skipping invalid DST filename:", f)

    if dst_dates==[]:
        max_dst_date = None
    else:
        max_dst_date = max(dst_dates)
    
    #SRC FILES
    src_files = [f.name for f in src_path.iterdir() if f.is_file()]
    filtered_src_files = []
    if(dst_dates!=[]):
        if max_dst_date:
            for f in src_files:
                try:
                    file_date = datetime.strptime(f[:-5], "%Y-%m-%d")
                    if file_date > max_dst_date:
                        filtered_src_files.append(f)
                except ValueError:
                    print("Skipping invalid SRC filename:", f)
    else:
        filtered_src_files = src_files
    
    for file_name in filtered_src_files:
        file_path = src_path / file_name

        if file_path.exists():
            sch_path = Path(folder_path + "/SCH_DATA/Before_Arrival")/file_name
            if(sch_path.exists()):
                df = pd.read_excel(file_path)
                sch_df = pd.read_excel(sch_path)
                df = df.merge(
                    sch_df[["Direction", "Scheduled Time"]],
                    on="Direction",
                    how="left"
                )
                df["Scheduled_dt"] = pd.to_datetime(df["Scheduled Time"], format="%H:%M")


                random_offset = pd.to_timedelta(
                    np.random.randint(-60, 61, size=len(df)),
                    unit="m"
                )

                df["Arrival_dt"] = df["Scheduled_dt"] + random_offset
                df["Arrival Time"] = df["Arrival_dt"].dt.strftime("%H:%M")
                df.drop(columns=["Scheduled_dt", "Arrival_dt"], inplace=True)
                print(df)

                #Storing clean Data
                end_path = dst_path / file_name
                with pd.ExcelWriter(end_path, engine="openpyxl") as writer:
                    df.to_excel(writer, index=False)
                
                #Processing clean data
                df["Scheduled_dt"] = pd.to_datetime(df["Scheduled Time"], format="%H:%M")
                df["Arrival_dt"]   = pd.to_datetime(df["Arrival Time"], format="%H:%M")  
                df["Diff_minutes"] = (
                    (df["Arrival_dt"] - df["Scheduled_dt"])
                    .dt.total_seconds() / 60
                ).round().astype(int)
                df.drop(columns=["Scheduled_dt", "Arrival_dt"], inplace=True)
                df["On Time"] = df["Diff_minutes"]<=10
                df["Fine Amount"] = df["Diff_minutes"].apply(calculate_fine)               
                
                print(df)
                #storing Processed data
                end_path = pro_ontime_dst_path / file_name
                on_time_df = df[df["On Time"]]
                if(not on_time_df.empty):
                    with pd.ExcelWriter(end_path, engine="openpyxl") as writer:
                        on_time_df.to_excel(writer, index=False)
                on_late_df = df[~df["On Time"]] 
                if(not on_late_df.empty):
                    end_path = pro_late_dst_path/file_name
                    with pd.ExcelWriter(end_path, engine="openpyxl") as writer:
                        on_late_df.to_excel(writer, index=False)

                print(file_name)
                #Sending schdule on email 
                server = smtplib.SMTP("smtp.gmail.com", 587)
                server.starttls()
                server.login(sender_email, password)
                for idx, row in df[~df["On Time"]].iterrows():
                    receiver_email = row["Email"]
                    receiver_email = "abcg@gmail.com" # ============For Testing

                    msg = MIMEMultipart()
                    msg["From"] = sender_email
                    msg["To"] = receiver_email
                    msg["Subject"] = "Suez Canal: Rescheduling & Delay Penalty"
                    
                    body = """<!DOCTYPE html>
                            <html>
                            <head>
                            <meta charset="UTF-8">
                            <style>
                                body {{
                                    margin: 0;
                                    padding: 0;
                                    background-color: #f4f6f8;
                                    font-family: "Segoe UI", Roboto, Arial, sans-serif;
                                    color: #333;
                                }}
                                .container {{
                                    max-width: 800px;
                                    margin: 30px auto;
                                    background: #ffffff;
                                    border-radius: 10px;
                                    overflow: hidden;
                                    box-shadow: 0 4px 15px rgba(0,0,0,0.08);
                                }}
                                .header {{
                                    background: linear-gradient(135deg, #7a1f1f, #a83232);
                                    color: white;
                                    padding: 22px 30px;
                                }}
                                .header h1 {{
                                    margin: 0;
                                    font-size: 20px;
                                    font-weight: 600;
                                }}
                                .content {{
                                    padding: 28px 30px;
                                    font-size: 14px;
                                    line-height: 1.7;
                                }}
                                .content p {{
                                    margin: 0 0 16px;
                                }}
                                .notice-box {{
                                    background: #fff4f4;
                                    border-left: 4px solid #a83232;
                                    padding: 16px 18px;
                                    margin: 20px 0;
                                    border-radius: 4px;
                                }}
                                .details {{
                                    background: #f8fafc;
                                    padding: 16px 18px;
                                    border-radius: 6px;
                                    margin: 20px 0;
                                    font-size: 13px;
                                }}
                                .details strong {{
                                    display: inline-block;
                                    width: 160px;
                                }}
                                .footer {{
                                    background: #f4f6f8;
                                    padding: 16px 30px;
                                    font-size: 12px;
                                    color: #666;
                                    text-align: center;
                                }}
                            </style>
                            </head>

                            <body>
                            <div class="container">
                                <div class="header">
                                    <h1>Notice of Rescheduling & Delay Penalty</h1>
                                </div>

                                <div class="content">
                                    <p>Dear Sir / Madam,</p>

                                    <p>
                                        This is to inform you that your vessel could not be accommodated
                                        in the originally assigned transit slot due to arrival after the
                                        scheduled time.
                                    </p>

                                    <div class="notice-box">
                                        As per operational regulations and traffic coordination requirements,
                                        the vessel has been <strong>rescheduled for transit on the next available day</strong>.
                                    </div>

                                    <p>
                                        Additionally, in accordance with the applicable delay penalty provisions,
                                        a fine has been imposed based on the extent of the delay beyond the scheduled
                                        arrival time.
                                    </p>

                                    <div class="details">
                                        <p><strong>Scheduled Arrival:</strong> {SCHEDULED_TIME}</p>
                                        <p><strong>Actual Arrival:</strong> {ARRIVAL_TIME}</p>
                                        <p><strong>Delay Duration:</strong> {DELAY_MINUTES} minutes</p>
                                        <p><strong>Fine Amount:</strong> {FINE_AMOUNT} dollars</p>
                                    </div>

                                    <p>
                                        Kindly ensure settlement of the above fine amount at the earliest
                                        to avoid any further operational delays or restrictions.
                                    </p>

                                    <p>
                                        Yours sincerely,<br>
                                        <strong>Transit Operations Department</strong><br>
                                        Canal Traffic & Scheduling Authority
                                    </p>
                                </div>

                                <div class="footer">
                                    This is an automated operational notification.  
                                    Please do not reply to this email.
                                </div>
                            </div>
                            </body>
                            </html>
                            """.format(
                                       SCHEDULED_TIME = row["Scheduled Time"],
                                       ARRIVAL_TIME = row["Arrival Time"],
                                       DELAY_MINUTES = row["Diff_minutes"],
                                       FINE_AMOUNT = row["Fine Amount"])

                    msg.attach(MIMEText(body, "html"))
                            
                    #server.send_message(msg)
                    
                    
                for idx, row in df[(df["On Time"]) & (df["Fine Amount"] != 0)].iterrows():
                    receiver_email = row["Email"]
                    receiver_email = "abcg@gmail.com" # ============For Testing

                    msg = MIMEMultipart()
                    msg["From"] = sender_email
                    msg["To"] = receiver_email
                    msg["Subject"] = "Suez Canal: Transit Schedule"

                    body = """<!DOCTYPE html>
                            <html lang="en">
                            <head>
                                <meta charset="UTF-8">
                                <title>Operational Clearance with Delay Fine</title>
                                <style>
                                    body {{
                                        font-family: "Segoe UI", Roboto, Arial, sans-serif;
                                        background-color: #f4f6f8;
                                        margin: 0;
                                        padding: 0;
                                    }}
                                    .container {{
                                        max-width: 720px;
                                        margin: 40px auto;
                                        background-color: #ffffff;
                                        border-radius: 8px;
                                        overflow: hidden;
                                        box-shadow: 0 6px 18px rgba(0,0,0,0.08);
                                    }}
                                    .header {{
                                        background: linear-gradient(90deg, #0b5ed7, #084298);
                                        color: #ffffff;
                                        padding: 24px;
                                    }}
                                    .header h1 {{
                                        margin: 0;
                                        font-size: 22px;
                                        font-weight: 600;
                                    }}
                                    .content {{
                                        padding: 28px;
                                        color: #333333;
                                        line-height: 1.6;
                                        font-size: 15px;
                                    }}
                                    .highlight {{
                                        background-color: #fff3cd;
                                        border-left: 5px solid #ffca2c;
                                        padding: 14px;
                                        margin: 18px 0;
                                        border-radius: 4px;
                                    }}
                                    .fine-box {{
                                        background-color: #f8f9fa;
                                        border: 1px dashed #adb5bd;
                                        padding: 16px;
                                        margin: 20px 0;
                                        border-radius: 6px;
                                        font-weight: 600;
                                    }}
                                    .footer {{
                                        background-color: #f1f3f5;
                                        padding: 18px;
                                        font-size: 13px;
                                        color: #555555;
                                    }}
                                    .footer strong {{
                                        color: #000000;
                                    }}
                                </style>
                            </head>
                            <body>

                            <div class="container">

                                <div class="header">
                                    <h1>Operational Clearance — Delay Penalty Applicable</h1>
                                </div>

                                <div class="content">

                                    <p>Dear Vessel Operator / Agent,</p>

                                    <p>
                                        This is to inform you that your vessel has arrived later than the 
                                        scheduled operational time window.
                                    </p>

                                    <div class="highlight">
                                        <strong>Important:</strong> Despite the delay, the vessel is 
                                        <strong>permitted to proceed with today operation</strong>.
                                    </div>

                                    <p>
                                        As per operational guidelines, a delay penalty is applicable due to 
                                        arrival beyond the approved schedule.
                                    </p>

                                    <div class="fine-box">
                                        Fine Amount Payable: <br>
                                        <span style="font-size:18px;"> {FINE_AMOUNT} dollars</span>
                                    </div>

                                    <p>
                                        Kindly ensure that the applicable fine is settled at the earliest to 
                                        avoid any administrative or operational inconvenience in future movements.
                                    </p>

                                    <p>
                                        Please treat this notice as an official operational communication.
                                    </p>

                                    <p>
                                        For any clarification or assistance, feel free to contact the operations team.
                                    </p>

                                    <p>
                                        Regards,<br>
                                        <strong>Operations Control Team</strong><br>
                                        Transit & Scheduling Department
                                    </p>

                                </div>

                                <div class="footer">
                                    This is a system-generated operational notice.  
                                    Please do not reply to this email.
                                </div>

                            </div>

                            </body>
                            </html>
                            """.format(FINE_AMOUNT = row["Fine Amount"])

                    msg.attach(MIMEText(body, "html"))

                            
                    #server.send_message(msg)
                server.quit()


        else:
            print("Missing file:", file_name)

