#!/bin/ python3

# UPDATED 22-JUNE-2024
# TO DO: 
# ADD BREAKDOWN BY PROGRAM
# ADD REGION FUNCTION BASED ON COUNTRY CODE
# ADD CONVERSION % - SHOULD USE A FUNCTION THAT TAKES THE REPORTING WEEK AND SUBTRACTS 365 DAYS
# ADD ACTIVE PIPELINE


import pandas as pd
from datetime import datetime, date, timedelta

# Load the cases.csv file
csv_path = 'cases.csv'
cases_df = pd.read_csv(csv_path, parse_dates=['create_date', 'fulfilled_date'])


# MAINLY USED FOR QBR TABLES
current_date = datetime.today().strftime('%m/%d/%Y')
current_month = int(datetime.today().month)
current_year = int(datetime.today().year)
previous_year = current_year - 1

def reporting_month(month):
    if month < 4:
        current_quarter = 1
        previous_quarter = 4
    elif 3 < month < 7:
        current_quarter = 2
        previous_quarter = 1
    elif 6 < month < 10:
        current_quarter = 3
        previous_quarter = 2
    else:
        current_quarter = 4
        previous_quarter = 3
    return current_quarter, previous_quarter

reporting_quarters = reporting_month(current_month)
current_quarter, previous_quarter = reporting_quarters

# Define the quarter start and end dates
quarter_dates = {
    1: (datetime(current_year, 1, 1), datetime(current_year, 3, 31)),
    2: (datetime(current_year, 4, 1), datetime(current_year, 6, 30)),
    3: (datetime(current_year, 7, 1), datetime(current_year, 9, 30)),
    4: (datetime(current_year, 10, 1), datetime(current_year, 12, 31)),
    5: (datetime(previous_year, 1, 1), datetime(previous_year, 3, 31)),
    6: (datetime(previous_year, 4, 1), datetime(previous_year, 6, 30)),
    7: (datetime(previous_year, 7, 1), datetime(previous_year, 9, 30)),
    8: (datetime(previous_year, 10, 1), datetime(previous_year, 12, 31))
}

# Get the current and previous quarter date ranges
current_start_date, current_end_date = quarter_dates[current_quarter]
previous_start_date, previous_end_date = quarter_dates[previous_quarter]

# GROUP  COUNTRY CODES INTO REGIONS
na = ["US", "CA", "MX"]
sa = ["AR", "BO", "BR", "CL", "CO", "EC", "FK", "GF", "GY", "PE", "PY", "SR", "UY", "VE"]
asean = ["AF", "AM", "AZ", "BH", "BD", "BT", "BN", "KH", "CN", "CY", "GE", "IN", "ID", "IR", "IQ", "IL", "JP", "JO", "KZ", "KP", "KR", "KW", "KG", "LA", "LB", "MY", "MV", "MN", "MM", "NP", "OM", "PK", "PH", "QA", "SA", "SG", "LK", "SY", "TJ", "TH", "TL", "TM", "AE", "UZ", "VN", "YE"]
apac = ["AU", "JP"]
emea = ["AL", "AD", "AT", "BY", "BE", "BA", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IS", "IE", "IT", "LV", "LI", "LT", "LU", "MT", "MD", "MC", "ME", "NL", "MK", "NO", "PL", "PT", "RO", "RU", "SM", "RS", "SK", "SI", "ES", "SE", "CH", "UA", "GB", "VA", "DZ", "AO", "BJ", "BW", "BF", "BI", "CM", "CV", "CF", "TD", "KM", "CG", "CD", "DJ", "EG", "GQ", "ER", "SZ", "ET", "GA", "GM", "GH", "GN", "GW", "CI", "KE", "LS", "LR", "LY", "MG", "MW", "ML", "MR", "MU", "MA", "MZ", "NA", "NE", "NG", "RW", "ST", "SN", "SC", "SL", "SO", "ZA", "SS", "SD", "TZ", "TG", "TN", "UG", "ZM", "ZW", "BH", "IQ", "IL", "JO", "KW", "LB", "OM", "PS", "QA", "SA", "SY", "AE", "YE"]
eu = ["AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE"]



# Filter the cases data for the specified date ranges
request_period = cases_df[(cases_df['create_date'] >= current_start_date) & (cases_df['create_date'] <= current_end_date)]
request_compare_period = cases_df[(cases_df['create_date'] >= previous_start_date) & (cases_df['create_date'] <= previous_end_date)]
complete_period = cases_df[(cases_df['fulfilled_date'] >= current_start_date) & (cases_df['fulfilled_date'] <= current_end_date)]
complete_compare_period = cases_df[(cases_df['fulfilled_date'] >= previous_start_date) & (cases_df['fulfilled_date'] <= previous_end_date)]




def calculate_metrics(df, region):
    metrics = {}
    if region == "WW":
     
        metrics['Survey Request'] = df[df['type'] == 'Survey'].shape[0]
        metrics['Survey Complete'] = df[df['type'] == 'Survey'].shape[0]
        metrics['Survey Cycle Time'] = df[df['type'] == 'Survey']['cycle_time'].mean()
        metrics['Install'] = df[df['type'] == 'Install'].shape[0]
        metrics['Install Cycle Time'] = df[df['type'] == 'Install']['cycle_time'].mean()
        metrics['Install Failure %'] = df[df['status'] == 'Canceled'].shape[0] 
        metrics['Decomm'] = df[df['type'] == 'Decomm'].shape[0]
        metrics['Decomm Cycle Time'] = df[df['type'] == 'Decomm']['cycle_time'].mean()
        return metrics
    
    else:
        filtered_df = df[df['region'] == region] 
        metrics['Survey Request'] = filtered_df[filtered_df['type'] == 'Survey'].shape[0]
        metrics['Survey Complete'] = filtered_df[filtered_df['type'] == 'Survey'].shape[0]
        metrics['Survey Cycle Time'] = filtered_df[filtered_df['type'] == 'Survey']['cycle_time'].mean()
        metrics['Install'] = filtered_df[filtered_df['type'] == 'Install'].shape[0]
        metrics['Install Cycle Time'] = filtered_df[filtered_df['type'] == 'Install']['cycle_time'].mean()
        metrics['Install Failure %'] = filtered_df[filtered_df['status'] == 'Canceled'].shape[0] 
        metrics['Decomm'] = filtered_df[filtered_df['type'] == 'Decomm'].shape[0]
        metrics['Decomm Cycle Time'] = filtered_df[filtered_df['type'] == 'Decomm']['cycle_time'].mean()
        return metrics

def calculate_request_metrics(df, region):
    metrics = {}
    if region == "WW":
        metrics['Survey Request'] = df[df['type'] == 'Survey'].shape[0]
        metrics['Decomm Request'] = df[df['type'] == 'Decomm'].shape[0]
        return metrics
    else:
        filtered_df = df[df['region'] == region]
        metrics['Survey Request'] =  df[df['type'] == 'Survey'].shape[0]
        metrics['Decomm Request'] =  df[df['type'] == 'Decomm'].shape[0]
        return metrics




request_metrics_ww =  calculate_request_metrics(request_period, "WW")
current_metrics_ww = calculate_metrics(complete_period, "WW")
compare_metrics_ww = calculate_metrics(complete_compare_period, "WW")
request_compare_ww =  calculate_request_metrics(request_compare_period, "WW")

request_metrics_eu = calculate_request_metrics(request_period, 'EU')
current_metrics_eu = calculate_metrics(complete_period, 'EU')
compare_metrics_eu = calculate_metrics(complete_compare_period, 'EU')
request_compare_eu = calculate_request_metrics(request_compare_period, 'EU')

request_metrics_na = calculate_request_metrics(request_period, 'North America')
current_metrics_na = calculate_metrics(complete_period, 'North America')
compare_metrics_na = calculate_metrics(complete_compare_period, 'North America')
request_compare_na = calculate_request_metrics(request_compare_period, 'NA')
# Function to calculate percent change
def percent_change(metric, current, previous):
    if previous == 0 or previous is None:
        return None
    
    elif metric == 'cycletime':
        ct = ((current - previous) / previous) < 0
        if ct < 0:
            return ct
        else:
            return ct * -1
            
    else:
        ct = ((current - previous) / previous) < 0
        if ct < 0:
            return ct * -1
        else:
            return ct

# Calculate percent changes for EU
ww_survey_request_qoq = percent_change("cycletime", request_metrics_ww['Survey Request'], request_compare_ww['Survey Request'])
ww_survey_qoq = percent_change("count", current_metrics_ww['Survey Complete'], compare_metrics_ww['Survey Complete'])
ww_cycle_time_qoq = percent_change("cycletime", current_metrics_ww['Survey Cycle Time'], compare_metrics_ww['Survey Cycle Time'])
ww_install_qoq = percent_change("count", current_metrics_ww['Install'], compare_metrics_ww['Install'])
ww_install_cycle_time_qoq = percent_change("cycletime", current_metrics_ww['Install Cycle Time'], compare_metrics_ww['Install Cycle Time'])
ww_failure_qoq = percent_change("count", current_metrics_ww['Install'], compare_metrics_ww['Install'])
ww_decomm_qoq = percent_change("count", current_metrics_ww['Decomm'], compare_metrics_ww['Decomm'])
ww_decomm_request_qoq = percent_change("count", request_metrics_ww['Decomm Request'],request_compare_ww['Decomm Request'])
ww_decomm_cycle_time_qoq = percent_change("cycletime", current_metrics_ww['Decomm Cycle Time'], compare_metrics_ww['Decomm Cycle Time'])


eu_survey_request_qoq = percent_change("cycletime", request_metrics_eu['Survey Request'], request_compare_eu['Survey Request'])
eu_survey_qoq = percent_change("count", current_metrics_eu['Survey Complete'], compare_metrics_eu['Survey Complete'])
eu_cycle_time_qoq = percent_change("cycletime", current_metrics_eu['Survey Cycle Time'], compare_metrics_eu['Survey Cycle Time'])
eu_install_qoq = percent_change("count", current_metrics_eu['Install'], compare_metrics_eu['Install'])
eu_install_cycle_time_qoq = percent_change("cycletime", current_metrics_eu['Install Cycle Time'], compare_metrics_eu['Install Cycle Time'])
eu_failure_qoq = percent_change("count", current_metrics_eu['Install'], compare_metrics_eu['Install'])
eu_decomm_qoq = percent_change("count", current_metrics_eu['Decomm'], compare_metrics_eu['Decomm'])
eu_decomm_request_qoq = percent_change("count", request_metrics_eu['Decomm Request'],request_compare_eu['Decomm Request'])
eu_decomm_cycle_time_qoq = percent_change("cycletime", current_metrics_eu['Decomm Cycle Time'], compare_metrics_eu['Decomm Cycle Time'])



na_survey_request_qoq = percent_change("cycletime", request_metrics_na['Survey Request'], request_compare_na['Survey Request'])
na_survey_qoq = percent_change("count", current_metrics_na['Survey Complete'], compare_metrics_na['Survey Complete'])
na_cycle_time_qoq = percent_change("cycletime", current_metrics_na['Survey Cycle Time'], compare_metrics_na['Survey Cycle Time'])
na_install_qoq = percent_change("count", current_metrics_na['Install'], compare_metrics_na['Install'])
na_install_cycle_time_qoq = percent_change("cycletime", current_metrics_na['Install Cycle Time'], compare_metrics_na['Install Cycle Time'])
na_failure_qoq = percent_change("count", current_metrics_na['Install'], compare_metrics_na['Install'])
na_decomm_qoq = percent_change("count", current_metrics_na['Decomm'], compare_metrics_na['Decomm'])
na_decomm_request_qoq = percent_change("count", request_metrics_na['Decomm Request'],request_compare_na['Decomm Request'])
na_decomm_cycle_time_qoq = percent_change("cycletime", current_metrics_na['Decomm Cycle Time'], compare_metrics_na['Decomm Cycle Time'])

# Create the output DataFrame
metrics = [
    'Survey Request', 'Survey Complete', 'Survey Cycle Time',
    'Install', 'Install Cycle Time', 'Install Failure %',
    'Decomm Request','Decomm', 'Decomm Cycle Time'
]
ww_values = [
    request_metrics_ww.get('Survey Request'), current_metrics_ww.get('Survey Complete'), current_metrics_ww.get('Survey Cycle Time'),
    current_metrics_ww.get('Install'), current_metrics_ww.get('Install Cycle Time'), current_metrics_na.get('Install Failure %'),
    request_metrics_ww.get('Decomm Request'), current_metrics_ww.get('Decomm'), current_metrics_ww.get('Decomm Cycle Time')
]
eu_values = [
    request_metrics_eu.get('Survey Request'), current_metrics_eu.get('Survey Complete'), current_metrics_eu.get('Survey Cycle Time'),
    current_metrics_eu.get('Install'), current_metrics_eu.get('Install Cycle Time'), current_metrics_na.get('Install Failure %'),
    request_metrics_eu.get('Decomm Request'), current_metrics_eu.get('Decomm'), current_metrics_eu.get('Decomm Cycle Time')
]

ww_qoq_values = [
    ww_survey_request_qoq,
    ww_survey_qoq, 
    ww_cycle_time_qoq, 
    ww_install_qoq, 
    ww_install_cycle_time_qoq,
    ww_failure_qoq, 
    ww_decomm_request_qoq,
    ww_decomm_qoq, 
    ww_decomm_cycle_time_qoq
]

eu_qoq_values = [
    eu_survey_request_qoq,
    eu_survey_qoq, 
    eu_cycle_time_qoq, 
    eu_install_qoq, 
    eu_install_cycle_time_qoq,
    eu_failure_qoq, 
    eu_decomm_request_qoq,
    eu_decomm_qoq, 
    eu_decomm_cycle_time_qoq
]

na_values = [
    request_metrics_na.get('Survey Request'), current_metrics_na.get('Survey Complete'), current_metrics_na.get('Survey Cycle Time'),
    current_metrics_na.get('Install'), current_metrics_na.get('Install Cycle Time'), current_metrics_na.get('Install Failure %'),
    request_metrics_na.get('Decomm Request'),current_metrics_na.get('Decomm'), current_metrics_na.get('Decomm Cycle Time')
]

na_qoq_values = [
    na_survey_request_qoq,
    na_survey_qoq, 
    na_cycle_time_qoq, 
    na_install_qoq,
    na_install_cycle_time_qoq,
    na_failure_qoq, 
    na_decomm_request_qoq,
    na_decomm_qoq, 
    na_decomm_cycle_time_qoq
]

output_df = pd.DataFrame({
    'Metric': metrics,
    'WW - Q2': ww_values,
    'WW QoQ': ww_qoq_values,
    'EU - Q2': eu_values,
    'EU QoQ': eu_qoq_values,
    'NA - Q2': na_values,
    'NA - QoQ': na_qoq_values
})

print(f'                            ')
print(f'                            ')
print(f'--------QBR TABLE-----------')
print(f'----------------------------')
print(f'                            ')

def display_dataframe_to_user(name: str, dataframe: pd.DataFrame) -> None:
  #  print(f"Displaying DataFrame: {name}")
    print(dataframe.to_string(index=False))

display_dataframe_to_user("Output Metrics", output_df)

print(f'                            ')
print(f'                            ')
print(f'--------MBR TABLE-----------')
print(f'----------------------------')
print(f'       in progress          ')
print(f'                            ')
print(f'                            ')
print(f'                            ')


# ------------------ WBR ------------------  
# USED FOR WBR TABLES
import pandas as pd
from datetime import datetime, timedelta

# Function to calculate percent change
def percent_change(metric, current, previous):
    if previous == 0 or previous is None:
        return None
    elif metric == 'Decomm Cycle Time' or metric == 'Survey Cycle Time' or  metric == 'Install Cycle Time':
        return round(((current - previous) / previous), 2) * -1
    else:
        return round(((current - previous) / previous), 2)



    
    

# Function to calculate metrics for a specific region
def calculate_metrics_region(df, region):
    metrics = {}
    region_df = df if region == "WW" else df[df['region'] == region]
    
    metrics['Survey Request'] = region_df[region_df['type'] == 'Survey'].shape[0]
    metrics['Survey Complete'] = region_df[region_df['type'] == 'Survey']['fulfilled_date'].notnull().sum()
    metrics['Survey Cycle Time'] = region_df[region_df['type'] == 'Survey']['cycle_time'].mean()
    
    metrics['Install'] = region_df[region_df['type'] == 'Install'].shape[0]
    metrics['Install Cycle Time'] = region_df[region_df['type'] == 'Install']['cycle_time'].mean()
    
    install_total = region_df[region_df['type'] == 'Install'].shape[0]
    install_failed = region_df[(region_df['type'] == 'Install') & (region_df['status'] == 'Failed')].shape[0]
    metrics['Install Failure %'] = (install_failed / install_total) if install_total > 0 else 0
    
    metrics['Decomm Request'] = region_df[region_df['type'] == 'Decomm'].shape[0]
    metrics['Decomm'] = region_df[region_df['type'] == 'Decomm']['fulfilled_date'].notnull().sum()
    metrics['Decomm Cycle Time'] = region_df[region_df['type'] == 'Decomm']['cycle_time'].mean()
    
    # Handle cases where the mean of cycle times might result in NaN
    for key in ['Survey Cycle Time', 'Install Cycle Time', 'Decomm Cycle Time']:
        if pd.isna(metrics[key]):
            metrics[key] = 0
    
    return metrics

# Define the metrics
wbr_metrics = [
    'Survey Request', 'Survey Complete', 'Survey Cycle Time',
    'Install', 'Install Cycle Time', 'Install Failure %',
    'Decomm Request','Decomm', 'Decomm Cycle Time'
]

# Define regions
regions = ["WW", "North America", "EU"]

# Calculate metrics for each region
for region in regions:
    # Calculate metrics for the previous 6 weeks
    today = datetime.today() - timedelta(days=7)
    week_dates = [today - timedelta(weeks=i) for i in range(6)]
    week_dates = week_dates[::-1]

    week_metrics = {}
    for i, week_date in enumerate(week_dates):
        week_start = week_date - timedelta(days=week_date.weekday())
        week_end = week_start + timedelta(days=6)
        week_data = cases_df[(cases_df['create_date'] >= week_start) & (cases_df['create_date'] <= week_end)]
        
        metrics = calculate_metrics_region(week_data, region)
        week_metrics[f'WK{week_date.isocalendar().week}'] = [
            round(metrics.get('Survey Request', 0), 0), round(metrics.get('Survey Complete', 0), 0), round(metrics.get('Survey Cycle Time', 0), 2),
            round(metrics.get('Install', 0), 0), round(metrics.get('Install Cycle Time', 0), 2), round(metrics.get('Install Failure %', 0), 2),
            round(metrics.get('Decomm Request', 0), 0), round(metrics.get('Decomm', 0), 0), round(metrics.get('Decomm Cycle Time', 0), 2)
        ]

    # Calculate WoW changes
    wow_values = []
    for metric in wbr_metrics:
        current_week = week_metrics[f'WK{week_dates[-1].isocalendar().week}'][wbr_metrics.index(metric)]
        previous_week = week_metrics[f'WK{week_dates[-2].isocalendar().week}'][wbr_metrics.index(metric)]
        wow_values.append(percent_change(metric, current_week, previous_week))

    # Calculate metrics for the previous 6 months
    months_back = 6
    first_day_of_current_month = today.replace(day=1)
    month_dates = [first_day_of_current_month]
    for _ in range(1, months_back):
        first_day_of_current_month = (first_day_of_current_month - timedelta(days=1)).replace(day=1)
        month_dates.append(first_day_of_current_month)
    month_dates = month_dates[::-1]

    month_metrics = {}
    for month_date in month_dates:
        month_start = month_date.replace(day=1)
        next_month = month_start + timedelta(days=31)
        month_end = next_month.replace(day=1) - timedelta(days=1)
        month_data = cases_df[(cases_df['create_date'] >= month_start) & (cases_df['create_date'] <= month_end)]
        
        metrics = calculate_metrics_region(month_data, region)
        month_metrics[month_start.strftime("%b")] = [
            round(metrics.get('Survey Request', 0), 0), round(metrics.get('Survey Complete', 0), 0), round(metrics.get('Survey Cycle Time', 0), 2),
            round(metrics.get('Install', 0), 0), round(metrics.get('Install Cycle Time', 0), 2), round(metrics.get('Install Failure %', 0), 2),
            round(metrics.get('Decomm Request', 0), 0), round(metrics.get('Decomm', 0), 0), round(metrics.get('Decomm Cycle Time', 0), 2)
        ]

    # Calculate MoM changes
    mom_values = []
    for metric in wbr_metrics:
        current_month = month_metrics[month_dates[-1].strftime("%b")][wbr_metrics.index(metric)]
        previous_month = month_metrics[month_dates[-2].strftime("%b")][wbr_metrics.index(metric)]
        mom_values.append(percent_change(metric, current_month, previous_month))

    # Calculate MTD, QTD, YTD
    current_month_start = today.replace(day=1)
    current_month_data = cases_df[(cases_df['create_date'] >= current_month_start) & (cases_df['create_date'] <= today)]
    metrics = calculate_metrics_region(current_month_data, region)
    # mtd_values = [
    #     round(metrics.get('Survey Request', 0), 0), round(metrics.get('Survey Complete', 0), 0), round(metrics.get('Survey Cycle Time', 0), 2),
    #     round(metrics.get('Install', 0), 0), round(metrics.get('Install Cycle Time', 0), 2), round(metrics.get('Install Failure %', 0), 2),
    #     round(metrics.get('Decomm Request', 0), 0), round(metrics.get('Decomm', 0), 0), round(metrics.get('Decomm Cycle Time', 0), 2)
    # ]

    current_quarter_start = current_month_start.replace(month=(current_month_start.month - (current_month_start.month - 1) % 3))
    current_quarter_data = cases_df[(cases_df['create_date'] >= current_quarter_start) & (cases_df['create_date'] <= today)]
    metrics = calculate_metrics_region(current_quarter_data, region)
    qtd_values = [
        round(metrics.get('Survey Request', 0), 0), round(metrics.get('Survey Complete', 0), 0), round(metrics.get('Survey Cycle Time', 0), 2),
        round(metrics.get('Install', 0), 0), round(metrics.get('Install Cycle Time', 0), 2), round(metrics.get('Install Failure %', 0), 2),
        round(metrics.get('Decomm Request', 0), 0), round(metrics.get('Decomm', 0), 0), round(metrics.get('Decomm Cycle Time', 0), 2)
    ]

    current_year_start = current_month_start.replace(month=1, day=1)
    current_year_data = cases_df[(cases_df['create_date'] >= current_year_start) & (cases_df['create_date'] <= today)]
    metrics = calculate_metrics_region(current_year_data, region)
    ytd_values = [
        round(metrics.get('Survey Request', 0), 0), round(metrics.get('Survey Complete', 0), 0), round(metrics.get('Survey Cycle Time', 0), 2),
        round(metrics.get('Install', 0), 0), round(metrics.get('Install Cycle Time', 0), 2), round(metrics.get('Install Failure %', 0), 2),
        round(metrics.get('Decomm Request', 0), 0), round(metrics.get('Decomm', 0), 0), round(metrics.get('Decomm Cycle Time', 0), 2)
    ]
    # Create the DataFrame
    columns = ['Metric'] + [f'WK{week_dates[i].isocalendar().week}' for i in range(6)] + ['WoW'] + [month.strftime("%b") for month in month_dates] + [
        'MoM',
        # 'MTD',
        'QTD', 
        'YTD'
        ]
    data = {
        'Metric': wbr_metrics,
        'WoW': wow_values,
        'MoM': mom_values,
        # 'MTD': mtd_values,
        'QTD': qtd_values,
        'YTD': ytd_values
    }
    for i, week in enumerate([f'WK{week_dates[i].isocalendar().week}' for i in range(6)]):
        data[week] = [week_metrics[week][wbr_metrics.index(metric)] for metric in wbr_metrics]

    for i, month in enumerate([month.strftime("%b") for month in month_dates]):
        data[month] = [month_metrics[month][wbr_metrics.index(metric)] for metric in wbr_metrics]

    weekly_df = pd.DataFrame(data, columns=columns)

    def display_dataframe_to_user(name: str, dataframe: pd.DataFrame) -> None:
        print(f"Displaying DataFrame: {name}")
        print(dataframe.to_string(index=False))

    print(' ')
    print(f'--------{region} WBR TABLE-----------')
    print('-------------------------------')
    display_dataframe_to_user(f"Weekly Metrics for {region}", weekly_df)