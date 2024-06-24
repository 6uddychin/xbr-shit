import pandas as pd
import datetime
from datetime import datetime, date, timedelta

# Load the cases.csv file
csv_path = 'cases3.csv'
cases_df = pd.read_csv(csv_path, parse_dates=['create_date', 'fulfilled_date'])

# Ensure 'product_type' exists in the DataFrame
if 'product_type' not in cases_df.columns:
    print("Error: 'product_type' column is missing in the CSV file.")
else:

    # WBR Reporting Week
    today = datetime.today()
    weeknum = today.isocalendar()[1]
    report_week = weeknum - 1

    # Define regions
    # regions = ["WW", "USA", "EU"]
    regions = [
        'JP',
        'AT',
        'IT',
        'FR',
        'DE',
        'US',
        'PT',
        'ES',
        'CA',
        'MX',
        'AU'
    ]
    # countries = cases_df['Country_Code']

    # Function to calculate percent change
    def percent_change(metric, current, previous):
        if previous == 0 or previous is None:
            return None
        elif metric in ['Decomm Cycle Time', 'Survey Cycle Time', 'Install Cycle Time']:
            return round(((current - previous) / previous), 2) * -1
        else:
            return round(((current - previous) / previous), 2)

    wbr_metrics = [
        'Survey Request',
        'Locker Survey Request',
        'Apt Locker Pro Survey Request',
        'Apt Locker Survey Request',
        'Survey Complete',
        'Locker Survey Complete',
        'Apt Locker Pro Survey Complete',
        'Apt Locker Survey Complete',
        'Install',
        'Locker Install',
        'Apt Locker Pro Install',
        'Apt Locker Install',
        'Install Cycle Time',
        'Locker Install Cycle Time',
        'Apt Locker Pro Install Cycle Time',
        'Apt Locker Install Cycle Time',
        'Install Failure %',
        'Locker Install Failure %',
        'Apt Locker Pro Install Failure %',
        'Apt Locker Install Failure %',
        'Decomm Request',
        'Locker Decomm Request',
        'Apt Locker Pro Decomm Request',
        'Apt Locker Decomm Request',
        'Decomm',
        'Locker Decomm',
        'Apt Locker Pro Decomm',
        'Apt Locker Decomm',
        'Decomm Cycle Time',
        'Locker Decomm Cycle Time',
        'Apt Locker Pro Decomm Cycle Time',
        'Apt Locker Decomm Cycle Time'
    ]

    def calculate_metrics(df, region):
        metrics = {}
        region_df = df if region == "WW" else df[df['region'] == region]


        # Survey Requests
        metrics['Apt Locker Survey Request'] = region_df[(region_df['type'] == 'Survey') & (region_df['product_type'] == 'Apt Locker')].shape[0]
        metrics['Apt Locker Pro Survey Request'] = region_df[(region_df['type'] == 'Survey') & (region_df['product_type'] == 'Apt Locker Pro')].shape[0]

        # Survey Completions
        metrics['Survey Complete'] = region_df[(region_df['type'] == 'Survey') & (region_df['fulfilled_date'].notnull())].shape[0]
        metrics['Apt Locker Survey Complete'] = region_df[(region_df['type'] == 'Survey') & (region_df['product_type'] == 'Apt Locker') & (region_df['fulfilled_date'].notnull())].shape[0]
        metrics['Apt Locker Pro Survey Complete'] = region_df[(region_df['type'] == 'Survey') & (region_df['product_type'] == 'Apt Locker Pro') & (region_df['fulfilled_date'].notnull())].shape[0]

        # Survey Cycle Times
        metrics['Survey Cycle Time'] = region_df[region_df['type'] == 'Survey']['cycle_time'].mean()
        metrics['Apt Locker Survey Cycle Time'] = region_df[(region_df['type'] == 'Survey') & (region_df['product_type'] == 'Apt Locker')]['cycle_time'].mean()
        metrics['Apt Locker Pro Survey Cycle Time'] = region_df[(region_df['type'] == 'Survey') & (region_df['product_type'] == 'Apt Locker Pro')]['cycle_time'].mean()

        # Install Requests
        metrics['Install'] = region_df[region_df['type'] == 'Install'].shape[0]
        metrics['Apt Locker Install'] = region_df[(region_df['type'] == 'Install') & (region_df['product_type'] == 'Apt Locker')].shape[0]
        metrics['Apt Locker Pro Install'] = region_df[(region_df['type'] == 'Install') & (region_df['product_type'] == 'Apt Locker Pro')].shape[0]

        # Decommission Requests
        metrics['Decomm'] = region_df[region_df['type'] == 'Decomm'].shape[0]
        metrics['Apt Locker Decomm'] = region_df[(region_df['type'] == 'Decomm') & (region_df['product_type'] == 'Apt Locker')].shape[0]
        metrics['Apt Locker Pro Decomm'] = region_df[(region_df['type'] == 'Decomm') & (region_df['product_type'] == 'Apt Locker Pro')].shape[0]

        # Install Cycle Times
        metrics['Install Cycle Time'] = region_df[region_df['type'] == 'Install']['cycle_time'].mean()
        metrics['Apt Locker Install Cycle Time'] = region_df[(region_df['type'] == 'Install') & (region_df['product_type'] == 'Apt Locker')]['cycle_time'].mean()
        metrics['Apt Locker Pro Install Cycle Time'] = region_df[(region_df['type'] == 'Install') & (region_df['product_type'] == 'Apt Locker Pro')]['cycle_time'].mean()

        # Decommission Cycle Times
        metrics['Decomm Cycle Time'] = region_df[region_df['type'] == 'Decomm']['cycle_time'].mean()
        metrics['Apt Locker Decomm Cycle Time'] = region_df[(region_df['type'] == 'Decomm') & (region_df['product_type'] == 'Apt Locker')]['cycle_time'].mean()
        metrics['Apt Locker Pro Decomm Cycle Time'] = region_df[(region_df['type'] == 'Decomm') & (region_df['product_type'] == 'Apt Locker Pro')]['cycle_time'].mean()

        return metrics
    

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
        
        metrics = calculate_metrics(week_data, region)
        week_metrics[f'WK{week_date.isocalendar().week}'] = [ 
            round(metrics.get('Survey Request',0), 0), 
            round(metrics.get('Locker Survey Request', 0),0),
            round(metrics.get('Apt Locker Pro Survey Request', 0), 0), 
            round(metrics.get('Apt Locker Survey Request', 0), 0), 
            round(metrics.get('Survey Complete ', 0), 0), 
            round(metrics.get('Locker Survey Complete ', 0), 0), 
            round(metrics.get('Apt Locker Pro Survey Complete ', 0), 0), 
            round(metrics.get('Apt Locker Survey Complete ', 0), 0), 
            round(metrics.get('Install ', 0), 0), 
            round(metrics.get('Locker Install ', 0), 0), 
            round(metrics.get('Apt Locker Pro Install ', 0), 0), 
            round(metrics.get('Apt Locker Install ', 0), 0), 
            round(metrics.get('Install Failure %', 0), 0), 
            round(metrics.get('Locker Install Failure %', 0), 0), 
            round(metrics.get('Apt Locker Pro Install Failure %', 0), 0), 
            round(metrics.get('Apt Locker Install Failure %', 0), 0), 
            round(metrics.get('Decomm Request', 0), 0), 
            round(metrics.get('Locker Decomm Request', 0), 0), 
            round(metrics.get('Apt Locker Pro Decomm Request', 0), 0), 
            round(metrics.get('Apt Locker Decomm Request', 0), 0), 
            round(metrics.get('Decomm ', 0), 0), 
            round(metrics.get('Locker Decomm ', 0), 0), 
            round(metrics.get('Apt Locker ProDecomm ', 0), 0), 
            round(metrics.get('Apt Locker Decomm ', 0), 0), 
            round(metrics.get('Install Cycle Time ',0), 2), 
            round(metrics.get('Locker Install Cycle Time ',0), 2), 
            round(metrics.get('Apt Locker Pro Install Cycle Time ',0), 2), 
            round(metrics.get('Apt Locker Install Cycle Time ',0), 2), 
            round(metrics.get('Decomm Cycle Time',0), 2), 
            round(metrics.get('Locker Decomm Cycle Time',0), 2), 
            round(metrics.get('Apt Locker Pro Decomm Cycle Time',0), 2), 
            round(metrics.get('Apt Locker Decomm Cycle Time',0), 2)
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
        
        metrics = calculate_metrics(month_data, region)
        month_metrics[month_start.strftime("%b")] = [
            round(metrics.get('Survey Request',0), 0), 
            round(metrics.get('Locker Survey Request', 0),0),
            round(metrics.get('Apt Locker Pro Survey Request', 0), 0), 
            round(metrics.get('Apt Locker Survey Request', 0), 0), 
            round(metrics.get('Survey Complete ', 0), 0), 
            round(metrics.get('Locker Survey Complete ', 0), 0), 
            round(metrics.get('Apt Locker Pro Survey Complete ', 0), 0), 
            round(metrics.get('Apt Locker Survey Complete ', 0), 0), 
            round(metrics.get('Install ', 0), 0), 
            round(metrics.get('Locker Install ', 0), 0), 
            round(metrics.get('Apt Locker Pro Install ', 0), 0), 
            round(metrics.get('Apt Locker Install ', 0), 0), 
            round(metrics.get('Install Failure %', 0), 0), 
            round(metrics.get('Locker Install Failure %', 0), 0), 
            round(metrics.get('Apt Locker Pro Install Failure %', 0), 0), 
            round(metrics.get('Apt Locker Install Failure %', 0), 0), 
            round(metrics.get('Decomm Request', 0), 0), 
            round(metrics.get('Locker Decomm Request', 0), 0), 
            round(metrics.get('Apt Locker Pro Decomm Request', 0), 0), 
            round(metrics.get('Apt Locker Decomm Request', 0), 0), 
            round(metrics.get('Decomm ', 0), 0), 
            round(metrics.get('Locker Decomm ', 0), 0), 
            round(metrics.get('Apt Locker ProDecomm ', 0), 0), 
            round(metrics.get('Apt Locker Decomm ', 0), 0), 
            round(metrics.get('Install Cycle Time ',0), 2), 
            round(metrics.get('Locker Install Cycle Time ',0), 2), 
            round(metrics.get('Apt Locker Pro Install Cycle Time ',0), 2), 
            round(metrics.get('Apt Locker Install Cycle Time ',0), 2), 
            round(metrics.get('Decomm Cycle Time',0), 2), 
            round(metrics.get('Locker Decomm Cycle Time',0), 2), 
            round(metrics.get('Apt Locker Pro Decomm Cycle Time',0), 2), 
            round(metrics.get('Apt Locker Decomm Cycle Time',0), 2)
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
    metrics = calculate_metrics(current_month_data, region)
    # mtd_values = [
    #     round(metrics.get('Survey Request', 0), 0), round(metrics.get('Survey Complete', 0), 0), round(metrics.get('Survey Cycle Time', 0), 2),
    #     round(metrics.get('Install', 0), 0), round(metrics.get('Install Cycle Time', 0), 2), round(metrics.get('Install Failure %', 0), 2),
    #     round(metrics.get('Decomm Request', 0), 0), round(metrics.get('Decomm', 0), 0), round(metrics.get('Decomm Cycle Time', 0), 2)
    # ]

    current_quarter_start = current_month_start.replace(month=(current_month_start.month - (current_month_start.month - 1) % 3))
    current_quarter_data = cases_df[(cases_df['create_date'] >= current_quarter_start) & (cases_df['create_date'] <= today)]
    metrics = calculate_metrics(current_quarter_data, region)
    qtd_values = [
            round(metrics.get('Survey Request',0), 0), 
            round(metrics.get('Locker Survey Request', 0),0),
            round(metrics.get('Apt Locker Pro Survey Request', 0), 0), 
            round(metrics.get('Apt Locker Survey Request', 0), 0), 
            round(metrics.get('Survey Complete ', 0), 0), 
            round(metrics.get('Locker Survey Complete ', 0), 0), 
            round(metrics.get('Apt Locker Pro Survey Complete ', 0), 0), 
            round(metrics.get('Apt Locker Survey Complete ', 0), 0), 
            round(metrics.get('Install ', 0), 0), 
            round(metrics.get('Locker Install ', 0), 0), 
            round(metrics.get('Apt Locker Pro Install ', 0), 0), 
            round(metrics.get('Apt Locker Install ', 0), 0), 
            round(metrics.get('Install Failure %', 0), 0), 
            round(metrics.get('Locker Install Failure %', 0), 0), 
            round(metrics.get('Apt Locker Pro Install Failure %', 0), 0), 
            round(metrics.get('Apt Locker Install Failure %', 0), 0), 
            round(metrics.get('Decomm Request', 0), 0), 
            round(metrics.get('Locker Decomm Request', 0), 0), 
            round(metrics.get('Apt Locker Pro Decomm Request', 0), 0), 
            round(metrics.get('Apt Locker Decomm Request', 0), 0), 
            round(metrics.get('Decomm ', 0), 0), 
            round(metrics.get('Locker Decomm ', 0), 0), 
            round(metrics.get('Apt Locker ProDecomm ', 0), 0), 
            round(metrics.get('Apt Locker Decomm ', 0), 0), 
            round(metrics.get('Install Cycle Time ',0), 2), 
            round(metrics.get('Locker Install Cycle Time ',0), 2), 
            round(metrics.get('Apt Locker Pro Install Cycle Time ',0), 2), 
            round(metrics.get('Apt Locker Install Cycle Time ',0), 2), 
            round(metrics.get('Decomm Cycle Time',0), 2), 
            round(metrics.get('Locker Decomm Cycle Time',0), 2), 
            round(metrics.get('Apt Locker Pro Decomm Cycle Time',0), 2), 
            round(metrics.get('Apt Locker Decomm Cycle Time',0), 2)
    ]

    current_year_start = current_month_start.replace(month=1, day=1)
    current_year_data = cases_df[(cases_df['create_date'] >= current_year_start) & (cases_df['create_date'] <= today)]
    metrics = calculate_metrics(current_year_data, region)
    ytd_values = [
            round(metrics.get('Survey Request',0), 0), 
            round(metrics.get('Locker Survey Request', 0),0),
            round(metrics.get('Apt Locker Pro Survey Request', 0), 0), 
            round(metrics.get('Apt Locker Survey Request', 0), 0), 
            round(metrics.get('Survey Complete ', 0), 0), 
            round(metrics.get('Locker Survey Complete ', 0), 0), 
            round(metrics.get('Apt Locker Pro Survey Complete ', 0), 0), 
            round(metrics.get('Apt Locker Survey Complete ', 0), 0), 
            round(metrics.get('Install ', 0), 0), 
            round(metrics.get('Locker Install ', 0), 0), 
            round(metrics.get('Apt Locker Pro Install ', 0), 0), 
            round(metrics.get('Apt Locker Install ', 0), 0), 
            round(metrics.get('Install Failure %', 0), 0), 
            round(metrics.get('Locker Install Failure %', 0), 0), 
            round(metrics.get('Apt Locker Pro Install Failure %', 0), 0), 
            round(metrics.get('Apt Locker Install Failure %', 0), 0), 
            round(metrics.get('Decomm Request', 0), 0), 
            round(metrics.get('Locker Decomm Request', 0), 0), 
            round(metrics.get('Apt Locker Pro Decomm Request', 0), 0), 
            round(metrics.get('Apt Locker Decomm Request', 0), 0), 
            round(metrics.get('Decomm ', 0), 0), 
            round(metrics.get('Locker Decomm ', 0), 0), 
            round(metrics.get('Apt Locker ProDecomm ', 0), 0), 
            round(metrics.get('Apt Locker Decomm ', 0), 0), 
            round(metrics.get('Install Cycle Time ',0), 2), 
            round(metrics.get('Locker Install Cycle Time ',0), 2), 
            round(metrics.get('Apt Locker Pro Install Cycle Time ',0), 2), 
            round(metrics.get('Apt Locker Install Cycle Time ',0), 2), 
            round(metrics.get('Decomm Cycle Time',0), 2), 
            round(metrics.get('Locker Decomm Cycle Time',0), 2), 
            round(metrics.get('Apt Locker Pro Decomm Cycle Time',0), 2), 
            round(metrics.get('Apt Locker Decomm Cycle Time',0), 2)
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


    def create_wbr_table(region, week_dates, month_dates, metrics):
        wbr_metrics = list(metrics.keys())
        wow_values = [metrics[metric] for metric in wbr_metrics]
        mom_values = [metrics[metric] for metric in wbr_metrics]
        qtd_values = [metrics[metric] for metric in wbr_metrics]
        ytd_values = [metrics[metric] for metric in wbr_metrics]

        columns = ['Metric'] + [f'WK{week_dates[i].isocalendar().week}' for i in range(6)] + ['WoW'] + [month.strftime("%b") for month in month_dates] + [
            'MoM', 'QTD', 'YTD']
        data = {
            'Metric': wbr_metrics,
            'WoW': wow_values,
            'MoM': mom_values,
            'QTD': qtd_values,
            'YTD': ytd_values
        }
    for i, week in enumerate([f'WK{week_dates[i].isocalendar().week}' for i in range(6)]):
        data[week] = [week_metrics[week][wbr_metrics.index(metric)] for metric in wbr_metrics]

    for i, month in enumerate([month.strftime("%b") for month in month_dates]):
        data[month] = [month_metrics[month][wbr_metrics.index(metric)] for metric in wbr_metrics]

        weekly_df = pd.DataFrame(data, columns=columns)

        def display_dataframe_to_user(name: str, dataframe: pd.DataFrame) -> None:
            wbr_file = f'Files/{name} WBR WK {report_week}.csv'
            dataframe.to_csv(wbr_file, index=False)
            print(f"Displaying DataFrame: {name}")
            print(dataframe.to_string(index=False))

        print(' ')
        print(f'--------{region} WBR TABLE-----------')
        print('-------------------------------')
        display_dataframe_to_user(f"{region}", weekly_df)

    # Replace with actual DataFrame
    region_df = cases_df  # Assuming cases_df is the input DataFrame for a specific region
    today = datetime.today() - timedelta(days=7)
    week_dates = [today - timedelta(weeks=i) for i in range(6)]
    week_dates = week_dates[::-1]

    months_back = 6
    first_day_of_current_month = today.replace(day=1)
    month_dates = [first_day_of_current_month]
    for _ in range(1, months_back):
        first_day_of_current_month = (first_day_of_current_month - timedelta(days=1)).replace(day=1)
        month_dates.append(first_day_of_current_month)
    month_dates = month_dates[::-1]


    for r in regions:
        metrics = calculate_metrics(region_df, r)
        create_wbr_table(r, week_dates, month_dates, metrics)