import pandas as pd
from datetime import datetime, timedelta

# Constants
CSV_PATH = 'cases.csv'

current_date = datetime.today()
current_month = current_date.month
current_year = current_date.year
previous_year = current_year - 1

REGION_GROUPS = {
    "NA": ["US", "CA", "MX"],
    "SA": ["AR", "BO", "BR", "CL", "CO", "EC", "FK", "GF", "GY", "PE", "PY", "SR", "UY", "VE"],
    "ASEAN": ["AF", "AM", "AZ", "BH", "BD", "BT", "BN", "KH", "CN", "CY", "GE", "IN", "ID", "IR", "IQ", "IL", "JP", "JO", "KZ", "KP", "KR", "KW", "KG", "LA", "LB", "MY", "MV", "MN", "MM", "NP", "OM", "PK", "PH", "QA", "SA", "SG", "LK", "SY", "TJ", "TH", "TL", "TM", "AE", "UZ", "VN", "YE"],
    "APAC": ["AU", "JP"],
    "EMEA": ["AL", "AD", "AT", "BY", "BE", "BA", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IS", "IE", "IT", "LV", "LI", "LT", "LU", "MT", "MD", "MC", "ME", "NL", "MK", "NO", "PL", "PT", "RO", "RU", "SM", "RS", "SK", "SI", "ES", "SE", "CH", "UA", "GB", "VA", "DZ", "AO", "BJ", "BW", "BF", "BI", "CM", "CV", "CF", "TD", "KM", "CG", "CD", "DJ", "EG", "GQ", "ER", "SZ", "ET", "GA", "GM", "GH", "GN", "GW", "CI", "KE", "LS", "LR", "LY", "MG", "MW", "ML", "MR", "MU", "MA", "MZ", "NA", "NE", "NG", "RW", "ST", "SN", "SC", "SL", "SO", "ZA", "SS", "SD", "TZ", "TG", "TN", "UG", "ZM", "ZW", "BH", "IQ", "IL", "JO", "KW", "LB", "OM", "PS", "QA", "SA", "SY", "AE", "YE"],
    "EU": ["AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE"]
}

def load_data(csv_path):
    return pd.read_csv(csv_path, parse_dates=['create_date', 'fulfilled_date'])

def get_quarter_dates(year, quarter):
    if quarter == 1:
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 3, 31)
    elif quarter == 2:
        start_date = datetime(year, 4, 1)
        end_date = datetime(year, 6, 30)
    elif quarter == 3:
        start_date = datetime(year, 7, 1)
        end_date = datetime(year, 9, 30)
    else:
        start_date = datetime(year, 10, 1)
        end_date = datetime(year, 12, 31)
    return start_date.strftime("%m/%d/%Y"), end_date.strftime("%m/%d/%Y")

# Determine the current quarter
current_quarter = (current_month - 1) // 3 + 1

# Determine the previous quarter and the quarter to compare
if current_quarter == 1:
    previous_quarter = 4
    previous_quarter_year = current_year - 1
    quarter_compare = 3
    quarter_compare_year = current_year - 1
elif current_quarter == 4:
    previous_quarter = 3
    previous_quarter_year = current_year
    quarter_compare = 2
    quarter_compare_year = current_year
else:
    previous_quarter = current_quarter - 1
    previous_quarter_year = current_year
    quarter_compare = previous_quarter - 1
    quarter_compare_year = current_year

    if quarter_compare == 0:
        quarter_compare = 4
        quarter_compare_year = current_year - 1

# Get the dates for the current, previous, and compare quarters
current_quarter_dates = get_quarter_dates(current_year, current_quarter)
previous_quarter_dates = get_quarter_dates(previous_quarter_year, previous_quarter)
compare_quarter_dates = get_quarter_dates(quarter_compare_year, quarter_compare)

# Print the results
print("Current Quarter Dates:", current_quarter_dates)
print("Previous Quarter Dates:", previous_quarter_dates)
print("Compare Quarter Dates:", compare_quarter_dates)

def filter_data_by_date(df, start_date, end_date, date_column='create_date'):
    return df[(df[date_column] >= start_date) & (df[date_column] <= end_date)]



def calculate_metrics(df, region="WW"):
        metrics = {}

        if region != "WW":
                region_df = df[df['region'] == region]

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

def calculate_request_metrics(df, region="WW"):
    if region != "WW":
        df = df[df['region'] == region]
    metrics = {
        'Survey Request': df[df['type'] == 'Survey'].shape[0],
        'Decomm Request': df[df['type'] == 'Decomm'].shape[0]
    }
    return {k: round(v, 2) if isinstance(v, (int, float)) else v for k, v in metrics.items()}

def percent_change(metric, current, previous):
    if previous == 0 or previous is None:
        return ""
    change = ((current - previous) / previous) * 100
    return f"{round(change, 2)}%" if isinstance(change, (int, float)) else ""

def add_percent_sign(qoq_values):
    return [f"{v:.2f}%" if isinstance(v, (int, float)) else v for v in qoq_values]

def main():
    cases_df = load_data(CSV_PATH)
    
    current_month = int(datetime.today().month)
    current_year = int(datetime.today().year)

    previous_dates, quarter_before_previous_dates = previous_quarter_dates(current_month, current_year)
    previous_start_date, previous_end_date = previous_dates
    quarter_before_previous_start_date, quarter_before_previous_end_date = quarter_before_previous_dates

    # Filter data for previous and quarter before previous date ranges
    request_period = filter_data_by_date(cases_df, previous_start_date, previous_end_date)
    request_compare_period = filter_data_by_date(cases_df, quarter_before_previous_start_date, quarter_before_previous_end_date)
    complete_period = filter_data_by_date(cases_df, previous_start_date, previous_end_date, 'fulfilled_date')
    complete_compare_period = filter_data_by_date(cases_df, quarter_before_previous_start_date, quarter_before_previous_end_date, 'fulfilled_date')

    regions = ["WW", "EU", "North America", "APAC"]

    metrics = [
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

    all_values = {}

    for region in regions:
        region_key = region if region != "WW" else "World Wide"
        
        request_metrics = calculate_request_metrics(request_period, region)
        current_metrics = calculate_metrics(complete_period, region)
        compare_metrics = calculate_metrics(complete_compare_period, region)
        request_compare_metrics = calculate_request_metrics(request_compare_period, region)

        qoq_values = []
        for metric in metrics:
            qoq_values = add_percent_sign([             
                percent_change('count', request_metrics['Survey Request'], request_compare_metrics['Survey Request']),
                percent_change('count', request_metrics['Locker Survey Request'], request_compare_metrics['Locker Survey Request']),
                percent_change('count', request_metrics['Apt Locker Pro Survey Request'], request_compare_metrics['Apt Locker Pro Survey Request']),
                percent_change('count', request_metrics['Apt Locker Survey Request'], request_compare_metrics['Apt Locker Survey Request']),
                percent_change('count', request_metrics['Survey Complete'], request_compare_metrics['Survey Complete']),
                percent_change('count', request_metrics['Locker Survey Complete'], request_compare_metrics['Locker Survey Complete']),
                percent_change('count', request_metrics['Apt Locker Pro Survey Complete'], request_compare_metrics['Apt Locker Pro Survey Complete']),
                percent_change('count', request_metrics['Apt Locker Survey Complete'], request_compare_metrics['Apt Locker Survey Complete']),
                percent_change('count', request_metrics['Install'], request_compare_metrics['Install']),
                percent_change('count', request_metrics['Locker Install'], request_compare_metrics['Locker Install']),
                percent_change('count', request_metrics['Apt Locker Pro Install'], request_compare_metrics['Apt Locker Pro Install']),
                percent_change('count', request_metrics['Apt Locker Install'], request_compare_metrics['Apt Locker Install']),
                percent_change('cycletime', request_metrics['Install Cycle Time'], request_compare_metrics['Install Cycle Time']),
                percent_change('cycletime', request_metrics['Locker Install Cycle Time'], request_compare_metrics['Locker Install Cycle Time']),
                percent_change('cycletime', request_metrics['Apt Locker Pro Install Cycle Time'], request_compare_metrics['Apt Locker Pro Install Cycle Time']),
                percent_change('cycletime', request_metrics['Apt Locker Install Cycle Time'], request_compare_metrics['Apt Locker Install Cycle Time']),
                percent_change('count', request_metrics['Install Failure %'], request_compare_metrics['Install Failure %']),
                percent_change('count', request_metrics['Locker Install Failure %'], request_compare_metrics['Locker Install Failure %']),
                percent_change('count', request_metrics['Apt Locker Pro Install Failure %'], request_compare_metrics['Apt Locker Pro Install Failure %']),
                percent_change('count', request_metrics['Apt Locker Install Failure %'], request_compare_metrics['Apt Locker Install Failure %']),
                percent_change('count', request_metrics['Decomm Request'], request_compare_metrics['Decomm Request']),
                percent_change('count', request_metrics['Locker Decomm Request'], request_compare_metrics['Locker Decomm Request']),
                percent_change('count', request_metrics['Apt Locker Pro Decomm Request'], request_compare_metrics['Apt Locker Pro Decomm Request']),
                percent_change('count', request_metrics['Apt Locker Decomm Request'], request_compare_metrics['Apt Locker Decomm Request']),
                percent_change('count', request_metrics['Decomm'], request_compare_metrics['Decomm']),
                percent_change('count', request_metrics['Locker Decomm'], request_compare_metrics['Locker Decomm']),
                percent_change('count', request_metrics['Apt Locker Pro Decomm'], request_compare_metrics['Apt Locker Pro Decomm']),
                percent_change('count', request_metrics['Apt Locker Decomm'], request_compare_metrics['Apt Locker Decomm']),
                percent_change('cycletime', request_metrics['Decomm Cycle Time'], request_compare_metrics['Decomm Cycle Time']),
                percent_change('cycletime', request_metrics['Locker Decomm Cycle Time'], request_compare_metrics['Locker Decomm Cycle Time']),
                percent_change('cycletime', request_metrics['Apt Locker Pro Decomm Cycle Time'], request_compare_metrics['Apt Locker Pro Decomm Cycle Time']),
                percent_change('cycletime', request_metrics['Apt Locker Decomm Cycle Time'], request_compare_metrics['Apt Locker Decomm Cycle Time'])
            ])
        
            all_values[region_key] = {
                    'metrics': metrics,
                    'previous_quarter_values': [
                            request_metrics.get('Survey Request'),
                            request_metrics.get('Locker Survey Request'),
                            request_metrics.get('Apt Locker Pro Survey Request'),
                            request_metrics.get('Apt Locker Survey Request'),
                            current_metrics.get('Survey Complete'),
                            current_metrics.get('Locker Survey Complete'),
                            current_metrics.get('Apt Locker Pro Survey Complete'),
                            current_metrics.get('Apt Locker Survey Complete'),
                            current_metrics.get('Install'),
                            current_metrics.get('Locker Install'),
                            current_metrics.get('Apt Locker Pro Install'),
                            current_metrics.get('Apt Locker Install'),
                            current_metrics.get('Install Cycle Time'),
                            current_metrics.get('Locker Install Cycle Time'),
                            current_metrics.get('Apt Locker Pro Install Cycle Time'),
                            current_metrics.get('Apt Locker Install Cycle Time'),
                            current_metrics.get('Install Failure %'),
                            current_metrics.get('Locker Install Failure %'),
                            current_metrics.get('Apt Locker Pro Install Failure %'),
                            current_metrics.get('Apt Locker Install Failure %'),
                            request_metrics.get('Decomm Request'),
                            request_metrics.get('Locker Decomm Request'),
                            request_metrics.get('Apt Locker Pro Decomm Request'),
                            request_metrics.get('Apt Locker Decomm Request'),
                            current_metrics.get('Decomm'),
                            current_metrics.get('Locker Decomm'),
                            current_metrics.get('Apt Locker Pro Decomm'),
                            current_metrics.get('Apt Locker Decomm'),
                            current_metrics.get('Decomm Cycle Time'),
                            current_metrics.get('Locker Decomm Cycle Time'),
                            current_metrics.get('Apt Locker Pro Decomm Cycle Time'),
                            current_metrics.get('Apt Locker Decomm Cycle Time')
                    ],
                    'qoq_values': qoq_values
            }

    output_df = pd.DataFrame({
        'Metric': metrics,
        'WW - Previous Quarter': all_values['World Wide']['previous_quarter_values'],
        'WW QoQ': all_values['World Wide']['qoq_values'],
        'EU - Previous Quarter': all_values['EU']['previous_quarter_values'],
        'EU QoQ': all_values['EU']['qoq_values'],
        'NA - Previous Quarter': all_values['North America']['previous_quarter_values'],
        'NA QoQ': all_values['North America']['qoq_values'],
        'APAC - Previous Quarter': all_values['APAC']['previous_quarter_values'],
        'APAC QoQ': all_values['APAC']['qoq_values']
    })

    print(f'                            ')
    print(f'                            ')
    print(f'--------QBR TABLE-----------')
    print(f'----------------------------')
    print(f'                            ')

    def display_dataframe_to_user(name: str, dataframe: pd.DataFrame) -> None:
        print(f"Displaying DataFrame: {name}")
        print(dataframe.to_string(index=False))

    display_dataframe_to_user("Output Metrics", output_df)

if __name__ == "__main__":
    main()