import json
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Get the bucket name and the key of the uploaded file
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Download the file from S3
    response = s3_client.get_object(Bucket=bucket, Key=key)
    file_content = response['Body'].read().decode('utf-8')
    
    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(StringIO(file_content))
    
    # Your existing code to process the DataFrame
    regions = ['WW', 'North America', 'Europe', 'Asia Pacific', 'Latin America']
    wbr_metrics = [
        'Survey Request', 'Apt Locker Survey Request', 'Apt Locker Pro Survey Request',
        'Survey Complete', 'Apt Locker Survey Complete', 'Apt Locker Pro Survey Complete',
        'Survey Cycle Time', 'Apt Locker Survey Cycle Time', 'Apt Locker Pro Survey Cycle Time',
        'Install Request', 'Apt Locker Install Request', 'Apt Locker Pro Install Request',
        'Install Complete', 'Apt Locker Install Complete', 'Apt Locker Pro Install Complete',
        'Install Cycle Time', 'Apt Locker Install Cycle Time', 'Apt Locker Pro Install Cycle Time',
        'Install Failure %', 'Apt Locker Install Failure %', 'Apt Locker Pro Install Failure %',
        'Decomm Request', 'Apt Locker Decomm Request', 'Apt Locker Pro Decomm Request',
        'Decomm', 'Apt Locker Decomm', 'Apt Locker Pro Decomm',
        'Decomm Cycle Time', 'Apt Locker Decomm Cycle Time', 'Apt Locker Pro Decomm Cycle Time'
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
        
        # Add more metric calculations as needed
        
        return metrics
    
    def create_wbr_table(region, week_dates, month_dates, metrics):
        wbr_metrics = [
            'Survey Request', 'Apt Locker Survey Request', 'Apt Locker Pro Survey Request',
            'Survey Complete', 'Apt Locker Survey Complete', 'Apt Locker Pro Survey Complete',
            'Survey Cycle Time', 'Apt Locker Survey Cycle Time', 'Apt Locker Pro Survey Cycle Time',
            'Install Request', 'Apt Locker Install Request', 'Apt Locker Pro Install Request',
            'Install Complete', 'Apt Locker Install Complete', 'Apt Locker Pro Install Complete',
            'Install Cycle Time', 'Apt Locker Install Cycle Time', 'Apt Locker Pro Install Cycle Time',
            'Install Failure %', 'Apt Locker Install Failure %', 'Apt Locker Pro Install Failure %',
            'Decomm Request', 'Apt Locker Decomm Request', 'Apt Locker Pro Decomm Request',
            'Decomm', 'Apt Locker Decomm', 'Apt Locker Pro Decomm',
            'Decomm Cycle Time', 'Apt Locker Decomm Cycle Time', 'Apt Locker Pro Decomm Cycle Time'
        ]
        
        columns = ['Metric'] + [f'WK{i}' for i in range(6)] + ['WoW'] + [month.strftime("%b") for month in month_dates] + [
            'MoM', 'QTD', 'YTD']
        data = {
            'Metric': wbr_metrics,
            'WoW': [metrics[metric] for metric in wbr_metrics],
            'MoM': [metrics[metric] for metric in wbr_metrics],
            'QTD': [metrics[metric] for metric in wbr_metrics],
            'YTD': [metrics[metric] for metric in wbr_metrics]
        }
        for i, week in enumerate([f'WK{week_dates[i].isocalendar().week}' for i in range(6)]):
            data[week] = [metrics[metric] for metric in wbr_metrics]
        
        for i, month in enumerate([month.strftime("%b") for month in month_dates]):
            data[month] = [metrics[metric] for metric in wbr_metrics]
        
        weekly_df = pd.DataFrame(data, columns=columns)
        
        def display_dataframe_to_user(name: str, dataframe: pd.DataFrame) -> None:
            wbr_file = f'Files/{name} WBR WK {week_dates[-1].isocalendar().week}.csv'
            dataframe.to_csv(wbr_file, index=False)
            print(f"Displaying DataFrame: {name}")
            print(dataframe.to_string(index=False))
        
        print(' ')
        print(f'--------{region} WBR TABLE-----------')
        print('-------------------------------')
        display_dataframe_to_user(f"{region}", weekly_df)
    
    # Example usage
    today = datetime.today()
    week_dates = [today - timedelta(weeks=i) for i in range(6)]
    week_dates = week_dates[::-1]
    
    months_back = 6
    first_day_of_current_month = today.replace(day=1)
    month_dates = [first_day_of_current_month]
    for _ in range(1, months_back):
        first_day_of_current_month = (first_day_of_current_month - timedelta(days=1)).replace(day=1)
        month_dates.append(first_day_of_current_month)
    month_dates = month_dates[::-1]
    
    for region in regions:
        metrics = calculate_metrics(df, region)
        create_wbr_table(region, week_dates, month_dates, metrics)

    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete!')
    }