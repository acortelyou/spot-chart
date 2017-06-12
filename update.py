#!/usr/bin/python3
from datetime import datetime, timedelta
import matplotlib
matplotlib.use('Agg')
from matplotlib import dates as mdates
from matplotlib import ticker as ticker
from matplotlib import pyplot as plt
import pandas as pd
import boto3
import re
import warnings
warnings.simplefilter("ignore", UserWarning)

# filters
instanceTypes=['g2.2xlarge', 'g2.8xlarge']
productDescriptions = ['Linux/UNIX (Amazon VPC)']
regions = ['us-east-1','us-west-1','us-west-2','eu-central-1','ap-northeast-1','ap-southeast-1']

# range
now = datetime.utcnow().replace(microsecond=0)
start = now - timedelta(days=7)
end = now
print('range\t', start, 'to', end)

# load data from file
try:
	fileName = 'data.csv'
	data = pd.read_csv(fileName, encoding='utf-8')
	data['Timestamp'] = pd.to_datetime(data['Timestamp'], utc=True)

	# find existing range
	az_timestamps = data.groupby(['AvailabilityZone'])['Timestamp']
	min_timestamp = az_timestamps.min().max()
	max_timestamp = az_timestamps.max().min()
	print('loaded\t', min_timestamp, 'to', max_timestamp)
except:
	data = None
	min_timestamp = datetime.max
	max_timestamp = datetime.min

# load data from aws
start_request = max_timestamp if min_timestamp < start else start
if end - start_request > timedelta(minutes=59):
	print('pulling\t', start_request, 'to', end)
	l = []
	for region in regions:
		print(region, end='', flush=True)
		client = boto3.client('ec2',region_name=region)
		next = 0
		while next != '':
			if next == 0:
				prices=client.describe_spot_price_history(StartTime=start_request, EndTime=end, InstanceTypes=instanceTypes, ProductDescriptions=productDescriptions)
			else:
				print('.', end='', flush=True)
				prices=client.describe_spot_price_history(StartTime=start_request, EndTime=end, InstanceTypes=instanceTypes, ProductDescriptions=productDescriptions, NextToken=next)
			for price in prices['SpotPriceHistory']:
				l.append({
					'Region': region,
					'AvailabilityZone': price['AvailabilityZone'],
					'InstanceType': price['InstanceType'],
					'SpotPrice': price['SpotPrice'],
					'Timestamp': price['Timestamp']
				})
			next = prices['NextToken']
		print('', flush=True)

	# import merge and persist data
	aws_data = pd.DataFrame(l)
	aws_data['Timestamp'] = pd.to_datetime(aws_data['Timestamp'], utc=True)
	try:
		data = data.append(aws_data).drop_duplicates()
	except:
		data = aws_data
	data.to_csv(fileName, encoding='utf-8', index=False, date_format="%Y-%m-%dT%H:%M:%SZ")
data.set_index(data['Timestamp'], inplace=True)
data['SpotPrice'] = data['SpotPrice'].astype(float)

for type in instanceTypes:
	print(type)

	# select dataframe
	df = data
	df = df[df.InstanceType == type]
	df = df[df.Timestamp > start]
	df = df[df.Timestamp < end]

	# process dataframe
	df['Timeminute'] = df['Timestamp'].apply(lambda dt: dt.strftime("%m-%d-%y %H:%M"))
	df['SpotPrice'] = df.groupby(['Region','Timeminute'])['SpotPrice'].transform(min)

	# set up plot figure
	pd.set_option('display.mpl_style', 'default')
	plt.figure(1, figsize=(25.6,7.2), tight_layout=True)

	# plot dataframe
	colors = ['#1f77b4','#ff7f0e','#d62728','#9467bd','#2ca02c','#bcbd22']
	for region, region_data in df.groupby(['Region'], as_index=False):
		region_data = region_data.resample('60s').interpolate()
		region_data = pd.rolling_mean(region_data,15)
		plt.plot(region_data.index, region_data['SpotPrice'],label=region,color=colors.pop())

	# set up axes
	ax = plt.gca()
	ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
	ax.xaxis.set_minor_locator(ticker.AutoMinorLocator(6))
	ax.xaxis.set_major_formatter(mdates.DateFormatter('%a'))
	ax.xaxis.set_minor_formatter(mdates.DateFormatter('%H:00'))
	plt.autoscale(enable=True, axis='y', tight=True)
	ylim = ax.get_ylim()
	ymax = (ylim[0]+.01) * 5
	ymax = ylim[1] if ylim[1] < ymax else ymax
	ymax = 2 if ymax > 2 else ymax
	plt.ylim(ylim[0],ymax)

	# set up labels
	title = type+' - '+productDescriptions[0]
	plt.title(title)
	plt.ylabel('Lowest Price')
	plt.xlabel('Zulu Time')

	# sort legend
	handles,labels = ax.get_legend_handles_labels()
	sorted=[]
	for region in regions:
		sorted.append(handles[labels.index(region)])
	ax.legend(sorted, regions, loc=2)

	# save output
	filename=re.sub('(/UNIX|Amazon|[ \(\)])','',title)+'.png'
	plt.savefig(filename)
	plt.close()

print('done')
