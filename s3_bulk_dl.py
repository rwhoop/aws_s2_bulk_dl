import csv
import boto3

from time import sleep

def try_retry(f, args={}, max_retries=3, wait_mult=0):
    #print(args)
    retries = 0
    err = None
    while retries < max_retries:
        try:
            f(**args)
            retries = max_retries + 1
        except Exception as e:
            retries += 1
            print("Attempt {retries} failed with err: {err}".format(retries=retries, err=e))
            sleep(retries * wait_mult) # sleep in case of throttling
            err = e

    return {
        'success': 'Y' if retries > max_retries else 'N',
        'msg': err
    }

session = boto3.Session(profile_name="gea-poc", region_name='us-east-1')
s3 = session.client('s3')

bucket = 'connect-call-recordings-us-east-1'
csv_file = "Parts_Frontline2.csv"
import_log_file = "import_log.csv"
import_log_headers = ['contactid', 'recording_location', 'output_filename', 'success', 'msg']
import_log = []

# import csv
print('Loading CSV file')
with open(csv_file, 'r') as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        import_log.append(
            {
                'contactid': row['contactid'],
                'recording_location': row['recording_location'].split('/', 1)[1] if row['recording_location'] else '', #he put the bucket in the key... strip this out
                'output_filename': row['recording_location'].split('/')[-1].replace(':','-') if row['recording_location'] else '', #object key has a colon (:) which is invalid for Windows
                'success': '' if row['recording_location'] else 'N',
                'msg': '' if row['recording_location'] else 'No recording_location provided'
            }
        )
        #print(row['recording_location'])


print('Downloading files')
max_rows = 5
count = 0
for row in import_log:
    count += 1
    if count > max_rows:
        break

    # args = {'Bucket':bucket, 'Key':row['recording_location'], 'Filename':row['output_filename']}
    # try_retry(s3.download_file, args=args)

    # args = {'Bucket':bucket, 'Key':row['recording_location']}
    #row = {**row, **try_retry(s3.head_object, args=args)}
    if row['recording_location']:
        print(count, ' / ', len(import_log))
        args = {'Bucket':bucket, 'Key':row['recording_location'], 'Filename':row['output_filename']}
        response = try_retry(s3.download_file, args=args)
        row['success'] = response['success']
        row['msg'] = response['msg']


print('Writing log')
with open(import_log_file, 'w', newline='') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=import_log_headers)
    
    writer.writeheader()
    for row in import_log:
        writer.writerow(row)

print('Done')