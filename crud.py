import boto3
from decimal import Decimal
import json
from boto3.dynamodb.conditions import Key,Attr
import pprint



# ---------------------------------------------------------------------------------------------------------------------
# a function to create the table

def create_movie_table(dynamodb=None):
    if not dynamodb:

        # dynamodb = boto3.resource('dynamodb')

        ## Explicitly specify a region
        # dynamodb = boto3.resource('dynamodb',region_name='us-west-2')

        ## Use a DynamoDB Local endpoint
        dynamodb = boto3.resource('dynamodb',endpoint_url="http://localhost:8000")

    table = dynamodb.create_table(
        TableName='Movies',
        KeySchema=[
            {
                'AttributeName': 'year',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'title',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'year',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

# ---------------------------------------------------------------------------------------------------------------------
# Loading movie data 

def load_movies(movies, dynamodb=None):
    if not dynamodb:

        # Use a DynamoDB Local endpoint
        dynamodb = boto3.resource('dynamodb',endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Movies')
    for movie in movies:
        year = int(movie['year'])
        title = movie['title']
        print("Adding movie:", year, title)
        table.put_item(Item=movie)

# ---------------------------------------------------------------------------------------------------------------------
# Querying data on the basis of the partition key

def query_movies(year, dynamodb=None):
    if not dynamodb:

        dynamodb = boto3.resource('dynamodb',endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Movies')
    response = table.query(
        KeyConditionExpression=Key('year').eq(year) 
    )
    return response['Items']

# ---------------------------------------------------------------------------------------------------------------------
# scanning for data, when the requested item is not a partition or sort key 

def scan_movies(year_range, display_movies, dynamodb=None):
    if not dynamodb:

        dynamodb = boto3.resource('dynamodb',endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Movies')
    scan_kwargs = {
        'FilterExpression': Key('year').between(*year_range),
        'ProjectionExpression': "#yr, title, info.rating",
        'ExpressionAttributeNames': {"#yr": "year"}
    }

    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        display_movies(response.get('Items', []))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

# ---------------------------------------------------------------------------------------------------------------------
#  update movie title using year
def update_movie_title(updated_year, dynamodb = None):
    if not dynamodb:
        dynamodb = boto3.resource('dynammodb',endpoint_url="http://localhost:8000")

    table =  dynamodb.Table('Movies')
    response = table.update_item(
        Key = {'year':updated_year},
        UpdateExpression="SET title= :s",
        ExpressionAttributeValues={':s','What is love? Baby dont hurt me? No more'},
        ReturnValues="UPDATED_NEW"
    )

    print(response['Attributes'])

# ---------------------------------------------------------------------------------------------------------------------
# delete movie using year
def delete_movie(deleting_year,dynamodb= None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb',endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Movies')
    response = table.delete_item(
        Key={
        'year':deleting_year
        }
    )

    print("Successfully Deleted")

# ---------------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':

# creating the table

    movie_table = create_movie_table()
    print("Table Name:", movie_table.table_name)
    print("Table status:", movie_table.table_status)
    print("Table ARN:", movie_table.table_arn)\
    
# loading data into the table

    with open("moviedata.json") as json_file:
        movie_list = json.load(json_file, parse_float=Decimal)
    load_movies(movie_list)

# querying data using the partition key

    movies = query_movies(1985) # sending the year
    for movie in movies:
        print(movie['year'], ":", movie['title'])

# scaning data, to find a value (which is not included as parition or sort key)

    def print_movies(movies):
        for movie in movies:
            print(f"\n{movie['year']} : {movie['title']}")
            pprint(movie['info'])

    query_range = (1950, 1959)

    scan_movies(query_range, print_movies)

# updating values present in the table 

    update_movie_title(1985)

#  deleting values preset in the table

    delete_movie(1985)

