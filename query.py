"""
(This is a file-level docstring.)
This file contains all required queries to MongoDb.
"""
from pymongo import MongoClient

db = MongoClient().test


def query1(minFare, maxFare):
    """ Finds taxi rides with fare amount greater than or equal to minFare and less than or equal to maxFare.  

    Args:
        minFare: An int represeting the minimum fare
        maxFare: An int represeting the maximum fare

    Projection:
        pickup_longitude
        pickup_latitude
        fare_amount

    Returns:
        An array of documents.
    """
    docs = db.taxi.find(
        {
            'fare_amount': { '$gte' : minFare, '$lte' : maxFare}
        }
        ,
        {
            '_id': 0,
            'pickup_longitude': 1,
            'pickup_latitude': 1,
            'fare_amount': 1
        }
    )

    result = [doc for doc in docs]
    print(result)
    return result


def query2(textSearch, minReviews):
    """ Finds airbnbs with that match textSearch and have number of reviews greater than or equal to minReviews.  

    Args:
        textSearch: A str representing an arbitrary text search
        minReviews: An int represeting the minimum amount of reviews

    Projection:
        name
        number_of_reviews
        neighbourhood
        price
        location

    Returns:
        An array of documents.
    """
    docs = db.airbnb.find(
        {
            '$text': {
                '$search': textSearch
            },
            'number_of_reviews': {
                '$gte': minReviews
            }
        },
        {
            '_id': 0,
            'name': 1,
            'number_of_reviews': 1,
            'neighbourhood': 1,
            'price': 1,
            'location': 1
        }
    )

    result = [doc for doc in docs]
    return result


def query3():
    """ Groups airbnbs by neighbourhood_group and finds average price of each neighborhood_group sorted in descending order.  

    Returns:
        An array of documents.
    """
    docs = db.airbnb.aggregate([
        {'$group': {'_id': '$neighbourhood_group', 'total' : {'$avg': "$price"}}},
        {'$sort': {'total': -1}}
    ])

    result = [doc for doc in docs]
    print(result)
    return result
"""
{
        "_id" : ObjectId("5dd85a8a81382be25462c734"),
        "key" : "2009-06-15 17:26:21.0000001",
        "fare_amount" : 4.5,
        "pickup_datetime" : ISODate("2009-06-15T17:26:21Z"),
        "pickup_longitude" : -73.844311,
        "pickup_latitude" : 40.721319,
        "dropoff_longitude" : -73.84161,
        "dropoff_latitude" : 40.712278000000005,
        "passenger_count" : 1
}
"""
def query4():
    """ Groups taxis by pickup hour. 
        Find average fare for each hour.
        Find average manhattan distance travelled for each hour.
        Count total number of rides per pickup hour.
        Sort by average fare in descending order.

    Returns:
        An array of documents.
    """
    docs = db.taxi.aggregate([
        {'$group': {
            '_id': {'$hour': '$pickup_datetime'}, 
            'avgFare': {'$avg': '$fare_amount'},
            'avgDist': {'$add': [
                        {'$abs': 
                            {'$subtract': ['$pickup_longitude', '$dropoff_longitude']}
                        },
                        {'$abs': 
                            {'$subtract': ['$pickup_latitude', '$dropoff_latitude']}
                        }
                        ]
                }
            }
        }
    ])
    result = [doc for doc in docs]
    print(result)
    return result


def query5():
    """ Finds airbnbs within 1000 meters from location (longitude, latitude) using geoNear. 
        Find average fare for each hour.
        Find average manhattan distance travelled for each hour.
        Count total number of rides per pickup hour.
        Sort by average fare in descending order.

    Projection:
        dist
        location
        name
        neighbourhood
        neighbourhood_group
        price
        room_type


    """
    docs = db.airbnb.aggregate(
        # TODO: implement me
    )
    result = [doc for doc in docs]
    return result

if __name__ == "__main__":
    query4()