# Create keyspace
CREATE KEYSPACE prod WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

# Create cassandra tables

# Use user_hashid in order to encrypt actual user ids 
CREATE TABLE prod.seeds(user_hashid bigint, restaurant_hashid bigint, PRIMARY KEY(user_hashid));

# Use rank as clustering column so that it gets sorted during compaction
# Need match_hashid as primary key otherwise it would get overwritten if two ranks happen to have the same value
CREATE TABLE prod.ranks(restaurant_hashid bigint, rank double, match_hashid bigint, PRIMARY KEY((restaurant_hashid), rank, match_hashid));

# Use restaurant_hashid as partition key instead of city so that data is distributed evenly and there aren't hotspots in case users in one city
# are more active than others 
CREATE TABLE prod.idmapper(restaurant_hashid bigint, name_city frozen <tuple<text, text>>, PRIMARY KEY(restaurant_hashid));
