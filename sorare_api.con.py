from dataclasses import replace
from time import sleep
from python_graphql_client import GraphqlClient
from settings import PROXIES, postgres_prod_str, aud, jwt_token
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(postgres_prod_str)  # Create engine

client = GraphqlClient(  # GraphQlClient, in headers I am passing my jwt-token, read more here: https://github.com/sorare/api
    endpoint="https://api.sorare.com/graphql",
    headers={
        "Content-Type": "application/json",
        "Authorization": f"ziruzavar {jwt_token}",
        "JWT_AUD": f"{aud}",
    },
    proxies={"https": PROXIES},
)

players = []

# Query to get 50 cards
query = """  {
      allCards ( first:50  $after , rarities: super_rare) {
        edges{
          cursor
          node {
            player{
              displayName,
              birthDate,
              age,
              id
            }
          }
        }
        pageInfo{
          startCursor
          endCursor
          hasPreviousPage
          hasNextPage
        }
      }
 }
"""

cursor = None
query_f = query.replace("$after", "")
data = client.execute(query=query_f)

sum = 0
while data["data"]["allCards"]["pageInfo"][
    "hasNextPage"
]:  # Loop through all the players
    for player in data["data"]["allCards"]["edges"]:
        name = player["node"]["player"]["displayName"]
        birth_date = player["node"]["player"]["birthDate"]
        age = player["node"]["player"]["age"]
        player_id = player["node"]["player"]["id"]
        players.append(
            {"name": name, "birth_date": birth_date, "age": age, "player_id": player_id}
        )
    sum += 50
    print(sum)
    # Limit for authenticated users is 60 requests/minute so I am sleeping for 1 second
    cursor = data["data"]["allCards"]["pageInfo"]["endCursor"]
    data = client.execute(query=query.replace("$after", f', after:"{cursor}" '))


df = pd.DataFrame(players)  # Saving all the players in a SQL database
df.to_sql(
    "sorare_players",
    con=engine,
    schema="transfermarkt_test",
    if_exists="append",
    index=False,
)
