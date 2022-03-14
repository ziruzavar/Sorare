from python_graphql_client import GraphqlClient
from settings import PROXIES, postgres_prod_str, aud, jwt_token
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(postgres_prod_str)  # Create engine

client = GraphqlClient(  # GraphQlClient, in the headers I am passing my jwt-token, read more here: https://github.com/sorare/api
    endpoint="https://api.sorare.com/graphql/",
    headers={
        "Content-Type": "application/json",
        "Authorization": f"ziruzavar {jwt_token}",
        "JWT_AUD": f"{aud}",
    },
    proxies={"https": PROXIES},
)

# Retrieving the slug teams from the DB
teams = pd.read_sql("select slug from transfermarkt_test.sorare_players sp where player_id in (select player_id from sorare.sorare_mvp_players smp)", engine)

#Price for limited and rare cards
query = """
{
  player(slug: "player_slug"){
    cards(after:null,rarities: [rare, limited]){
      nodes{
        rarity
        player{
          id
        }
        userOwnersWithRate{
          priceInFiat{
						usd
          }
          from
        }       
      }
      pageInfo{
          endCursor
        	hasNextPage
      }
    }
  }
}
"""

players = {"player_id": [], "price_usd": [], "date": [], "rarity": []}
# Iteratting over players from each team
def iterate_over_batch(data):
    for node in data["data"]["player"]["cards"]["nodes"]:
        rarity = node["rarity"]
        player_id = node["player"]["id"].split(":")[-1]

        for card_offer in node["userOwnersWithRate"]:
            if node["userOwnersWithRate"]:
                price_usd = float(round(card_offer["priceInFiat"]["usd"], 2))
                date = card_offer["from"]

                players["player_id"].append(player_id)
                players["price_usd"].append(price_usd)
                players["date"].append(date)
                players["rarity"].append(rarity)


# Iteratting over the teams
for index, row in teams.iterrows():
    slug = row['slug']
    data = client.execute(query=query.replace("player_slug", slug))
    iterate_over_batch(data)
    while data["data"]["player"]["cards"]["pageInfo"]["hasNextPage"]:
        cursor = data["data"]["player"]["cards"]["pageInfo"]["endCursor"]
        slug = row['slug']
        data = client.execute(
            query=query.replace("null", f'"{cursor}"').replace("player_slug", slug)
        )
        iterate_over_batch(data)

    df = pd.DataFrame(players)  # Saving all the players in a SQL database
    df.to_sql(
        "sorare_prices",
        con=engine,
        schema="sorare",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=100,
    )
    players = {"player_id": [], "price_usd": [], "date": [], "rarity": []}
