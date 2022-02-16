from python_graphql_client import GraphqlClient
from settings import PROXIES, postgres_prod_str, aud, jwt_token
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(postgres_prod_str)  # Create engine

client = GraphqlClient(  # GraphQlClient, in the headers I am passing my jwt-token, read more here: https://github.com/sorare/api
    endpoint="https://api.sorare.com/graphql",
    headers={
        "Content-Type": "application/json",
        "Authorization": f"ziruzavar {jwt_token}",
        "JWT_AUD": f"{aud}",
    },
    proxies={"https": PROXIES},
)

# Retrieving the teams from the DB
teams = pd.read_sql("SELECT * FROM transfermarkt_test.sorare_teams", engine)

query = """
{
  club(slug: "team_slug"){
    players(after: null){
      nodes{
          id
          displayName
          slug
        activeClub{
          name
        }
        cardSupply{
          rare
          limited
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

players = []
# Iteratting over players from each team
def iterate_over_players(data):
    for node in data["club"]["players"]["nodes"]:
        current_team_name = node["activeClub"]["name"] if node["activeClub"] else None
        if node["cardSupply"]:
            players.append(
                {
                    "player_name": node["displayName"],
                    "current_team_name": current_team_name,
                    "player_id": node["id"].split(":")[-1],
                    "slug": node['slug']
                }
            )


# Iteratting over the teams
for index, row in teams.iterrows():
    data = client.execute(query=query.replace("team_slug", row["slug"]))
    iterate_over_players(data["data"])
    while data["data"]["club"]["players"]["pageInfo"]["hasNextPage"]:
        cursor = data["data"]["club"]["players"]["pageInfo"]["endCursor"]
        data = client.execute(
            query=query.replace("team_slug", row["slug"]).replace("null", f'"{cursor}"')
        )
        iterate_over_players(data["data"])

df = pd.DataFrame(players)  # Saving all the players in a SQL database
df.to_sql(
    "sorare_players",
    con=engine,
    schema="transfermarkt_test",
    if_exists="append",
    index=False,
    method="multi",
    chunksize=100,
)
