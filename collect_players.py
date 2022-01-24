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
    players{
      edges{
        node{
            displayName
          cardSupply{
            rare
          }
        }
      }
    }
  }
}
"""

players = []

# Iteratting over the teams
for index, row in teams.iterrows():
    data = client.execute(query=query.replace("team_slug", row["slug"]))

    # Iteratting over players from each team
    for node in data["data"]["club"]["players"]["edges"]:
        if node["node"]["cardSupply"]:
            players.append(
                {"player_name": node["node"]["displayName"], "team_name": row["name"]}
            )
            
df = pd.DataFrame(players)  # Saving all the players in a SQL database
df.to_sql(
    "sorare_players",
    con=engine,
    schema="transfermarkt_test",
    if_exists="replace",
    index=False,
)
