from python_graphql_client import GraphqlClient
from sklearn.feature_selection import chi2
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

# Manually loading the wanted countries slugs
countries = ['uruguay', 'wales', 'senegal', 'peru', 'iran', 'japan', 'algeria', 'tunisia', 'australia', 'turkey',
'scotland', 'cameroon', 'ghana', 'mali', 'venezuela', 'panama', 'el-salvador'
,'russia', 'germany', 'france', 'italy', 'spain', 'ukraine', 'poland', 'romania',
'netherlands', 'belgium', 'czech-republic', 'greece', 'portugal', 'sweden', 'hungary', 'belarus', 'austria', 'serbia', 'switzerland',
'bulgaria', 'denmark', 'finland', 'slovakia', 'norway', 'croatia', 'moldova', 'albania', 'lithuania',
'slovenia', 'latvia', 'estonia', 'iceland', 'malta', 'england', 'united-states', 'canada', 'mexico', 'brazil',
'argentina', 'colombia', 'bolivia', 'ecuador', 'chile', 'nigeria', 'egypt']

query = """
{
  nationalTeam(slug:"team_slug"){
    activePlayers{
      edges{
        node{
          displayName
          id
          activeClub{
			name
          }
          cardSupply{
            rare
            limited
          }
        }
      }
    }
  }
}
"""

players = []

# Iteratting over the teams
for country in countries:
    print(country)
    data = client.execute(query=query.replace("team_slug", country))

    # Iteratting over players from each team
    for node in data["data"]["nationalTeam"]["activePlayers"]["edges"]:
        if node["node"]['activeClub']:
          current_team_name = node["node"]['activeClub']['name']
        else:
          current_team_name = None
        if node["node"]["cardSupply"]:
            players.append(
                {
                    "player_name": node["node"]["displayName"],
                    "current_team_name": current_team_name,
                    "player_id": node["node"]["id"].split(":")[-1],
                }
            )

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
