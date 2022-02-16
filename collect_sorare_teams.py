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


query = """
{
  clubsReady{
    slug
    name
    domesticLeague{
      country{
        code
      }
      name
    }
  }
}
"""


teams = []
data = client.execute(query=query)

for club in data["data"]["clubsReady"]:
    name = club["slug"]
    t_name = club["name"]
    league = club["domesticLeague"]["name"]
    teams.append({"name": t_name, "slug": name, "league": league})

df = pd.DataFrame(teams)  # Saving all the teams in a SQL database
df.to_sql(
    "sorare_teams",
    con=engine,
    schema="transfermarkt_test",
    if_exists="append",
    index=False,
)
